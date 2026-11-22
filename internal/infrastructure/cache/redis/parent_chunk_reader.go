package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

// ParentChunkPayload represents the pre-compiled parent container stored in Redis.
type ParentChunkPayload struct {
	Text     string            `json:"text"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ParentChunkReader implements repository.ParentChunkPort using Redis Cluster-safe per-slot MGET batching.
type ParentChunkReader struct {
	client goredis.UniversalClient
	logger *slog.Logger
}

// NewParentChunkReader constructs a ParentChunkReader backed by a Redis UniversalClient.
func NewParentChunkReader(client goredis.UniversalClient, logger *slog.Logger) *ParentChunkReader {
	if logger == nil {
		logger = slog.Default()
	}
	return &ParentChunkReader{
		client: client,
		logger: logger,
	}
}

// FormatParentKey builds the canonical Redis parent container key: parent:<tenant_id>:<doc_id>:<chunk_id>.
func FormatParentKey(tenantID string, docID uint32, chunkID string) string {
	return "parent:" + tenantID + ":" + strconv.FormatUint(uint64(docID), 10) + ":" + chunkID
}

// HydrateParents fetches pre-compiled parent containers via CRC16 slot-grouped MGET batches
// (preventing Redis Cluster CROSSSLOT errors) while preserving input candidate order and FinalScore.
func (r *ParentChunkReader) HydrateParents(
	ctx context.Context,
	tenantID string,
	candidates []entity.FusedCandidate,
) ([]entity.EnrichedChunk, error) {
	if len(candidates) == 0 {
		return []entity.EnrichedChunk{}, nil
	}
	if r == nil || r.client == nil {
		return nil, errors.New("redis: parent chunk client is nil")
	}

	keys := make([]string, len(candidates))
	slotGroups := make(map[uint16][]int, len(candidates))
	slotOrder := make([]uint16, 0, len(candidates))

	for i, cand := range candidates {
		key := cand.ParentKey
		if key == "" {
			key = FormatParentKey(tenantID, cand.DocID, cand.ChunkID)
		}
		keys[i] = key
		slot := KeySlot(key)
		if _, exists := slotGroups[slot]; !exists {
			slotOrder = append(slotOrder, slot)
		}
		slotGroups[slot] = append(slotGroups[slot], i)
	}

	rawResults := make([]any, len(candidates))

	if len(slotOrder) == 1 {
		vals, err := r.client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("redis parent mget: %w", err)
		}
		copy(rawResults, vals)
	} else {
		pipe := r.client.Pipeline()
		cmds := make([]*goredis.SliceCmd, len(slotOrder))
		for sIdx, slot := range slotOrder {
			indices := slotGroups[slot]
			slotKeys := make([]string, len(indices))
			for j, candIdx := range indices {
				slotKeys[j] = keys[candIdx]
			}
			cmds[sIdx] = pipe.MGet(ctx, slotKeys...)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("redis parent pipeline mget: %w", err)
		}
		for sIdx, slot := range slotOrder {
			vals, err := cmds[sIdx].Result()
			if err != nil {
				return nil, fmt.Errorf("redis parent slot %d mget: %w", slot, err)
			}
			indices := slotGroups[slot]
			for j, candIdx := range indices {
				if j < len(vals) {
					rawResults[candIdx] = vals[j]
				}
			}
		}
	}

	out := make([]entity.EnrichedChunk, 0, len(candidates))
	for i, cand := range candidates {
		val := rawResults[i]
		if val == nil {
			r.logger.WarnContext(ctx, "parent chunk missing or expired in redis",
				slog.String("tenant_id", tenantID),
				slog.Uint64("doc_id", uint64(cand.DocID)),
				slog.String("chunk_id", cand.ChunkID),
			)
			continue
		}

		var rawBytes []byte
		switch v := val.(type) {
		case string:
			rawBytes = []byte(v)
		case []byte:
			rawBytes = v
		default:
			r.logger.WarnContext(ctx, "unexpected redis parent chunk value type",
				slog.String("tenant_id", tenantID),
				slog.Uint64("doc_id", uint64(cand.DocID)),
				slog.String("chunk_id", cand.ChunkID),
			)
			continue
		}

		var payload ParentChunkPayload
		if err := json.Unmarshal(rawBytes, &payload); err != nil {
			r.logger.WarnContext(ctx, "failed to unmarshal parent chunk payload",
				slog.String("tenant_id", tenantID),
				slog.Uint64("doc_id", uint64(cand.DocID)),
				slog.String("chunk_id", cand.ChunkID),
				slog.String("error", err.Error()),
			)
			continue
		}

		metadata := cand.Metadata
		if len(payload.Metadata) > 0 {
			metadata = payload.Metadata
		}

		out = append(out, entity.EnrichedChunk{
			DocID:     cand.DocID,
			ChunkID:   cand.ChunkID,
			ParentKey: keys[i],
			Text:      payload.Text,
			Score:     cand.FinalScore,
			Metadata:  metadata,
		})
	}

	return out, nil
}

// Ping verifies Redis Cluster / single-node connectivity for readiness probes.
func (r *ParentChunkReader) Ping(ctx context.Context) error {
	if r == nil || r.client == nil {
		return errors.New("redis client not configured")
	}
	return r.client.Ping(ctx).Err()
}
