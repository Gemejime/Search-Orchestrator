package redis

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
)

const (
	// DefaultQueryCacheTTL is the default Tier 1 Redis cache entry expiration.
	DefaultQueryCacheTTL = 5 * time.Minute
	// DefaultCacheReadTimeout bounds Tier 1 Redis GET operations.
	DefaultCacheReadTimeout = 5 * time.Millisecond
	// DefaultCacheWriteTimeout bounds Tier 1 Redis SET operations so writes never stall request budgets.
	DefaultCacheWriteTimeout = 5 * time.Millisecond

	tier1BinaryVersion byte = 2
)

var (
	// ErrInvalidCachePayload indicates corrupt or unsupported serialized Tier 1 cache bytes.
	ErrInvalidCachePayload = errors.New("redis: invalid tier1 cache payload")
)

// QueryCacheConfig configures the Tier 1 normalized query cache adapter.
type QueryCacheConfig struct {
	TTL          time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// QueryTextCache implements repository.QueryCachePort backed by Redis 7.2 Cluster or single-node Redis.
type QueryTextCache struct {
	client       goredis.UniversalClient
	ttl          time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewQueryTextCache constructs a QueryTextCache with production defaults.
func NewQueryTextCache(client goredis.UniversalClient, cfg QueryCacheConfig) *QueryTextCache {
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultQueryCacheTTL
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = DefaultCacheReadTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = DefaultCacheWriteTimeout
	}
	return &QueryTextCache{
		client:       client,
		ttl:          cfg.TTL,
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
	}
}

// Get retrieves and decodes a Tier1CacheEntry for cacheKey. Returns (entry, false, nil) on cache miss.
func (c *QueryTextCache) Get(ctx context.Context, cacheKey string) (repository.Tier1CacheEntry, bool, error) {
	if c == nil || c.client == nil {
		return repository.Tier1CacheEntry{}, false, nil
	}

	readCtx, cancel := context.WithTimeout(ctx, c.readTimeout)
	defer cancel()

	raw, err := c.client.Get(readCtx, cacheKey).Bytes()
	if errors.Is(err, goredis.Nil) {
		return repository.Tier1CacheEntry{}, false, nil
	}
	if err != nil {
		return repository.Tier1CacheEntry{}, false, fmt.Errorf("redis tier1 get: %w", err)
	}

	entry, err := DecodeTier1CacheEntry(raw)
	if err != nil {
		return repository.Tier1CacheEntry{}, false, err
	}
	return entry, true, nil
}

// Set serializes and stores a caller-independent Tier1CacheEntry with a bounded write timeout.
func (c *QueryTextCache) Set(ctx context.Context, cacheKey string, entry repository.Tier1CacheEntry) error {
	if c == nil || c.client == nil {
		return nil
	}

	payload, err := EncodeTier1CacheEntry(entry)
	if err != nil {
		return err
	}

	writeCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()

	if err := c.client.Set(writeCtx, cacheKey, payload, c.ttl).Err(); err != nil {
		return fmt.Errorf("redis tier1 set: %w", err)
	}
	return nil
}

// EncodeTier1CacheEntry encodes a Tier1CacheEntry into a compact, deterministic binary payload.
// Format: [1B version][1B reranker_fallback][2B count] followed per candidate by
// [4B doc_id][2B chunk_id_len][chunk_id][2B parent_key_len][parent_key][8B float64 score][4B text_len][text].
func EncodeTier1CacheEntry(entry repository.Tier1CacheEntry) ([]byte, error) {
	countU16, ok := safeUint16(len(entry.Candidates))
	if !ok {
		return nil, fmt.Errorf("%w: too many candidates (%d)", ErrInvalidCachePayload, len(entry.Candidates))
	}

	size := 4
	for _, cand := range entry.Candidates {
		if _, ok := safeUint16(len(cand.ChunkID)); !ok {
			return nil, fmt.Errorf("%w: candidate identifier exceeds uint16 length", ErrInvalidCachePayload)
		}
		if _, ok := safeUint16(len(cand.ParentKey)); !ok {
			return nil, fmt.Errorf("%w: candidate identifier exceeds uint16 length", ErrInvalidCachePayload)
		}
		if _, ok := safeUint32(len(cand.Text)); !ok {
			return nil, fmt.Errorf("%w: candidate text exceeds uint32 length", ErrInvalidCachePayload)
		}
		size += 4 + 2 + len(cand.ChunkID) + 2 + len(cand.ParentKey) + 8 + 4 + len(cand.Text)
	}

	buf := make([]byte, 0, size)
	var fallbackByte byte
	if entry.RerankerFallback {
		fallbackByte = 1
	}
	buf = append(buf, tier1BinaryVersion, fallbackByte, 0, 0)
	binary.BigEndian.PutUint16(buf[2:4], countU16)

	for _, cand := range entry.Candidates {
		var u32 [4]byte
		binary.BigEndian.PutUint32(u32[:], cand.DocID)
		buf = append(buf, u32[:]...)

		chunkLen, ok := safeUint16(len(cand.ChunkID))
		if !ok {
			return nil, fmt.Errorf("%w: candidate identifier exceeds uint16 length", ErrInvalidCachePayload)
		}
		var u16 [2]byte
		binary.BigEndian.PutUint16(u16[:], chunkLen)
		buf = append(buf, u16[:]...)
		buf = append(buf, cand.ChunkID...)

		parentLen, ok := safeUint16(len(cand.ParentKey))
		if !ok {
			return nil, fmt.Errorf("%w: candidate identifier exceeds uint16 length", ErrInvalidCachePayload)
		}
		binary.BigEndian.PutUint16(u16[:], parentLen)
		buf = append(buf, u16[:]...)
		buf = append(buf, cand.ParentKey...)

		var u64 [8]byte
		binary.BigEndian.PutUint64(u64[:], math.Float64bits(cand.Score))
		buf = append(buf, u64[:]...)

		textLen, ok := safeUint32(len(cand.Text))
		if !ok {
			return nil, fmt.Errorf("%w: candidate text exceeds uint32 length", ErrInvalidCachePayload)
		}
		binary.BigEndian.PutUint32(u32[:], textLen)
		buf = append(buf, u32[:]...)
		buf = append(buf, cand.Text...)
	}
	return buf, nil
}

func safeUint16(v int) (uint16, bool) {
	if v < 0 || v > math.MaxUint16 {
		return 0, false
	}
	return uint16(v), true
}

func safeUint32(v int) (uint32, bool) {
	if v < 0 || v > math.MaxUint32 {
		return 0, false
	}
	return uint32(v), true
}

// DecodeTier1CacheEntry decodes a binary Tier1CacheEntry payload produced by EncodeTier1CacheEntry.
func DecodeTier1CacheEntry(raw []byte) (repository.Tier1CacheEntry, error) {
	if len(raw) < 4 {
		return repository.Tier1CacheEntry{}, fmt.Errorf("%w: truncated header", ErrInvalidCachePayload)
	}
	version := raw[0]
	if version != 1 && version != tier1BinaryVersion {
		return repository.Tier1CacheEntry{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidCachePayload, version)
	}

	entry := repository.Tier1CacheEntry{
		RerankerFallback: raw[1] != 0,
	}
	count := int(binary.BigEndian.Uint16(raw[2:4]))
	entry.Candidates = make([]repository.CachedCandidate, count)

	offset := 4
	for i := 0; i < count; i++ {
		if offset+4+2 > len(raw) {
			return repository.Tier1CacheEntry{}, fmt.Errorf("%w: truncated candidate %d", ErrInvalidCachePayload, i)
		}
		docID := binary.BigEndian.Uint32(raw[offset : offset+4])
		offset += 4

		chunkLen := int(binary.BigEndian.Uint16(raw[offset : offset+2]))
		offset += 2
		if offset+chunkLen+2 > len(raw) {
			return repository.Tier1CacheEntry{}, fmt.Errorf("%w: truncated chunk_id %d", ErrInvalidCachePayload, i)
		}
		chunkID := string(raw[offset : offset+chunkLen])
		offset += chunkLen

		parentLen := int(binary.BigEndian.Uint16(raw[offset : offset+2]))
		offset += 2
		if offset+parentLen+8 > len(raw) {
			return repository.Tier1CacheEntry{}, fmt.Errorf("%w: truncated parent_key %d", ErrInvalidCachePayload, i)
		}
		parentKey := string(raw[offset : offset+parentLen])
		offset += parentLen

		score := math.Float64frombits(binary.BigEndian.Uint64(raw[offset : offset+8]))
		offset += 8

		var text string
		if version >= 2 {
			if offset+4 > len(raw) {
				return repository.Tier1CacheEntry{}, fmt.Errorf("%w: truncated text_len %d", ErrInvalidCachePayload, i)
			}
			textLen := int(binary.BigEndian.Uint32(raw[offset : offset+4]))
			offset += 4
			if textLen < 0 || offset+textLen > len(raw) {
				return repository.Tier1CacheEntry{}, fmt.Errorf("%w: truncated text %d", ErrInvalidCachePayload, i)
			}
			text = string(raw[offset : offset+textLen])
			offset += textLen
		}

		entry.Candidates[i] = repository.CachedCandidate{
			DocID:     docID,
			ChunkID:   chunkID,
			ParentKey: parentKey,
			Text:      text,
			Score:     score,
		}
	}

	if offset != len(raw) {
		return repository.Tier1CacheEntry{}, fmt.Errorf("%w: trailing bytes", ErrInvalidCachePayload)
	}
	return entry, nil
}
