package entity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"golang.org/x/text/unicode/norm"
)

const (
	// DefaultTopK is the default number of hydrated chunks returned when top_k is 0.
	DefaultTopK = 5
	// MaxTopK is the maximum permitted number of hydrated chunks per request.
	MaxTopK = 5
)

var (
	// ErrEmptyTenantID is returned when tenant_id is empty or whitespace-only.
	ErrEmptyTenantID = errors.New("tenant_id must not be empty")
	// ErrEmptyQueryText is returned when normalized query_text is empty.
	ErrEmptyQueryText = errors.New("query_text must not be empty after normalization")
	// ErrEmptyEmbeddingModelVersion is returned when embedding_model_version is empty or whitespace-only.
	ErrEmptyEmbeddingModelVersion = errors.New("embedding_model_version must not be empty")
	// ErrInvalidTopK is returned when top_k is outside the allowed [0..5] range.
	ErrInvalidTopK = errors.New("top_k must be between 1 and 5 (or 0 to use default)")
)

// QueryContext holds validated, normalized ingress parameters and derived cache keys.
type QueryContext struct {
	RequestID             string
	TenantID              string
	QueryText             string
	NormalizedQuery       string
	EmbeddingModelVersion string
	CallerACLBitmap       []byte
	TopK                  int
	QueryHash             string
}

// NewQueryContext validates request parameters, normalizes query text, and computes the
// canonical Tier 1 SHA-256 query hash. A topK of 0 defaults to DefaultTopK (5) for
// Protobuf v3 compatibility.
func NewQueryContext(
	requestID string,
	tenantID string,
	queryText string,
	embeddingModelVersion string,
	callerACLBitmap []byte,
	topK int,
) (QueryContext, error) {
	if strings.TrimSpace(tenantID) == "" {
		return QueryContext{}, ErrEmptyTenantID
	}
	if strings.TrimSpace(embeddingModelVersion) == "" {
		return QueryContext{}, ErrEmptyEmbeddingModelVersion
	}

	normalized := NormalizeQuery(queryText)
	if normalized == "" {
		return QueryContext{}, ErrEmptyQueryText
	}

	effectiveTopK := topK
	if effectiveTopK == 0 {
		effectiveTopK = DefaultTopK
	} else if effectiveTopK < 0 || effectiveTopK > MaxTopK {
		return QueryContext{}, ErrInvalidTopK
	}

	queryHash := BuildCanonicalCacheHash(normalized, tenantID, embeddingModelVersion)

	return QueryContext{
		RequestID:             requestID,
		TenantID:              tenantID,
		QueryText:             queryText,
		NormalizedQuery:       normalized,
		EmbeddingModelVersion: embeddingModelVersion,
		CallerACLBitmap:       callerACLBitmap,
		TopK:                  effectiveTopK,
		QueryHash:             queryHash,
	}, nil
}

// NormalizeQuery applies Unicode NFC normalization, trims leading/trailing whitespace,
// collapses internal whitespace runs to a single ASCII space, and lowercases the text.
func NormalizeQuery(raw string) string {
	nfc := norm.NFC.String(raw)
	collapsed := strings.Join(strings.Fields(nfc), " ")
	return norm.NFC.String(strings.ToLower(collapsed))
}

// BuildCanonicalCacheHash computes the length-prefixed SHA-256 digest:
// SHA256(len_bytes(NormQuery) + ":" + NormQuery + ":" + len_bytes(TenantID) + ":" + TenantID + ":" + len_bytes(EmbeddingModelVersion) + ":" + EmbeddingModelVersion).
func BuildCanonicalCacheHash(normQuery, tenantID, embeddingModelVersion string) string {
	var b strings.Builder
	b.Grow(len(normQuery) + len(tenantID) + len(embeddingModelVersion) + 32)
	writeLengthPrefixed(&b, normQuery)
	b.WriteByte(':')
	writeLengthPrefixed(&b, tenantID)
	b.WriteByte(':')
	writeLengthPrefixed(&b, embeddingModelVersion)

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// Tier1CacheKey returns the Redis 7.2 Cluster Tier 1 cache key for this query context.
func (q QueryContext) Tier1CacheKey() string {
	return "qcache:" + q.TenantID + ":" + q.QueryHash
}

func writeLengthPrefixed(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}
