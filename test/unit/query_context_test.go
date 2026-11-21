package unit_test

import (
	"errors"
	"testing"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

func TestNewQueryContext_ValidationAndDefaults(t *testing.T) {
	t.Run("top_k=0 defaults to 5", func(t *testing.T) {
		qc, err := entity.NewQueryContext("req-1", "tenant-a", "What is SOC2?", "bge-m3-v2", []byte{1, 2}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if qc.TopK != 5 {
			t.Fatalf("expected TopK=5, got %d", qc.TopK)
		}
	})

	t.Run("valid top_k 1..5", func(t *testing.T) {
		for k := 1; k <= 5; k++ {
			qc, err := entity.NewQueryContext("req-1", "tenant-a", "hello", "bge-m3-v2", nil, k)
			if err != nil {
				t.Fatalf("top_k=%d unexpected error: %v", k, err)
			}
			if qc.TopK != k {
				t.Fatalf("expected TopK=%d, got %d", k, qc.TopK)
			}
		}
	})

	cases := []struct {
		name    string
		tenant  string
		query   string
		model   string
		topK    int
		wantErr error
	}{
		{name: "negative top_k", tenant: "t1", query: "q", model: "m1", topK: -1, wantErr: entity.ErrInvalidTopK},
		{name: "top_k > 5", tenant: "t1", query: "q", model: "m1", topK: 6, wantErr: entity.ErrInvalidTopK},
		{name: "empty tenant_id", tenant: "", query: "q", model: "m1", topK: 5, wantErr: entity.ErrEmptyTenantID},
		{name: "whitespace tenant_id", tenant: "   \t ", query: "q", model: "m1", topK: 5, wantErr: entity.ErrEmptyTenantID},
		{name: "empty query_text", tenant: "t1", query: "", model: "m1", topK: 5, wantErr: entity.ErrEmptyQueryText},
		{name: "whitespace query_text", tenant: "t1", query: " \n\t ", model: "m1", topK: 5, wantErr: entity.ErrEmptyQueryText},
		{name: "empty embedding_model_version", tenant: "t1", query: "q", model: "", topK: 5, wantErr: entity.ErrEmptyEmbeddingModelVersion},
		{name: "whitespace embedding_model_version", tenant: "t1", query: "q", model: "   ", topK: 5, wantErr: entity.ErrEmptyEmbeddingModelVersion},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := entity.NewQueryContext("req", tc.tenant, tc.query, tc.model, nil, tc.topK)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestNormalizeQuery_NFCAndWhitespace(t *testing.T) {
	// "Cafe\u0301" (NFD) vs "CAF\u00c9" (NFC uppercase)
	nfd := "  Cafe\u0301\t\n   Encryption   ROTATION  "
	nfc := "caf\u00e9 encryption rotation"

	gotNFD := entity.NormalizeQuery(nfd)
	gotNFC := entity.NormalizeQuery(nfc)

	if gotNFD != nfc {
		t.Fatalf("expected %q, got %q", nfc, gotNFD)
	}
	if gotNFC != gotNFD {
		t.Fatalf("expected NFD and NFC to normalize identically: %q vs %q", gotNFD, gotNFC)
	}
}

func TestBuildCanonicalCacheHash_DeterminismAndPrefixSafety(t *testing.T) {
	h1 := entity.BuildCanonicalCacheHash("soc2 rotation", "tenant-01", "bge-m3-v2")
	h2 := entity.BuildCanonicalCacheHash("soc2 rotation", "tenant-01", "bge-m3-v2")
	if h1 != h2 {
		t.Fatalf("expected deterministic hash, got %q != %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("expected 64-char hex SHA-256, got len %d", len(h1))
	}

	// Length-prefixing prevents boundary ambiguity: ("ab", "c") vs ("a", "bc").
	c1 := entity.BuildCanonicalCacheHash("ab", "c", "v1")
	c2 := entity.BuildCanonicalCacheHash("a", "bc", "v1")
	if c1 == c2 {
		t.Fatalf("length-prefixed hashes collided for boundary-shifted inputs: %q", c1)
	}

	qc, err := entity.NewQueryContext("req-1", "tenant-01", "  SOC2   Rotation ", "bge-m3-v2", nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if qc.QueryHash != h1 {
		t.Fatalf("expected QueryHash %q, got %q", h1, qc.QueryHash)
	}
	wantKey := "qcache:tenant-01:" + h1
	if qc.Tier1CacheKey() != wantKey {
		t.Fatalf("expected Tier1CacheKey %q, got %q", wantKey, qc.Tier1CacheKey())
	}
}

func BenchmarkNormalizeAndCacheKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := entity.NewQueryContext(
			"req-bench",
			"tenant-corp-01",
			"  What are the Q3   SOC 2 encryption rotation requirements? ",
			"bge-m3-fp16-v2",
			nil,
			5,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}
