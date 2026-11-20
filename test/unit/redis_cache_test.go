package unit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
	rediscache "github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/cache/redis"
)

// miniRESPServer is a lightweight in-memory Redis RESP2 server for unit-testing
// go-redis UniversalClient commands (PING, GET, SET, MGET, pipelines) without Docker.
type miniRESPServer struct {
	ln net.Listener
	mu sync.Mutex
	kv map[string]string
}

func startMiniRESPServer(t *testing.T) *miniRESPServer {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen miniRESPServer: %v", err)
	}
	s := &miniRESPServer{
		ln: ln,
		kv: make(map[string]string),
	}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *miniRESPServer) addr() string {
	return s.ln.Addr().String()
}

func (s *miniRESPServer) setRaw(key, val string) {
	s.mu.Lock()
	s.kv[key] = val
	s.mu.Unlock()
}

func (s *miniRESPServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *miniRESPServer) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		args, err := readRESPArray(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "HELLO":
			_, _ = w.WriteString("-ERR unknown command 'HELLO'\r\n")
		case "CLIENT":
			_, _ = w.WriteString("+OK\r\n")
		case "PING":
			_, _ = w.WriteString("+PONG\r\n")
		case "GET":
			if len(args) < 2 {
				_, _ = w.WriteString("-ERR wrong number of arguments\r\n")
				break
			}
			s.mu.Lock()
			val, ok := s.kv[args[1]]
			s.mu.Unlock()
			if !ok {
				_, _ = w.WriteString("$-1\r\n")
			} else {
				_, _ = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(val), val)
			}
		case "SET":
			if len(args) < 3 {
				_, _ = w.WriteString("-ERR wrong number of arguments\r\n")
				break
			}
			s.mu.Lock()
			s.kv[args[1]] = args[2]
			s.mu.Unlock()
			_, _ = w.WriteString("+OK\r\n")
		case "MGET":
			keys := args[1:]
			_, _ = fmt.Fprintf(w, "*%d\r\n", len(keys))
			s.mu.Lock()
			for _, k := range keys {
				if val, ok := s.kv[k]; ok {
					_, _ = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(val), val)
				} else {
					_, _ = w.WriteString("$-1\r\n")
				}
			}
			s.mu.Unlock()
		default:
			_, _ = w.WriteString("+OK\r\n")
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" || line[0] != '*' {
		return nil, errors.New("expected RESP array")
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimSuffix(hdr, "\r\n")
		if hdr == "" || hdr[0] != '$' {
			return nil, errors.New("expected RESP bulk string")
		}
		sz, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, sz+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out[i] = string(buf[:sz])
	}
	return out, nil
}

func TestTier1BinaryCodec_RoundTripAndValidation(t *testing.T) {
	t.Parallel()

	in := repository.Tier1CacheEntry{
		RerankerFallback: true,
		Candidates: []repository.CachedCandidate{
			{DocID: 101, ChunkID: "chk-1", ParentKey: "parent:t1:101:chk-1", Text: "passage one", Score: 0.9123},
			{DocID: 202, ChunkID: "chk-2", ParentKey: "parent:t1:202:chk-2", Text: "passage two", Score: 0.5432},
		},
	}

	raw, err := rediscache.EncodeTier1CacheEntry(in)
	if err != nil {
		t.Fatalf("EncodeTier1CacheEntry: %v", err)
	}
	out, err := rediscache.DecodeTier1CacheEntry(raw)
	if err != nil {
		t.Fatalf("DecodeTier1CacheEntry: %v", err)
	}
	if out.RerankerFallback != in.RerankerFallback || len(out.Candidates) != len(in.Candidates) {
		t.Fatalf("decoded mismatch: got %+v, want %+v", out, in)
	}
	for i := range in.Candidates {
		if out.Candidates[i] != in.Candidates[i] {
			t.Fatalf("candidate %d mismatch: got %+v, want %+v", i, out.Candidates[i], in.Candidates[i])
		}
	}

	if _, err := rediscache.DecodeTier1CacheEntry([]byte{1, 0}); !errors.Is(err, rediscache.ErrInvalidCachePayload) {
		t.Fatalf("expected ErrInvalidCachePayload for truncated input, got %v", err)
	}
	if _, err := rediscache.DecodeTier1CacheEntry([]byte{99, 0, 0, 0}); !errors.Is(err, rediscache.ErrInvalidCachePayload) {
		t.Fatalf("expected ErrInvalidCachePayload for bad version, got %v", err)
	}
}

func TestRedisClusterKeySlot_HashTag(t *testing.T) {
	t.Parallel()

	s1 := rediscache.KeySlot("{tenant-1}.parent:10:chk1")
	s2 := rediscache.KeySlot("{tenant-1}.parent:20:chk2")
	if s1 != s2 {
		t.Fatalf("expected identical hash slots for shared hash tag, got %d vs %d", s1, s2)
	}
	if s1 >= 16384 {
		t.Fatalf("slot out of bounds: %d", s1)
	}
}

func TestQueryTextCache_And_ParentChunkReader_MultiSlot(t *testing.T) {
	t.Parallel()

	srv := startMiniRESPServer(t)
	client := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:    []string{srv.addr()},
		Protocol: 2,
	})
	t.Cleanup(func() { _ = client.Close() })

	cache := rediscache.NewQueryTextCache(client, rediscache.QueryCacheConfig{
		TTL:          time.Minute,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})

	ctx := context.Background()
	_, hit, err := cache.Get(ctx, "qcache:t1:missing")
	if err != nil || hit {
		t.Fatalf("expected miss without error, got hit=%v err=%v", hit, err)
	}

	entry := repository.Tier1CacheEntry{
		RerankerFallback: false,
		Candidates: []repository.CachedCandidate{
			{DocID: 1, ChunkID: "c1", ParentKey: "parent:t1:1:c1", Score: 0.99},
			{DocID: 2, ChunkID: "c2", ParentKey: "parent:t1:2:c2", Score: 0.88},
		},
	}
	if err := cache.Set(ctx, "qcache:t1:k1", entry); err != nil {
		t.Fatalf("cache.Set: %v", err)
	}
	got, hit, err := cache.Get(ctx, "qcache:t1:k1")
	if err != nil || !hit {
		t.Fatalf("expected hit, got hit=%v err=%v", hit, err)
	}
	if len(got.Candidates) != 2 || got.Candidates[0].DocID != 1 {
		t.Fatalf("unexpected cached candidates: %+v", got.Candidates)
	}

	// Populate parent chunks across distinct hash slots (and leave doc 2 missing).
	srv.setRaw("parent:t1:1:c1", `{"text":"first parent chunk","metadata":{"source":"doc1.pdf"}}`)
	srv.setRaw("parent:t1:3:c3", `{"text":"third parent chunk","metadata":{"source":"doc3.pdf"}}`)

	reader := rediscache.NewParentChunkReader(client, slog.Default())
	if err := reader.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	chunks, err := reader.HydrateParents(ctx, "t1", []entity.FusedCandidate{
		{DocID: 3, ChunkID: "c3", ParentKey: "parent:t1:3:c3", FinalScore: 0.95},
		{DocID: 2, ChunkID: "c2", ParentKey: "parent:t1:2:c2", FinalScore: 0.85},
		{DocID: 1, ChunkID: "c1", ParentKey: "parent:t1:1:c1", FinalScore: 0.75},
	})
	if err != nil {
		t.Fatalf("HydrateParents: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 hydrated chunks (1 missing skipped), got %d", len(chunks))
	}
	if chunks[0].DocID != 3 || chunks[0].Score != 0.95 || chunks[0].Text != "third parent chunk" {
		t.Fatalf("unexpected first hydrated chunk: %+v", chunks[0])
	}
	if chunks[1].DocID != 1 || chunks[1].Score != 0.75 || chunks[1].Metadata["source"] != "doc1.pdf" {
		t.Fatalf("unexpected second hydrated chunk: %+v", chunks[1])
	}
}
