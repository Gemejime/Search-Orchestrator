package pool

import (
	"sync"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

const (
	// DefaultSearchCandidateCap is the initial slice capacity for per-modality search candidates (Top-40).
	DefaultSearchCandidateCap = 40
	// MaxSearchCandidateCap is the upper capacity bound retained by the pool.
	MaxSearchCandidateCap = 256

	// DefaultFusedCandidateCap is the initial slice capacity for RRF and ACL-filtered candidates (Top-20).
	DefaultFusedCandidateCap = 20
	// MaxFusedCandidateCap is the upper capacity bound retained by the pool.
	MaxFusedCandidateCap = 128

	// DefaultDocIDCap is the initial slice capacity for uint32 doc_id batches.
	DefaultDocIDCap = 40
	// MaxDocIDCap is the upper capacity bound retained by the pool.
	MaxDocIDCap = 1024
)

// CandidatePool manages reusable, zero-allocation slice buffers for hot-path retrieval stages.
// Slice pointers (*[]T) are stored in sync.Pool to avoid slice-header boxing allocations.
type CandidatePool struct {
	searchPool sync.Pool
	fusedPool  sync.Pool
	docIDPool  sync.Pool
}

// NewCandidatePool constructs a CandidatePool with pre-sized slice factories.
func NewCandidatePool() *CandidatePool {
	return &CandidatePool{
		searchPool: sync.Pool{
			New: func() any {
				s := make([]entity.SearchCandidate, 0, DefaultSearchCandidateCap)
				return &s
			},
		},
		fusedPool: sync.Pool{
			New: func() any {
				s := make([]entity.FusedCandidate, 0, DefaultFusedCandidateCap)
				return &s
			},
		},
		docIDPool: sync.Pool{
			New: func() any {
				s := make([]uint32, 0, DefaultDocIDCap)
				return &s
			},
		},
	}
}

// GetSearchCandidates borrows a zero-length []entity.SearchCandidate buffer from the pool.
func (p *CandidatePool) GetSearchCandidates() *[]entity.SearchCandidate {
	buf := p.searchPool.Get().(*[]entity.SearchCandidate)
	*buf = (*buf)[:0]
	return buf
}

// PutSearchCandidates clears element references, resets length to zero, and returns the buffer.
// Oversized buffers exceeding MaxSearchCandidateCap are dropped to bound pool memory.
func (p *CandidatePool) PutSearchCandidates(buf *[]entity.SearchCandidate) {
	if buf == nil {
		return
	}
	if cap(*buf) > MaxSearchCandidateCap {
		return
	}
	clear((*buf)[:cap(*buf)])
	*buf = (*buf)[:0]
	p.searchPool.Put(buf)
}

// GetFusedCandidates borrows a zero-length []entity.FusedCandidate buffer from the pool.
func (p *CandidatePool) GetFusedCandidates() *[]entity.FusedCandidate {
	buf := p.fusedPool.Get().(*[]entity.FusedCandidate)
	*buf = (*buf)[:0]
	return buf
}

// PutFusedCandidates clears element references, resets length to zero, and returns the buffer.
// Oversized buffers exceeding MaxFusedCandidateCap are dropped to bound pool memory.
func (p *CandidatePool) PutFusedCandidates(buf *[]entity.FusedCandidate) {
	if buf == nil {
		return
	}
	if cap(*buf) > MaxFusedCandidateCap {
		return
	}
	clear((*buf)[:cap(*buf)])
	*buf = (*buf)[:0]
	p.fusedPool.Put(buf)
}

// GetDocIDs borrows a zero-length []uint32 buffer from the pool.
func (p *CandidatePool) GetDocIDs() *[]uint32 {
	buf := p.docIDPool.Get().(*[]uint32)
	*buf = (*buf)[:0]
	return buf
}

// PutDocIDs resets length to zero and returns the uint32 buffer to the pool.
// Oversized buffers exceeding MaxDocIDCap are dropped to bound pool memory.
func (p *CandidatePool) PutDocIDs(buf *[]uint32) {
	if buf == nil {
		return
	}
	if cap(*buf) > MaxDocIDCap {
		return
	}
	*buf = (*buf)[:0]
	p.docIDPool.Put(buf)
}
