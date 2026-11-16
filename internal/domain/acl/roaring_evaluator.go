package acl

import (
	"errors"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

var (
	// ErrEmptyACLBitmap is returned when caller_acl_bitmap is nil or empty.
	ErrEmptyACLBitmap = errors.New("caller_acl_bitmap must not be empty")
	// ErrInvalidACLBitmap is returned when caller_acl_bitmap fails Roaring binary decoding.
	// Raw bitmap bytes are intentionally omitted from the error to uphold secured-logging invariants.
	ErrInvalidACLBitmap = errors.New("caller_acl_bitmap is malformed")
)

// RoaringEvaluatorPool recycles *roaring.Bitmap instances to avoid per-request heap allocations
// when decoding caller_acl_bitmap payloads.
type RoaringEvaluatorPool struct {
	bitmapPool sync.Pool
}

// NewRoaringEvaluatorPool constructs a RoaringEvaluatorPool.
func NewRoaringEvaluatorPool() *RoaringEvaluatorPool {
	return &RoaringEvaluatorPool{
		bitmapPool: sync.Pool{
			New: func() any {
				return roaring.New()
			},
		},
	}
}

// Acquire decodes compressed Roaring Bitmap bytes into a pooled RoaringEvaluator.
// Callers must invoke Close() when finished to return the underlying bitmap to the pool.
// Fails closed on empty or malformed payloads.
func (p *RoaringEvaluatorPool) Acquire(compressed []byte) (*RoaringEvaluator, error) {
	if len(compressed) == 0 {
		return nil, ErrEmptyACLBitmap
	}

	bm := p.bitmapPool.Get().(*roaring.Bitmap)
	bm.Clear()

	if err := bm.UnmarshalBinary(compressed); err != nil {
		bm.Clear()
		p.bitmapPool.Put(bm)
		return nil, ErrInvalidACLBitmap
	}

	return &RoaringEvaluator{
		bitmap: bm,
		pool:   p,
	}, nil
}

// RoaringEvaluator evaluates native uint32 document entitlements against a caller's
// compressed Roaring Bitmap in orchestrator heap memory with zero allocations.
type RoaringEvaluator struct {
	bitmap *roaring.Bitmap
	pool   *RoaringEvaluatorPool
}

// NewRoaringEvaluator decodes compressed Roaring Bitmap bytes into a standalone RoaringEvaluator.
// Fails closed on empty or malformed payloads.
func NewRoaringEvaluator(compressed []byte) (*RoaringEvaluator, error) {
	if len(compressed) == 0 {
		return nil, ErrEmptyACLBitmap
	}

	bm := roaring.New()
	if err := bm.UnmarshalBinary(compressed); err != nil {
		return nil, ErrInvalidACLBitmap
	}

	return &RoaringEvaluator{bitmap: bm}, nil
}

// IsPermitted returns true if docID is present in the caller's Roaring Bitmap.
// Fails closed (returns false) if the evaluator or underlying bitmap is nil.
func (e *RoaringEvaluator) IsPermitted(docID uint32) bool {
	if e == nil || e.bitmap == nil {
		return false
	}
	return e.bitmap.Contains(docID)
}

// Cardinality returns the number of permitted uint32 document IDs in the bitmap.
func (e *RoaringEvaluator) Cardinality() uint64 {
	if e == nil || e.bitmap == nil {
		return 0
	}
	return e.bitmap.GetCardinality()
}

// FilterDocIDs appends all permitted uint32 docIDs from docIDs into dst[:0] and returns
// the resulting slice. When dst has sufficient capacity, this executes with zero heap allocations.
func (e *RoaringEvaluator) FilterDocIDs(docIDs []uint32, dst []uint32) []uint32 {
	out := dst[:0]
	if e == nil || e.bitmap == nil || len(docIDs) == 0 {
		return out
	}
	for _, id := range docIDs {
		if e.bitmap.Contains(id) {
			out = append(out, id)
		}
	}
	return out
}

// FilterCandidates appends all ACL-permitted FusedCandidates into dst[:0] and returns
// the resulting slice. When dst has sufficient capacity, this executes with zero heap allocations.
func (e *RoaringEvaluator) FilterCandidates(candidates []entity.FusedCandidate, dst []entity.FusedCandidate) []entity.FusedCandidate {
	out := dst[:0]
	if e == nil || e.bitmap == nil || len(candidates) == 0 {
		return out
	}
	for i := range candidates {
		if e.bitmap.Contains(candidates[i].DocID) {
			out = append(out, candidates[i])
		}
	}
	return out
}

// IntersectInPlace performs an in-place bitwise AND intersection of candidates with
// the caller's Roaring Bitmap. Fails closed (clears candidates) if the evaluator is nil or closed.
func (e *RoaringEvaluator) IntersectInPlace(candidates *roaring.Bitmap) {
	if candidates == nil {
		return
	}
	if e == nil || e.bitmap == nil {
		candidates.Clear()
		return
	}
	candidates.And(e.bitmap)
}

// Close clears the underlying Roaring Bitmap and returns it to the pool if acquired from one.
// Subsequent IsPermitted calls on a closed evaluator fail closed (return false).
func (e *RoaringEvaluator) Close() {
	if e == nil || e.bitmap == nil {
		return
	}
	e.bitmap.Clear()
	if e.pool != nil {
		e.pool.bitmapPool.Put(e.bitmap)
	}
	e.bitmap = nil
}
