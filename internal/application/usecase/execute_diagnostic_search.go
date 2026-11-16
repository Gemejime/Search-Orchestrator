package usecase

import (
	"context"

	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
)

// ExecuteDiagnosticSearchUseCase executes the same deterministic retrieval cascade as
// ExecuteSearchUseCase while recording per-stage candidate counts, logits, and timings.
type ExecuteDiagnosticSearchUseCase struct {
	search *ExecuteSearchUseCase
}

// NewExecuteDiagnosticSearchUseCase constructs an ExecuteDiagnosticSearchUseCase.
func NewExecuteDiagnosticSearchUseCase(search *ExecuteSearchUseCase) *ExecuteDiagnosticSearchUseCase {
	return &ExecuteDiagnosticSearchUseCase{search: search}
}

// Execute runs the staged retrieval cascade and returns both SearchResponse and ScoreTrace.
func (uc *ExecuteDiagnosticSearchUseCase) Execute(
	ctx context.Context,
	req dto.DiagnosticSearchRequest,
) (dto.DiagnosticSearchResponse, error) {
	return uc.search.runCascade(ctx, dto.SearchRequest(req), true)
}
