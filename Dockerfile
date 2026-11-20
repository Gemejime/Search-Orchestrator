# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/search-orchestrator ./cmd/search-orchestrator

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/search-orchestrator /search-orchestrator

USER 65532:65532

EXPOSE 8080 50051

ENTRYPOINT ["/search-orchestrator"]
