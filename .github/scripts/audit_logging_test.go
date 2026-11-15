package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanDirs_MultilineSensitiveLoggingRegression(t *testing.T) {
	tmpDir := t.TempDir()

	badFile := filepath.Join(tmpDir, "handler.go")
	src := `package handler

import "log/slog"

type Request struct {
	Authorization string
}

func Handle(req Request) {
	slog.Info(
		"incoming request",
		"authorization",
		req.Authorization,
	)
}
`
	if err := os.WriteFile(badFile, []byte(src), 0o600); err != nil {
		t.Fatalf("write badFile: %v", err)
	}

	findings, err := scanDirs([]string{tmpDir})
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("expected 2 findings (key + struct field) in multiline slog call, got %d: %+v", len(findings), findings)
	}
}

func TestScanDirs_SafeMetadataAllowed(t *testing.T) {
	tmpDir := t.TempDir()

	goodFile := filepath.Join(tmpDir, "safe.go")
	src := `package safe

import "log/slog"

func LogSafe(bitmapLen int) {
	slog.Info(
		"acl evaluated",
		"caller_acl_bitmap_len", bitmapLen,
	)
}
`
	if err := os.WriteFile(goodFile, []byte(src), 0o600); err != nil {
		t.Fatalf("write goodFile: %v", err)
	}

	findings, err := scanDirs([]string{tmpDir})
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings for safe metadata key, got %+v", findings)
	}
}

func TestScanDirs_MissingDirFailsScan(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := scanDirs([]string{missing}); err == nil {
		t.Fatal("expected error for missing directory, got nil")
	}
}
