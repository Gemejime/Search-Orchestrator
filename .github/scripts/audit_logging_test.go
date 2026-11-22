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

func TestScanDirs_FmtPrintForbidden(t *testing.T) {
	tmpDir := t.TempDir()

	badFile := filepath.Join(tmpDir, "print.go")
	src := `package handler

import "fmt"

func Debug() {
	fmt.Println("debug")
}
`
	if err := os.WriteFile(badFile, []byte(src), 0o600); err != nil {
		t.Fatalf("write badFile: %v", err)
	}

	findings, err := scanDirs([]string{tmpDir})
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding for fmt.Println, got %d: %+v", len(findings), findings)
	}
}

func TestScanDirs_WithPersistentAttributesFlagged(t *testing.T) {
	tmpDir := t.TempDir()

	badFile := filepath.Join(tmpDir, "with_auth.go")
	src := `package handler

import "log/slog"

type Request struct {
	Authorization string
}

func Handle(logger *slog.Logger, req Request) {
	reqLogger := logger.With("authorization", req.Authorization)
	reqLogger.Info("request completed")
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
		t.Fatalf("expected 2 findings (key + struct field) in logger.With call, got %d: %+v", len(findings), findings)
	}
}

func TestScanDirs_SlogLoggerInstanceAndAliasedFmt(t *testing.T) {
	tmpDir := t.TempDir()

	badFile := filepath.Join(tmpDir, "aliased.go")
	src := `package handler

import (
	myfmt "fmt"
	"log/slog"
)

type Request struct {
	Authorization string
}

func Handle(logger *slog.Logger, req Request) {
	logger.Warn("unauthorized", "authorization", req.Authorization)
	myfmt.Printf("debug: %s\n", "oops")
}
`
	if err := os.WriteFile(badFile, []byte(src), 0o600); err != nil {
		t.Fatalf("write badFile: %v", err)
	}

	findings, err := scanDirs([]string{tmpDir})
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if len(findings) != 3 {
		t.Fatalf("expected 3 findings (slog.Logger key+field and aliased fmt.Printf), got %d: %+v", len(findings), findings)
	}
}

func TestScanDirs_RedactedMarkersAndBitmapLengthAllowed(t *testing.T) {
	tmpDir := t.TempDir()

	goodFile := filepath.Join(tmpDir, "redacted_and_len.go")
	src := `package safe

import "log/slog"

type Request struct {
	Authorization   string
	CallerAclBitmap []byte
}

func LogSafe(logger *slog.Logger, req Request) {
	slog.Info("auth header", "authorization", "[REDACTED]")
	logger.Info("acl metadata", "caller_acl_bitmap", len(req.CallerAclBitmap))
	logger.With("caller_acl_bitmap_len", len(req.CallerAclBitmap)).Info("ok")
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
		t.Fatalf("expected 0 findings for redaction marker and safe bitmap len, got %+v", findings)
	}
}

func TestScanDirs_NestedKeyValueAndMessageOnlySensitiveFields(t *testing.T) {
	tmpDir := t.TempDir()

	badFile := filepath.Join(tmpDir, "nested_and_msg.go")
	src := `package handler

import "log/slog"

type Request struct {
	Authorization string
}

func Handle(req Request, token string) {
	slog.Info(req.Authorization)
	slog.Info("kv path", "meta", map[string]any{"authorization": token})
	slog.Info("attr path", slog.Any("meta", map[string]any{"authorization": token}))
	slog.Info("redacted nested", "meta", map[string]any{"authorization": "[REDACTED]"})
	slog.Info("redacted nested attr", slog.Any("meta", map[string]any{"authorization": "[REDACTED]"}))
}
`
	if err := os.WriteFile(badFile, []byte(src), 0o600); err != nil {
		t.Fatalf("write badFile: %v", err)
	}

	findings, err := scanDirs([]string{tmpDir})
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if len(findings) != 3 {
		t.Fatalf("expected 3 findings (message-only .Authorization + 2 nested unredacted authorization keys), got %d: %+v", len(findings), findings)
	}
}

func TestScanDirs_GroupValueSensitiveSelectorAndRedaction(t *testing.T) {
	tmpDir := t.TempDir()

	testFile := filepath.Join(tmpDir, "group_value.go")
	src := `package handler

import "log/slog"

type Request struct {
	Authorization string
	Password      string
}

func Handle(req Request) {
	slog.Info("nested group value",
		slog.Any("meta", slog.GroupValue(
			slog.String("first", req.Authorization),
			slog.String("authorization", "[REDACTED]"),
			slog.String("second", req.Password),
		)),
	)
}
`
	if err := os.WriteFile(testFile, []byte(src), 0o600); err != nil {
		t.Fatalf("write testFile: %v", err)
	}

	findings, err := scanDirs([]string{tmpDir})
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("expected 2 findings (Authorization in arg 0 + RedisPassword in arg 2, while redacted authorization is exempted), got %d: %+v", len(findings), findings)
	}
}
