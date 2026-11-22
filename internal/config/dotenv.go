package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

// DefaultEnvFile is the default path checked for local environment overrides.
const DefaultEnvFile = ".env"

// LoadDotEnv reads key=value pairs from path and sets any variable that is not
// already present in the process environment. Missing files are ignored so
// containerized deployments without a .env file start normally.
func LoadDotEnv(path string) error {
	if strings.TrimSpace(path) == "" {
		path = DefaultEnvFile
	}
	cleanPath := filepath.Clean(path)

	f, err := os.Open(cleanPath) // #nosec G304 -- path is the configured local .env file
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open env file: %s", classifyOpenError(err))
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if lineNum == 1 {
			line = strings.TrimPrefix(line, "\uFEFF")
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		eqIdx := strings.IndexByte(line, '=')
		if eqIdx <= 0 {
			return fmt.Errorf("parse env file line %d: missing '='", lineNum)
		}

		key := strings.TrimSpace(line[:eqIdx])
		if key == "" {
			return fmt.Errorf("parse env file line %d: empty key", lineNum)
		}

		val := parseEnvValue(strings.TrimSpace(line[eqIdx+1:]))
		if _, exists := os.LookupEnv(key); !exists {
			if setErr := os.Setenv(key, val); setErr != nil {
				return fmt.Errorf("set env %q: invalid value", key)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return errors.New("scan env file: line too long")
		}
		return errors.New("scan env file: read error")
	}
	return nil
}

func classifyOpenError(err error) string {
	switch {
	case errors.Is(err, os.ErrPermission):
		return "permission denied"
	default:
		return "open failed"
	}
}

// ApplyRuntimeEnv applies Go runtime tuning variables (GOMEMLIMIT, GOGC) when
// they are present in the environment (for example, after loading .env).
func ApplyRuntimeEnv() {
	if raw := strings.TrimSpace(os.Getenv("GOGC")); raw != "" {
		if strings.EqualFold(raw, "off") {
			debug.SetGCPercent(-1)
		} else if pct, err := strconv.Atoi(raw); err == nil {
			debug.SetGCPercent(pct)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("GOMEMLIMIT")); raw != "" {
		if limit, ok := parseByteSize(raw); ok {
			debug.SetMemoryLimit(limit)
		}
	}
}

func parseEnvValue(raw string) string {
	if len(raw) >= 2 && (raw[0] == '"' || raw[0] == '\'') {
		quote := raw[0]
		if endIdx, ok := findClosingQuote(raw, quote); ok {
			rest := strings.TrimSpace(raw[endIdx+1:])
			if rest == "" || strings.HasPrefix(rest, "#") {
				inner := raw[1:endIdx]
				if quote == '"' {
					return unescapeDoubleQuoted(inner)
				}
				return inner
			}
		}
	}

	if idx := strings.Index(raw, " #"); idx >= 0 {
		raw = raw[:idx]
	}
	return strings.TrimSpace(raw)
}

func findClosingQuote(raw string, quote byte) (int, bool) {
	escaped := false
	for i := 1; i < len(raw); i++ {
		if quote == '"' && escaped {
			escaped = false
			continue
		}
		if quote == '"' && raw[i] == '\\' {
			escaped = true
			continue
		}
		if raw[i] == quote {
			return i, true
		}
	}
	return 0, false
}

func unescapeDoubleQuoted(inner string) string {
	var b strings.Builder
	b.Grow(len(inner))
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\\' && i+1 < len(inner) {
			switch inner[i+1] {
			case '"':
				b.WriteByte('"')
				i++
				continue
			case 'n':
				b.WriteByte('\n')
				i++
				continue
			case 'r':
				b.WriteByte('\r')
				i++
				continue
			case 't':
				b.WriteByte('\t')
				i++
				continue
			case '\\':
				b.WriteByte('\\')
				i++
				continue
			}
		}
		b.WriteByte(inner[i])
	}
	return b.String()
}

func parseByteSize(raw string) (int64, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, false
	}

	type suffixMul struct {
		suffix string
		mul    int64
	}
	suffixes := []suffixMul{
		{"TiB", 1 << 40},
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"TB", 1000 * 1000 * 1000 * 1000},
		{"GB", 1000 * 1000 * 1000},
		{"MB", 1000 * 1000},
		{"KB", 1000},
		{"B", 1},
	}

	for _, sm := range suffixes {
		if strings.HasSuffix(s, sm.suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(s, sm.suffix))
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil || n <= 0 {
				return 0, false
			}
			return n * sm.mul, true
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
