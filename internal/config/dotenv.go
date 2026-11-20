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
		return fmt.Errorf("open env file %q: %w", cleanPath, err)
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
			return fmt.Errorf("parse env file %q line %d: missing '='", cleanPath, lineNum)
		}

		key := strings.TrimSpace(line[:eqIdx])
		if key == "" {
			return fmt.Errorf("parse env file %q line %d: empty key", cleanPath, lineNum)
		}

		val := parseEnvValue(strings.TrimSpace(line[eqIdx+1:]))
		if _, exists := os.LookupEnv(key); !exists {
			if setErr := os.Setenv(key, val); setErr != nil {
				return fmt.Errorf("set env %q: %w", key, setErr)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan env file %q: %w", cleanPath, err)
	}
	return nil
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
	if len(raw) >= 2 {
		quote := raw[0]
		if (quote == '"' || quote == '\'') && raw[len(raw)-1] == quote {
			inner := raw[1 : len(raw)-1]
			if quote == '"' {
				inner = strings.ReplaceAll(inner, `\"`, `"`)
				inner = strings.ReplaceAll(inner, `\n`, "\n")
				inner = strings.ReplaceAll(inner, `\r`, "\r")
				inner = strings.ReplaceAll(inner, `\t`, "\t")
				inner = strings.ReplaceAll(inner, `\\`, `\`)
			}
			return inner
		}
	}

	if idx := strings.Index(raw, " #"); idx >= 0 {
		raw = raw[:idx]
	}
	return strings.TrimSpace(raw)
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
