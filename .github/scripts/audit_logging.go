package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var slogMethods = map[string]bool{
	"Info":         true,
	"Warn":         true,
	"Error":        true,
	"Debug":        true,
	"Log":          true,
	"LogAttrs":     true,
	"InfoContext":  true,
	"WarnContext":  true,
	"ErrorContext": true,
	"DebugContext": true,
}

var sensitiveKeys = map[string]bool{
	"authorization":     true,
	"bearer":            true,
	"caller_acl_bitmap": true,
	"password":          true,
	"secret_key":        true,
	"api_key":           true,
	"dsn":               true,
}

var sensitiveFields = map[string]bool{
	"CallerAclBitmap": true,
	"Authorization":   true,
	"Password":        true,
	"SecretKey":       true,
	"ApiKey":          true,
	"DSN":             true,
}

// Finding represents a single secured-logging violation in a Go source file.
type Finding struct {
	File   string
	Line   int
	Reason string
}

func scanDirs(dirs []string) ([]Finding, error) {
	var findings []Finding
	fset := token.NewFileSet()

	for _, dir := range dirs {
		info, err := os.Stat(dir)
		if err != nil {
			return nil, fmt.Errorf("stat %q: %w", dir, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%q is not a directory", dir)
		}

		err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			fileFindings, err := scanFile(fset, path)
			if err != nil {
				return err
			}
			findings = append(findings, fileFindings...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return findings, nil
}

func scanFile(fset *token.FileSet, path string) ([]Finding, error) {
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}

	var findings []Finding
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isSlogCall(call) {
			return true
		}

		for _, arg := range call.Args {
			ast.Inspect(arg, func(inner ast.Node) bool {
				switch node := inner.(type) {
				case *ast.BasicLit:
					if node.Kind == token.STRING {
						val, unquoteErr := strconv.Unquote(node.Value)
						if unquoteErr == nil && sensitiveKeys[strings.ToLower(val)] {
							pos := fset.Position(node.Pos())
							findings = append(findings, Finding{
								File:   path,
								Line:   pos.Line,
								Reason: fmt.Sprintf("sensitive log key %q", val),
							})
						}
					}
				case *ast.SelectorExpr:
					if sensitiveFields[node.Sel.Name] {
						pos := fset.Position(node.Sel.Pos())
						findings = append(findings, Finding{
							File:   path,
							Line:   pos.Line,
							Reason: fmt.Sprintf("sensitive struct field .%s", node.Sel.Name),
						})
					}
				}
				return true
			})
		}
		return true
	})

	return findings, nil
}

func isSlogCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !slogMethods[sel.Sel.Name] {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "slog"
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: audit_logging <dir> [<dir>...]")
		os.Exit(2)
	}

	findings, err := scanDirs(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: secured-logging audit failed: %v\n", err)
		os.Exit(2)
	}

	if len(findings) > 0 {
		for _, f := range findings {
			fmt.Fprintf(os.Stderr, "%s:%d: %s\n", f.File, f.Line, f.Reason)
		}
		fmt.Fprintln(os.Stderr, "ERROR: Potential unredacted sensitive field logged. Redact credentials, tokens, and raw ACL bitmaps before emission.")
		os.Exit(1)
	}
}
