package main

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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
	"With":         true,
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

var redactionMarkers = map[string]bool{
	"[redacted]": true,
	"redacted":   true,
	"<redacted>": true,
	"***":        true,
	"****":       true,
}

var fmtPrintMethods = map[string]bool{
	"Print":   true,
	"Printf":  true,
	"Println": true,
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

		dirFiles := make(map[string][]string)
		err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			parent := filepath.Dir(path)
			dirFiles[parent] = append(dirFiles[parent], path)
			return nil
		})
		if err != nil {
			return nil, err
		}

		subDirs := make([]string, 0, len(dirFiles))
		for d := range dirFiles {
			subDirs = append(subDirs, d)
		}
		sort.Strings(subDirs)

		for _, subDir := range subDirs {
			paths := dirFiles[subDir]
			sort.Strings(paths)
			dirFindings, err := scanPackageFiles(fset, paths)
			if err != nil {
				return nil, err
			}
			findings = append(findings, dirFindings...)
		}
	}

	return findings, nil
}

func scanPackageFiles(fset *token.FileSet, paths []string) ([]Finding, error) {
	files := make([]*ast.File, 0, len(paths))
	pkgName := ""
	for _, path := range paths {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", path, err)
		}
		if pkgName == "" {
			pkgName = file.Name.Name
		}
		files = append(files, file)
	}

	typeInfo := &types.Info{
		Types: make(map[ast.Expr]types.TypeAndValue),
		Uses:  make(map[*ast.Ident]types.Object),
		Defs:  make(map[*ast.Ident]types.Object),
	}
	conf := types.Config{
		Importer: importer.Default(),
		Error:    func(error) {},
	}
	_, _ = conf.Check(pkgName, fset, files, typeInfo)

	var findings []Finding
	for i, file := range files {
		fileFindings := inspectFile(fset, paths[i], file, typeInfo)
		findings = append(findings, fileFindings...)
	}
	return findings, nil
}

func inspectFile(fset *token.FileSet, path string, file *ast.File, typeInfo *types.Info) []Finding {
	fileImports := collectFileImports(file)
	var findings []Finding

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if isFmtPrintCall(call, typeInfo, fileImports) {
			pos := fset.Position(call.Pos())
			findings = append(findings, Finding{
				File:   path,
				Line:   pos.Line,
				Reason: "unstructured fmt.Print* call (use structured log/slog)",
			})
			return true
		}

		methodName, isSlog := slogCallMethod(call, typeInfo, fileImports)
		if !isSlog {
			return true
		}

		attrArgs := slogAttrArgs(methodName, call.Args)
		findings = append(findings, inspectSlogArgs(fset, path, attrArgs, typeInfo, fileImports)...)
		return true
	})

	return findings
}

func collectFileImports(file *ast.File) map[string]string {
	imports := make(map[string]string, len(file.Imports))
	for _, imp := range file.Imports {
		importPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		var localName string
		if imp.Name != nil {
			localName = imp.Name.Name
		} else {
			parts := strings.Split(importPath, "/")
			localName = parts[len(parts)-1]
		}
		imports[localName] = importPath
	}
	return imports
}

func slogCallMethod(call *ast.CallExpr, typeInfo *types.Info, fileImports map[string]string) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !slogMethods[sel.Sel.Name] {
		return "", false
	}
	if ident, ok := sel.X.(*ast.Ident); ok {
		if isPkgBinding(ident, "log/slog", typeInfo, fileImports) {
			return sel.Sel.Name, true
		}
	}
	if typeInfo != nil {
		if tv, ok := typeInfo.Types[sel.X]; ok && isSlogLoggerType(tv.Type) {
			return sel.Sel.Name, true
		}
	}
	return "", false
}

func isFmtPrintCall(call *ast.CallExpr, typeInfo *types.Info, fileImports map[string]string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !fmtPrintMethods[sel.Sel.Name] {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return isPkgBinding(ident, "fmt", typeInfo, fileImports)
}

func isPkgBinding(ident *ast.Ident, targetPath string, typeInfo *types.Info, fileImports map[string]string) bool {
	if typeInfo != nil {
		if obj, ok := typeInfo.Uses[ident]; ok {
			if pkgName, ok := obj.(*types.PkgName); ok {
				return pkgName.Imported().Path() == targetPath
			}
			return false
		}
	}
	return fileImports[ident.Name] == targetPath
}

func isSlogLoggerType(t types.Type) bool {
	if t == nil {
		return false
	}
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == "log/slog" && named.Obj().Name() == "Logger"
}

func slogAttrArgs(methodName string, args []ast.Expr) []ast.Expr {
	var start int
	switch methodName {
	case "With":
		start = 0
	case "Info", "Warn", "Error", "Debug":
		start = 1
	case "InfoContext", "WarnContext", "ErrorContext", "DebugContext":
		start = 2
	case "Log", "LogAttrs":
		start = 3
	default:
		start = 0
	}
	if start >= len(args) {
		return nil
	}
	return args[start:]
}

func inspectSlogArgs(fset *token.FileSet, path string, args []ast.Expr, typeInfo *types.Info, fileImports map[string]string) []Finding {
	var findings []Finding
	for i := 0; i < len(args); i++ {
		arg := args[i]

		// Handle slog.<AttrConstructor>(key, val) and slog.Group(name, attrs...).
		if attrCall, ok := arg.(*ast.CallExpr); ok && isSlogAttrCall(attrCall, typeInfo, fileImports) {
			sel := attrCall.Fun.(*ast.SelectorExpr)
			if sel.Sel.Name == "Group" && len(attrCall.Args) > 1 {
				findings = append(findings, inspectSlogArgs(fset, path, attrCall.Args[1:], typeInfo, fileImports)...)
				continue
			}
			if len(attrCall.Args) >= 2 {
				findings = append(findings, checkKeyValuePair(fset, path, attrCall.Args[0], attrCall.Args[1])...)
				findings = append(findings, inspectExprForSensitiveFields(fset, path, attrCall.Args[1])...)
				continue
			}
		}

		// Handle key-value pair arguments: ("key", value).
		if keyLit, ok := arg.(*ast.BasicLit); ok && keyLit.Kind == token.STRING {
			var valExpr ast.Expr
			if i+1 < len(args) {
				valExpr = args[i+1]
			}
			findings = append(findings, checkKeyValuePair(fset, path, keyLit, valExpr)...)
			if valExpr != nil {
				findings = append(findings, inspectExprForSensitiveFields(fset, path, valExpr)...)
				i++
			}
			continue
		}

		// Fallback for non-literal attr expressions (e.g. helper calls or variables).
		findings = append(findings, inspectExprForSensitiveNodes(fset, path, arg)...)
	}
	return findings
}

func isSlogAttrCall(call *ast.CallExpr, typeInfo *types.Info, fileImports map[string]string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return isPkgBinding(ident, "log/slog", typeInfo, fileImports)
}

func checkKeyValuePair(fset *token.FileSet, path string, keyExpr ast.Expr, valExpr ast.Expr) []Finding {
	keyLit, ok := keyExpr.(*ast.BasicLit)
	if !ok || keyLit.Kind != token.STRING {
		return nil
	}
	val, unquoteErr := strconv.Unquote(keyLit.Value)
	if unquoteErr != nil || !sensitiveKeys[strings.ToLower(val)] {
		return nil
	}
	if valExpr != nil && (isRedactedStringExpr(valExpr) || isSafeDerivedExpr(valExpr)) {
		return nil
	}
	pos := fset.Position(keyLit.Pos())
	return []Finding{{
		File:   path,
		Line:   pos.Line,
		Reason: fmt.Sprintf("sensitive log key %q", val),
	}}
}

func isRedactedStringExpr(expr ast.Expr) bool {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	val, err := strconv.Unquote(lit.Value)
	if err != nil {
		return false
	}
	return redactionMarkers[strings.ToLower(strings.TrimSpace(val))]
}

func isSafeDerivedExpr(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	return ident.Name == "len" || ident.Name == "cap"
}

func inspectExprForSensitiveFields(fset *token.FileSet, path string, expr ast.Expr) []Finding {
	var findings []Finding
	ast.Inspect(expr, func(inner ast.Node) bool {
		if call, ok := inner.(*ast.CallExpr); ok && isSafeDerivedExpr(call) {
			return false
		}
		if sel, ok := inner.(*ast.SelectorExpr); ok && sensitiveFields[sel.Sel.Name] {
			pos := fset.Position(sel.Sel.Pos())
			findings = append(findings, Finding{
				File:   path,
				Line:   pos.Line,
				Reason: fmt.Sprintf("sensitive struct field .%s", sel.Sel.Name),
			})
		}
		return true
	})
	return findings
}

func inspectExprForSensitiveNodes(fset *token.FileSet, path string, expr ast.Expr) []Finding {
	var findings []Finding
	ast.Inspect(expr, func(inner ast.Node) bool {
		if call, ok := inner.(*ast.CallExpr); ok && isSafeDerivedExpr(call) {
			return false
		}
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
	return findings
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
