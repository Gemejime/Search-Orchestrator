package openapi

import _ "embed"

// SpecYAML holds the embedded OpenAPI 3.1 YAML specification so distroless containers
// with readOnlyRootFilesystem: true can serve /openapi.yaml without filesystem reads.
//
//go:embed openapi.yaml
var SpecYAML []byte
