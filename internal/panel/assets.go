package panel

import (
	"bytes"
	_ "embed"
	"net/http"
)

//go:embed assets/index.html
var indexHTML []byte

// StaticPage is public shell only. No configuration, credential, record or
// statistics are injected; every data request goes through management auth.
func StaticPage(apiBase string) Response {
	body := bytes.ReplaceAll(indexHTML, []byte("__CPA_DISTILL_API_BASE__"), []byte(apiBase))
	return Response{StatusCode: 200, Headers: http.Header{
		"Content-Type":            {"text/html; charset=utf-8"},
		"Cache-Control":           {"no-store"},
		"X-Content-Type-Options":  {"nosniff"},
		"Referrer-Policy":         {"no-referrer"},
		"Content-Security-Policy": {"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'"},
	}, Body: body}
}
