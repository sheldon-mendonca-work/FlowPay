package httpx

import (
	"context"
	"net/http"
)

// Compile-time check that Router implements http.Handler.
var _ http.Handler = (*Router)(nil)

type Router struct {
	mux *http.ServeMux
}

func NewRouter() *Router {
	return &Router{
		mux: http.NewServeMux(),
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

func (r *Router) HandleFunc(pattern string, handler http.HandlerFunc) {
	wrapped := func(w http.ResponseWriter, req *http.Request) {
		ctx := WithRequestMetadata(
			req.Context(),
			&RequestMetadata{
				RoutePattern: pattern,
			},
		)

		handler(w, req.WithContext(ctx))
	}

	r.mux.HandleFunc(pattern, wrapped)
}

func (r *Router) Handle(pattern string, handler http.Handler) {
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := WithRequestMetadata(
			req.Context(),
			&RequestMetadata{
				RoutePattern: pattern,
			},
		)

		handler.ServeHTTP(w, req.WithContext(ctx))
	})

	r.mux.Handle(pattern, wrapped)
}

// -----------------------------------------------------------------------------
// Request Metadata
// -----------------------------------------------------------------------------

type contextKey struct{}

type RequestMetadata struct {
	RoutePattern string
}

func WithRequestMetadata(
	ctx context.Context,
	metadata *RequestMetadata,
) context.Context {
	return context.WithValue(ctx, contextKey{}, metadata)
}

func GetRequestMetadata(
	ctx context.Context,
) (*RequestMetadata, bool) {
	metadata, ok := ctx.Value(contextKey{}).(*RequestMetadata)
	return metadata, ok
}

func GetRoutePattern(
	ctx context.Context,
) (string, bool) {
	metadata, ok := GetRequestMetadata(ctx)
	if !ok || metadata == nil {
		return "", false
	}

	return metadata.RoutePattern, true
}
