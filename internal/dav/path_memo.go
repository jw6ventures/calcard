package dav

import (
	"context"
	"sync"
)

// davPathMemo memoizes canonicalDAVPath resolutions for the duration of a
// single request. Canonicalizing a name/slug segment can hit the database, and
// privilege evaluation canonicalizes the same handful of paths over and over —
// a Depth: 1 PROPFIND re-resolves each collection path once per privilege per
// child. Resolutions are keyed by user because unresolved segments canonicalize
// to per-user pending paths.
//
// Collection create/rename sites (MKCOL, MKCALENDAR, PROPPATCH) call
// invalidateDAVPathMemo so later resolutions in the same request see the new
// name-to-ID binding.
type davPathMemo struct {
	mu    sync.Mutex
	paths map[davPathMemoKey]string
}

type davPathMemoKey struct {
	userID int64
	path   string
}

type davPathMemoKeyType struct{}

var davPathMemoContextKey = davPathMemoKeyType{}

func withDAVPathMemo(ctx context.Context) context.Context {
	return context.WithValue(ctx, davPathMemoContextKey, &davPathMemo{paths: make(map[davPathMemoKey]string)})
}

func davPathMemoFromContext(ctx context.Context) *davPathMemo {
	memo, _ := ctx.Value(davPathMemoContextKey).(*davPathMemo)
	return memo
}

func (m *davPathMemo) get(key davPathMemoKey) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	canonical, ok := m.paths[key]
	return canonical, ok
}

func (m *davPathMemo) put(key davPathMemoKey, canonical string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paths[key] = canonical
}

func (m *davPathMemo) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paths = make(map[davPathMemoKey]string)
}

// invalidateDAVPathMemo drops every memoized path resolution for the current
// request. Any code path that creates or renames a collection must call it so
// later resolutions within the same request see the change.
func invalidateDAVPathMemo(ctx context.Context) {
	if memo := davPathMemoFromContext(ctx); memo != nil {
		memo.invalidate()
	}
}
