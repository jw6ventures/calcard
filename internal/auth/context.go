package auth

import (
	"context"

	"github.com/example/calcard/internal/store"
)

// contextKey marks values stored on the request context.
type contextKey string

const (
	contextKeyUser contextKey = "user"
)

// WithUser attaches a user to the context for downstream handlers.
func WithUser(ctx context.Context, user *store.User) context.Context {
	return context.WithValue(ctx, contextKeyUser, user)
}

// UserFromContext retrieves the authenticated user from context when present.
func UserFromContext(ctx context.Context) (*store.User, bool) {
	u, ok := ctx.Value(contextKeyUser).(*store.User)
	return u, ok
}
