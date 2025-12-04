package auth

import (
	"context"

	"gitea.jw6.us/james/calcard/internal/store"
)

// contextKey marks values stored on the request context.
type contextKey string

const (
	contextKeyUser      contextKey = "user"
	contextKeySessionID contextKey = "session_id"
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

// WithSessionID attaches a session ID to the context.
func WithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, contextKeySessionID, sessionID)
}

// SessionIDFromContext retrieves the session ID from context when present.
func SessionIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(contextKeySessionID).(string)
	return s
}
