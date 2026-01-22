package auth

import (
	"context"

	"gitea.jw6.us/james/calcard/internal/store"
)

type contextKey string

const (
	contextKeyUser      contextKey = "user"
	contextKeySessionID contextKey = "session_id"
)

func WithUser(ctx context.Context, user *store.User) context.Context {
	return context.WithValue(ctx, contextKeyUser, user)
}

func UserFromContext(ctx context.Context) (*store.User, bool) {
	u, ok := ctx.Value(contextKeyUser).(*store.User)
	return u, ok
}

func WithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, contextKeySessionID, sessionID)
}

func SessionIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(contextKeySessionID).(string)
	return s
}
