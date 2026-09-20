package session

import (
	"context"
	stderrors "errors"
)

var ErrOwnershipGenerationExhausted = stderrors.New("session ownership generation exhausted")

// OwnershipStore coordinates the current session token for a UID.
// Implementations must allocate a generation that is newer than every
// generation previously allocated for the same UID.
type OwnershipStore interface {
	Acquire(ctx context.Context, uid int64) (Token, error)
	Current(ctx context.Context, uid int64) (Token, bool, error)
	Release(ctx context.Context, uid int64, token Token) (bool, error)
}
