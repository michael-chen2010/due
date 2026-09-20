package session

import (
	"context"
	"math"
	"sync"

	dueerrors "github.com/dobyte/due/v2/errors"
)

// MemoryOwnershipStore is an in-process OwnershipStore implementation.
// It is intended for tests and single-process deployments; distributed
// deployments should use a shared store implementation.
type MemoryOwnershipStore struct {
	mu          sync.RWMutex
	owners      map[int64]Token
	generations map[int64]uint64
}

func NewMemoryOwnershipStore() *MemoryOwnershipStore {
	return &MemoryOwnershipStore{
		owners:      make(map[int64]Token),
		generations: make(map[int64]uint64),
	}
}

func (s *MemoryOwnershipStore) Acquire(ctx context.Context, uid int64) (Token, error) {
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}
	if uid <= 0 {
		return Token{}, dueerrors.ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	currentGeneration := s.generations[uid]
	if currentGeneration == math.MaxUint64 {
		return Token{}, ErrOwnershipGenerationExhausted
	}

	token := Token{UID: uid, Generation: currentGeneration + 1}
	s.generations[uid] = token.Generation
	s.owners[uid] = token
	return token, nil
}

func (s *MemoryOwnershipStore) Current(ctx context.Context, uid int64) (Token, bool, error) {
	if err := ctx.Err(); err != nil {
		return Token{}, false, err
	}
	if uid <= 0 {
		return Token{}, false, dueerrors.ErrInvalidArgument
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	token, ok := s.owners[uid]
	return token, ok, nil
}

func (s *MemoryOwnershipStore) Release(ctx context.Context, uid int64, token Token) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if uid <= 0 || token.UID != uid || token.Generation == 0 {
		return false, dueerrors.ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.owners[uid]
	if !ok || current != token {
		return false, nil
	}

	delete(s.owners, uid)
	return true, nil
}
