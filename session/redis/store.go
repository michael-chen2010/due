package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/session"
	goredis "github.com/redis/go-redis/v9"
)

var _ session.OwnershipStore = (*Store)(nil)

// Store implements session.OwnershipStore using Redis.
// Each UID uses one Redis hash. The generation field is never removed by
// Release, so a later Acquire cannot reuse an older fencing generation.
type Store struct {
	client        goredis.UniversalClient
	builtin       bool
	prefix        string
	acquireScript *goredis.Script
	releaseScript *goredis.Script
}

func NewStore(opts ...Option) *Store {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	store := &Store{
		client:        o.client,
		prefix:        o.prefix,
		acquireScript: goredis.NewScript(acquireScript),
		releaseScript: goredis.NewScript(releaseScript),
	}
	if store.client == nil {
		store.client = goredis.NewUniversalClient(&goredis.UniversalOptions{
			Addrs:      o.addrs,
			DB:         o.db,
			Username:   o.username,
			Password:   o.password,
			MaxRetries: o.maxRetries,
		})
		store.builtin = true
	}
	return store
}

func (s *Store) Acquire(ctx context.Context, uid int64) (session.Token, error) {
	if err := ctx.Err(); err != nil {
		return session.Token{}, err
	}
	if uid <= 0 {
		return session.Token{}, dueerrors.ErrInvalidArgument
	}

	generation, err := s.acquireScript.Run(ctx, s.client, []string{s.key(uid)}).Int64()
	if err != nil {
		if strings.Contains(err.Error(), "increment or decrement would overflow") {
			return session.Token{}, session.ErrOwnershipGenerationExhausted
		}
		return session.Token{}, err
	}
	if generation <= 0 {
		return session.Token{}, session.ErrOwnershipGenerationExhausted
	}

	return session.Token{UID: uid, Generation: uint64(generation)}, nil
}

func (s *Store) Current(ctx context.Context, uid int64) (session.Token, bool, error) {
	if err := ctx.Err(); err != nil {
		return session.Token{}, false, err
	}
	if uid <= 0 {
		return session.Token{}, false, dueerrors.ErrInvalidArgument
	}

	value, err := s.client.HGet(ctx, s.key(uid), "current").Result()
	if err != nil {
		if dueerrors.Is(err, goredis.Nil) {
			return session.Token{}, false, nil
		}
		return session.Token{}, false, err
	}

	generation, err := strconv.ParseUint(value, 10, 64)
	if err != nil || generation == 0 {
		if err != nil {
			return session.Token{}, false, fmt.Errorf("decode session ownership generation %q: %w", value, err)
		}
		return session.Token{}, false, session.ErrOwnershipGenerationExhausted
	}
	return session.Token{UID: uid, Generation: generation}, true, nil
}

func (s *Store) Release(ctx context.Context, uid int64, token session.Token) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if uid <= 0 || token.UID != uid || token.Generation == 0 {
		return false, dueerrors.ErrInvalidArgument
	}

	released, err := s.releaseScript.Run(
		ctx,
		s.client,
		[]string{s.key(uid)},
		strconv.FormatUint(token.Generation, 10),
	).Int()
	if err != nil {
		return false, err
	}
	return released == 1, nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *Store) Close() error {
	if !s.builtin {
		return nil
	}
	return s.client.Close()
}

func (s *Store) key(uid int64) string {
	return fmt.Sprintf("%s:%d", s.prefix, uid)
}
