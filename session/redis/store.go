package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/session"
	goredis "github.com/redis/go-redis/v9"
)

var _ session.OwnershipStore = (*Store)(nil)

// Store implements session.OwnershipStore using Redis.
// Each UID uses one Redis hash backed by durable generation ranges. Release
// keeps the range; complete hash loss must reserve a new range before issuance.
type Store struct {
	client           goredis.UniversalClient
	builtin          bool
	prefix           string
	rangeAllocator   GenerationRangeAllocator
	initializeScript *goredis.Script
	acquireScript    *goredis.Script
	releaseScript    *goredis.Script
}

func NewStore(opts ...Option) *Store {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	store := &Store{
		client:           o.client,
		rangeAllocator:   o.rangeAllocator,
		initializeScript: goredis.NewScript(initializeRangeScript),
		prefix:           o.prefix,
		acquireScript:    goredis.NewScript(acquireScript),
		releaseScript:    goredis.NewScript(releaseScript),
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

	values, err := s.acquireScript.Run(ctx, s.client, []string{s.key(uid)}).StringSlice()
	if err != nil {
		return session.Token{}, err
	}
	generation, needRange, err := decodeRangeReply(values)
	if err != nil {
		return session.Token{}, err
	}
	if !needRange {
		return session.Token{UID: uid, Generation: generation}, nil
	}
	if generation == math.MaxInt64 {
		return session.Token{}, session.ErrOwnershipGenerationExhausted
	}
	if s.rangeAllocator == nil {
		return session.Token{}, ErrGenerationRangeRequired
	}
	floor := generation
	calls := 0
	err = s.rangeAllocator.WithGenerationRange(ctx, uid, floor, func(operation context.Context, start, end uint64) error {
		calls++
		if calls != 1 || operation == nil || start <= floor || start == 0 || end < start || end > math.MaxInt64 {
			return ErrInvalidGenerationRange
		}
		reply, e := s.initializeScript.Run(operation, s.client, []string{s.key(uid)}, strconv.FormatUint(start, 10), strconv.FormatUint(end, 10)).StringSlice()
		if e != nil {
			return e
		}
		var needs bool
		generation, needs, e = decodeRangeReply(reply)
		if e != nil {
			return e
		}
		if needs || generation <= floor {
			return ErrInvalidGenerationRange
		}
		return nil
	})
	if err != nil {
		return session.Token{}, err
	}
	if calls != 1 || generation == 0 {
		return session.Token{}, ErrInvalidGenerationRange
	}
	if err := ctx.Err(); err != nil {
		return session.Token{}, err
	}
	return session.Token{UID: uid, Generation: generation}, nil
}

var (
	ErrGenerationRangeRequired = errors.New("session ownership requires a durable generation range allocator")
	ErrInvalidGenerationRange  = errors.New("invalid session ownership generation range or allocator reply")
)

// GenerationRangeAllocator must commit a never-reused inclusive range before
// invoking use, and hold a UID-exclusive lock through use. The callback is
// synchronous and must run exactly once on success, with the operation budget.
type GenerationRangeAllocator interface {
	WithGenerationRange(context.Context, int64, uint64, func(context.Context, uint64, uint64) error) error
}

func decodeRangeReply(values []string) (uint64, bool, error) {
	if len(values) != 2 || (values[0] != "ok" && values[0] != "range") {
		return 0, false, ErrInvalidGenerationRange
	}
	value, e := strconv.ParseUint(values[1], 10, 63)
	if e != nil || strconv.FormatUint(value, 10) != values[1] || (values[0] == "ok" && value == 0) {
		return 0, false, ErrInvalidGenerationRange
	}
	return value, values[0] == "range", nil
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

	generation, err := strconv.ParseUint(value, 10, 63)
	if err != nil || generation == 0 || strconv.FormatUint(generation, 10) != value {
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
