package gate

import (
	"context"
	"strings"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/endpoint"
	dueerrors "github.com/dobyte/due/v2/errors"
	transportgate "github.com/dobyte/due/v2/internal/transporter/gate"
	"github.com/dobyte/due/v2/locate"
	"github.com/dobyte/due/v2/registry"
	"github.com/dobyte/due/v2/session"
)

type ControlClientConfig struct {
	ID                string
	Locator           locate.Locator
	Registry          registry.Registry
	ConnNum           int
	CallTimeout       time.Duration
	DialTimeout       time.Duration
	DialRetryTimes    int
	WriteTimeout      time.Duration
	WriteQueueSize    int32
	FaultRecoveryTime time.Duration
}

type controlGateClient interface {
	DisconnectCurrent(
		context.Context,
		session.Kind,
		int64,
		session.Token,
		bool,
	) error
}

type controlGateBuilder func(string) (controlGateClient, error)

type ControlClient struct {
	locator  locate.Locator
	registry registry.Registry
	build    controlGateBuilder
}

func NewControlClient(cfg ControlClientConfig) (*ControlClient, error) {
	normalized, err := normalizeControlClientConfig(cfg)
	if err != nil {
		return nil, err
	}
	builder := transportgate.NewBuilder(&transportgate.ClientOptions{
		ID:                normalized.ID,
		Kind:              cluster.Master,
		ConnNum:           normalized.ConnNum,
		CallTimeout:       normalized.CallTimeout,
		DialTimeout:       normalized.DialTimeout,
		DialRetryTimes:    normalized.DialRetryTimes,
		WriteTimeout:      normalized.WriteTimeout,
		WriteQueueSize:    normalized.WriteQueueSize,
		FaultRecoveryTime: normalized.FaultRecoveryTime,
	})
	return newControlClient(
		normalized,
		func(address string) (controlGateClient, error) {
			return builder.Build(address)
		},
	)
}

func newControlClient(
	cfg ControlClientConfig,
	build controlGateBuilder,
) (*ControlClient, error) {
	normalized, err := normalizeControlClientConfig(cfg)
	if err != nil {
		return nil, err
	}
	if build == nil {
		return nil, dueerrors.ErrInvalidArgument
	}
	return &ControlClient{
		locator:  normalized.Locator,
		registry: normalized.Registry,
		build:    build,
	}, nil
}

func normalizeControlClientConfig(
	cfg ControlClientConfig,
) (ControlClientConfig, error) {
	cfg.ID = strings.TrimSpace(cfg.ID)
	if cfg.ID == "" || cfg.Locator == nil || cfg.Registry == nil {
		return ControlClientConfig{}, dueerrors.ErrInvalidArgument
	}
	if cfg.ConnNum <= 0 {
		cfg.ConnNum = 1
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = 3 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	if cfg.DialRetryTimes < 0 {
		cfg.DialRetryTimes = 0
	}
	if cfg.WriteQueueSize <= 0 {
		cfg.WriteQueueSize = 128
	}
	if cfg.FaultRecoveryTime <= 0 {
		cfg.FaultRecoveryTime = 5 * time.Second
	}
	return cfg, nil
}

func (c *ControlClient) DisconnectCurrent(
	ctx context.Context,
	uid int64,
	token session.Token,
	force bool,
) error {
	if c == nil || c.locator == nil || c.registry == nil || c.build == nil {
		return dueerrors.ErrInvalidArgument
	}
	if uid <= 0 ||
		token.UID != uid ||
		token.Generation == 0 {
		return dueerrors.ErrStaleSession
	}
	if ctx == nil {
		ctx = context.Background()
	}

	gid, err := c.locator.LocateGate(ctx, uid)
	if err != nil {
		return err
	}
	gid = strings.TrimSpace(gid)
	if gid == "" {
		return dueerrors.ErrNotFoundUserLocation
	}

	services, err := c.registry.Services(ctx, cluster.Gate.String())
	if err != nil {
		return err
	}
	var address string
	for _, service := range services {
		if service == nil || service.ID != gid {
			continue
		}
		ep, parseErr := endpoint.ParseEndpoint(service.Endpoint)
		if parseErr != nil {
			return parseErr
		}
		address = strings.TrimSpace(ep.Address())
		if address == "" {
			return dueerrors.ErrNotFoundEndpoint
		}
		break
	}
	if address == "" {
		return dueerrors.ErrNotFoundEndpoint
	}

	client, err := c.build(address)
	if err != nil {
		return err
	}
	return client.DisconnectCurrent(
		ctx,
		session.User,
		uid,
		token,
		force,
	)
}
