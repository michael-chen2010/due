package redis

import goredis "github.com/redis/go-redis/v9"

const (
	defaultAddr       = "127.0.0.1:6379"
	defaultDB         = 0
	defaultMaxRetries = 3
	defaultPrefix     = "due:session:ownership"
)

type Option func(*options)

type options struct {
	addrs      []string
	db         int
	username   string
	password   string
	maxRetries int
	client     goredis.UniversalClient
	prefix     string
}

func defaultOptions() *options {
	return &options{
		addrs:      []string{defaultAddr},
		db:         defaultDB,
		maxRetries: defaultMaxRetries,
		prefix:     defaultPrefix,
	}
}

func WithAddrs(addrs ...string) Option {
	return func(o *options) {
		if len(addrs) > 0 {
			o.addrs = append([]string(nil), addrs...)
		}
	}
}

func WithDB(db int) Option {
	return func(o *options) { o.db = db }
}

func WithUsername(username string) Option {
	return func(o *options) { o.username = username }
}

func WithPassword(password string) Option {
	return func(o *options) { o.password = password }
}

func WithMaxRetries(maxRetries int) Option {
	return func(o *options) { o.maxRetries = maxRetries }
}

func WithClient(client goredis.UniversalClient) Option {
	return func(o *options) { o.client = client }
}

func WithPrefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}
