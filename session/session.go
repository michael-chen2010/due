package session

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/network"
	"golang.org/x/sync/errgroup"
)

const (
	Conn Kind = iota + 1 // 连接SESSION
	User                 // 用户SESSION
)

type Kind int

// Token identifies one binding generation of a user session.
// A zero Token is never current.
type Token struct {
	UID        int64
	Generation uint64
}

func (k Kind) String() string {
	switch k {
	case Conn:
		return "conn"
	case User:
		return "user"
	}

	return ""
}

type Session struct {
	rw          sync.RWMutex                         // 读写锁
	conns       map[int64]network.Conn               // 连接会话（连接ID -> network.Conn）
	users       map[int64]network.Conn               // 用户会话（用户ID -> network.Conn）
	tokens      map[int64]Token                      // 连接绑定令牌（连接ID -> Token）
	generations map[int64]uint64                     // 用户会话代次（用户ID -> generation）
	channels    map[string]map[network.Conn]struct{} // 会话频道（频道名 -> [network.Conn --> none]）
}

func NewSession() *Session {
	return &Session{
		conns:       make(map[int64]network.Conn),
		users:       make(map[int64]network.Conn),
		tokens:      make(map[int64]Token),
		generations: make(map[int64]uint64),
		channels:    make(map[string]map[network.Conn]struct{}),
	}
}

// AddConn 添加连接
func (s *Session) AddConn(conn network.Conn) {
	s.rw.Lock()
	defer s.rw.Unlock()

	cid, uid := conn.ID(), conn.UID()

	s.conns[cid] = conn

	if uid != 0 {
		generation := s.nextGeneration(uid)
		s.users[uid] = conn
		s.tokens[cid] = Token{UID: uid, Generation: generation}
	}
}

// RemConn 移除连接
func (s *Session) RemConn(conn network.Conn) {
	s.rw.Lock()
	defer s.rw.Unlock()

	cid, uid := conn.ID(), conn.UID()

	delete(s.conns, cid)
	delete(s.tokens, cid)

	if uid != 0 {
		if current, ok := s.users[uid]; ok && current == conn {
			delete(s.users, uid)
		}
	}

	conn.Attr().Visit(func(channel, _ any) bool {
		s.doUnsubscribe(channel.(string), conn)

		return true
	})
}

// Has 是否存在会话
func (s *Session) Has(kind Kind, target int64) (ok bool, err error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	switch kind {
	case Conn:
		_, ok = s.conns[target]
	case User:
		_, ok = s.users[target]
	default:
		err = errors.ErrInvalidSessionKind
	}

	return
}

// Bind 绑定用户ID，并由本地 Session 分配 generation。
func (s *Session) Bind(cid, uid int64) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	conn, err := s.conn(Conn, cid)
	if err != nil {
		return err
	}
	if conn.UID() == uid {
		return nil
	}

	token := Token{UID: uid, Generation: s.nextGeneration(uid)}
	s.bindToken(conn, uid, token)
	return nil
}

// BindToken 绑定由外部 OwnershipStore 分配的 Session Token。
func (s *Session) BindToken(cid, uid int64, token Token) error {
	if uid <= 0 || token.UID != uid || token.Generation == 0 {
		return errors.ErrInvalidArgument
	}

	s.rw.Lock()
	defer s.rw.Unlock()

	conn, err := s.conn(Conn, cid)
	if err != nil {
		return err
	}

	s.bindToken(conn, uid, token)
	if token.Generation > s.generations[uid] {
		s.generations[uid] = token.Generation
	}
	return nil
}

func (s *Session) bindToken(conn network.Conn, uid int64, token Token) {
	cid := conn.ID()
	if oldUID := conn.UID(); oldUID != 0 {
		if oldUID == uid {
			if current, ok := s.users[uid]; ok && current == conn {
				s.tokens[cid] = token
				return
			}
		} else if current, ok := s.users[oldUID]; ok && current == conn {
			delete(s.users, oldUID)
		}
	}

	if oldConn, ok := s.users[uid]; ok && oldConn != conn {
		oldConn.Unbind()
	}

	conn.Bind(uid)
	s.users[uid] = conn
	s.tokens[cid] = token
}

// Unbind 解绑用户ID
func (s *Session) Unbind(uid int64) (int64, error) {
	s.rw.Lock()
	defer s.rw.Unlock()

	conn, err := s.conn(User, uid)
	if err != nil {
		return 0, err
	}

	conn.Unbind()
	delete(s.users, uid)

	return conn.ID(), nil
}

// Token returns the latest binding token associated with a connection or user session.
// An existing but currently unbound connection returns the last token it held.
func (s *Session) Token(kind Kind, target int64) (Token, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	conn, err := s.conn(kind, target)
	if err != nil {
		return Token{}, err
	}

	return s.tokens[conn.ID()], nil
}

// IsCurrent reports whether token identifies the currently bound session for its UID.
func (s *Session) IsCurrent(token Token) bool {
	if token.UID == 0 || token.Generation == 0 {
		return false
	}

	s.rw.RLock()
	defer s.rw.RUnlock()

	conn, ok := s.users[token.UID]
	if !ok {
		return false
	}

	return s.tokens[conn.ID()] == token
}

// LocalIP 获取本地IP
func (s *Session) LocalIP(kind Kind, target int64) (string, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	conn, err := s.conn(kind, target)
	if err != nil {
		return "", err
	}

	return conn.LocalIP()
}

// LocalAddr 获取本地地址
func (s *Session) LocalAddr(kind Kind, target int64) (net.Addr, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	conn, err := s.conn(kind, target)
	if err != nil {
		return nil, err
	}

	return conn.LocalAddr()
}

// RemoteIP 获取远端IP
func (s *Session) RemoteIP(kind Kind, target int64) (string, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	conn, err := s.conn(kind, target)
	if err != nil {
		return "", err
	}

	return conn.RemoteIP()
}

// RemoteAddr 获取远端地址
func (s *Session) RemoteAddr(kind Kind, target int64) (net.Addr, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	conn, err := s.conn(kind, target)
	if err != nil {
		return nil, err
	}

	return conn.RemoteAddr()
}

// Close 关闭会话
func (s *Session) Close(kind Kind, target int64, force ...bool) error {
	s.rw.RLock()
	conn, err := s.conn(kind, target)
	s.rw.RUnlock()

	if err != nil {
		return err
	}

	return conn.Close(force...)
}

// Send 发送消息（同步）
func (s *Session) Send(kind Kind, target int64, message []byte) error {
	s.rw.RLock()
	defer s.rw.RUnlock()

	conn, err := s.conn(kind, target)
	if err != nil {
		return err
	}

	return conn.Send(message)
}

// Push 推送消息（异步）
func (s *Session) Push(kind Kind, target int64, disconnect bool, message []byte) error {
	s.rw.RLock()
	conn, err := s.conn(kind, target)
	s.rw.RUnlock()

	if err != nil {
		return err
	}

	if err = conn.Push(message); err != nil {
		return err
	}

	if disconnect {
		return conn.Close()
	} else {
		return nil
	}
}

// Multicast 推送组播消息（异步）
func (s *Session) Multicast(kind Kind, targets []int64, disconnect bool, message []byte) (int64, error) {
	if len(targets) == 0 {
		return 0, nil
	}

	var (
		total int64
		conns map[int64]network.Conn
		eg, _ = errgroup.WithContext(context.Background())
	)

	s.rw.RLock()

	switch kind {
	case Conn:
		conns = s.conns
	case User:
		conns = s.users
	default:
		s.rw.RUnlock()
		return 0, errors.ErrInvalidSessionKind
	}

	for _, target := range targets {
		conn, ok := conns[target]
		if !ok {
			continue
		}

		eg.Go(func() error {
			if err := conn.Push(message); err != nil {
				return err
			}

			atomic.AddInt64(&total, 1)

			if disconnect {
				_ = conn.Close()
			}

			return nil
		})
	}

	s.rw.RUnlock()

	if err := eg.Wait(); err != nil && total == 0 {
		return 0, err
	} else {
		return total, nil
	}
}

// Broadcast 推送广播消息（异步）
func (s *Session) Broadcast(kind Kind, disconnect bool, message []byte) (int64, error) {
	var (
		total int64
		conns map[int64]network.Conn
		eg, _ = errgroup.WithContext(context.Background())
	)

	s.rw.RLock()

	switch kind {
	case Conn:
		conns = s.conns
	case User:
		conns = s.users
	default:
		s.rw.RUnlock()
		return 0, errors.ErrInvalidSessionKind
	}

	for i := range conns {
		conn := conns[i]

		eg.Go(func() error {
			if err := conn.Push(message); err != nil {
				return err
			}

			atomic.AddInt64(&total, 1)

			if disconnect {
				_ = conn.Close()
			}

			return nil
		})
	}

	s.rw.RUnlock()

	if err := eg.Wait(); err != nil && total == 0 {
		return 0, err
	} else {
		return total, nil
	}
}

// Publish 发布频道消息（异步）
func (s *Session) Publish(channel string, disconnect bool, message []byte) (int64, error) {
	var (
		total int64
		eg, _ = errgroup.WithContext(context.Background())
	)

	s.rw.RLock()

	channels, ok := s.channels[channel]
	if !ok {
		s.rw.RUnlock()
		return 0, nil
	}

	for c := range channels {
		conn := c

		eg.Go(func() error {
			if err := conn.Push(message); err != nil {
				return err
			}

			atomic.AddInt64(&total, 1)

			if disconnect {
				_ = conn.Close()
			}

			return nil
		})
	}

	s.rw.RUnlock()

	if err := eg.Wait(); err != nil && total == 0 {
		return 0, err
	} else {
		return total, nil
	}
}

// Subscribe 订阅频道
func (s *Session) Subscribe(kind Kind, targets []int64, channel string) (err error) {
	if len(targets) == 0 {
		return
	}

	s.rw.Lock()
	defer s.rw.Unlock()

	var conns map[int64]network.Conn
	switch kind {
	case Conn:
		conns = s.conns
	case User:
		conns = s.users
	default:
		err = errors.ErrInvalidSessionKind
		return
	}

	for _, target := range targets {
		conn, ok := conns[target]
		if !ok {
			continue
		}

		conn.Attr().Set(channel, struct{}{})

		if channels, ok := s.channels[channel]; ok {
			channels[conn] = struct{}{}
		} else {
			channels = make(map[network.Conn]struct{}, len(targets))
			channels[conn] = struct{}{}
			s.channels[channel] = channels
		}
	}

	return
}

// Unsubscribe 取消订阅频道
func (s *Session) Unsubscribe(kind Kind, targets []int64, channel string) (err error) {
	if len(targets) == 0 {
		return
	}

	s.rw.Lock()
	defer s.rw.Unlock()

	var conns map[int64]network.Conn
	switch kind {
	case Conn:
		conns = s.conns
	case User:
		conns = s.users
	default:
		err = errors.ErrInvalidSessionKind
		return
	}

	for _, target := range targets {
		if conn, ok := conns[target]; ok {
			if ok = conn.Attr().Del(channel); ok {
				s.doUnsubscribe(channel, conn)
			}
		}
	}

	return
}

// 取消订阅频道
func (s *Session) doUnsubscribe(channel string, conn network.Conn) {
	if channels, ok := s.channels[channel]; ok {
		delete(channels, conn)

		if len(channels) == 0 {
			delete(s.channels, channel)
		}
	}
}

// Stat 统计会话总数
func (s *Session) Stat(kind Kind) (int64, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	switch kind {
	case Conn:
		return int64(len(s.conns)), nil
	case User:
		return int64(len(s.users)), nil
	default:
		return 0, errors.ErrInvalidSessionKind
	}
}

func (s *Session) nextGeneration(uid int64) uint64 {
	generation := s.generations[uid] + 1
	if generation == 0 {
		generation++
	}
	s.generations[uid] = generation
	return generation
}

// 获取会话
func (s *Session) conn(kind Kind, target int64) (network.Conn, error) {
	switch kind {
	case Conn:
		conn, ok := s.conns[target]
		if !ok {
			return nil, errors.ErrNotFoundSession
		}
		return conn, nil
	case User:
		conn, ok := s.users[target]
		if !ok {
			return nil, errors.ErrNotFoundSession
		}
		return conn, nil
	default:
		return nil, errors.ErrInvalidSessionKind
	}
}
