package gate

import (
	"context"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/internal/link"
	"github.com/dobyte/due/v2/log"
	"github.com/dobyte/due/v2/mode"
	"github.com/dobyte/due/v2/packet"
	"github.com/dobyte/due/v2/session"
)

type proxy struct {
	gate       *Gate            // 网关服
	nodeLinker *link.NodeLinker // 节点链接器
}

func newProxy(gate *Gate) *proxy {
	return &proxy{gate: gate, nodeLinker: link.NewNodeLinker(gate.ctx, &link.Options{
		ID:                gate.opts.id,
		Kind:              cluster.Gate,
		Locator:           gate.opts.locator,
		Registry:          gate.opts.registry,
		Dispatch:          gate.opts.dispatch,
		ConnNum:           gate.opts.connNum,
		CallTimeout:       gate.opts.callTimeout,
		DialTimeout:       gate.opts.dialTimeout,
		DialRetryTimes:    gate.opts.dialRetryTimes,
		WriteTimeout:      gate.opts.writeTimeout,
		WriteQueueSize:    gate.opts.writeQueueSize,
		FaultRecoveryTime: gate.opts.faultRecoveryTime,
	})}
}

// 绑定用户与网关间的关系
func (p *proxy) bindGate(ctx context.Context, cid, uid int64) error {
	err := p.gate.opts.locator.BindGate(ctx, uid, p.gate.opts.id)
	if err != nil {
		return err
	}

	token, _ := p.gate.session.Token(session.Conn, cid)
	p.trigger(ctx, cluster.Reconnect, cid, uid, token)

	return nil
}

// 解绑用户与网关间的关系
func (p *proxy) unbindGate(ctx context.Context, cid, uid int64) error {
	err := p.gate.opts.locator.UnbindGate(ctx, uid, p.gate.opts.id)
	if err != nil {
		log.Errorf("user unbind failed, gid: %s, cid: %d, uid: %d, err: %v", p.gate.opts.id, cid, uid, err)
	}

	return err
}

// 触发事件
func (p *proxy) trigger(ctx context.Context, event cluster.Event, cid, uid int64, token session.Token) {
	if mode.IsDebugMode() {
		log.Debugf("trigger event, event: %v cid: %d uid: %d", event.String(), cid, uid)
	}

	if err := p.nodeLinker.Trigger(ctx, &link.TriggerArgs{
		Event: event,
		CID:   cid,
		UID:   uid,
		Token: token,
	}); err != nil {
		switch {
		case errors.Is(err, errors.ErrNotFoundEvent), errors.Is(err, errors.ErrNotFoundUserLocation):
			log.Warnf("trigger event failed, cid: %d, uid: %d, event: %v, err: %v", cid, uid, event.String(), err)
		default:
			log.Errorf("trigger event failed, cid: %d, uid: %d, event: %v, err: %v", cid, uid, event.String(), err)
		}
	}
}

func requestDeliveryToken(manager *session.Session, cid, uid int64) session.Token {
	if uid == 0 {
		return session.Token{}
	}
	token, _ := manager.Token(session.Conn, cid)
	return token
}

// 投递消息
func (p *proxy) deliver(ctx context.Context, cid, uid int64, metadata cluster.RequestMetadata, data []byte) error {
	token := requestDeliveryToken(p.gate.session, cid, uid)
	message, err := packet.UnpackMessage(data)
	if err != nil {
		log.Errorf("unpack message failed: %v", err)
		return err
	}

	var preferredNID string
	if uid == 0 && p.gate.opts.statelessRouteAffinity != nil {
		routingUID, group, ok := p.gate.opts.statelessRouteAffinity(message.Route, message.Buffer)
		if ok {
			preferredNID, err = p.nodeLinker.ResolveStatelessAffinityNode(ctx, message.Route, routingUID, group)
			if err != nil {
				return err // Do not pick a random Game when the owner lookup failed.
			}
		}
	}

	if err = p.nodeLinker.Deliver(ctx, &link.DeliverArgs{
		NID:      preferredNID,
		CID:      cid,
		UID:      uid,
		Token:    token,
		Metadata: metadata,
		Route:    message.Route,
		Buffer:   data,
	}); err != nil {
		switch {
		case errors.Is(err, errors.ErrNotFoundRoute), errors.Is(err, errors.ErrNotFoundEndpoint):
			log.Warnf("deliver message failed, cid: %d uid: %d seq: %d route: %d err: %v", cid, uid, message.Seq, message.Route, err)
		default:
			log.Errorf("deliver message failed, cid: %d uid: %d seq: %d route: %d err: %v", cid, uid, message.Seq, message.Route, err)
		}
		return err
	}
	if mode.IsDebugMode() {
		log.Debugf("deliver message success, cid: %d uid: %d seq: %d route: %d", cid, uid, message.Seq, message.Route)
	}
	return nil
}

// 开始监听
func (p *proxy) watch() {
	p.nodeLinker.WatchUserLocate()

	p.nodeLinker.WatchClusterInstance()
}
