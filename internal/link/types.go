package link

import (
	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/session"
)

type (
	Message         = cluster.Message
	GetIPArgs       = cluster.GetIPArgs
	IsOnlineArgs    = cluster.IsOnlineArgs
	DisconnectArgs  = cluster.DisconnectArgs
	PushArgs        = cluster.PushArgs
	MulticastArgs   = cluster.MulticastArgs
	BroadcastArgs   = cluster.BroadcastArgs
	PublishArgs     = cluster.PublishArgs
	SubscribeArgs   = cluster.SubscribeArgs
	UnsubscribeArgs = cluster.UnsubscribeArgs
)

type DeliverArgs struct {
	NID    string        // 接收节点。存在接收节点时，消息会直接投递给接收节点；不存在接收节点时，系统定位用户所在节点，然后投递。
	CID    int64         // 连接ID
	UID    int64         // 用户ID
	Token  session.Token // 会话绑定令牌
	Route  int32         // 消息路由
	Buffer any           // 投递消息
}

type TriggerArgs struct {
	Event cluster.Event // 事件
	CID   int64         // 连接ID
	UID   int64         // 用户ID
	Token session.Token // 会话绑定令牌
}
