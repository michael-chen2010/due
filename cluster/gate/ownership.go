package gate

import (
	"context"

	"github.com/dobyte/due/v2/log"
	"github.com/dobyte/due/v2/session"
)

func (g *Gate) bindSession(ctx context.Context, cid, uid int64) (session.Token, error) {
	if g.opts.ownershipStore == nil {
		if err := g.session.Bind(cid, uid); err != nil {
			return session.Token{}, err
		}
		return g.session.Token(session.Conn, cid)
	}

	localToken, err := g.session.Token(session.Conn, cid)
	if err != nil {
		return session.Token{}, err
	}

	if localToken.UID == uid && localToken.Generation != 0 && g.session.IsCurrent(localToken) {
		current, ok, err := g.opts.ownershipStore.Current(ctx, uid)
		if err != nil {
			return session.Token{}, err
		}
		if ok && current == localToken {
			return localToken, nil
		}
	}

	token, err := g.opts.ownershipStore.Acquire(ctx, uid)
	if err != nil {
		return session.Token{}, err
	}

	if err = g.session.BindToken(cid, uid, token); err != nil {
		_, _ = g.opts.ownershipStore.Release(ctx, uid, token)
		return session.Token{}, err
	}

	if localToken.UID != 0 && localToken.Generation != 0 && localToken.UID != uid {
		if _, releaseErr := g.opts.ownershipStore.Release(ctx, localToken.UID, localToken); releaseErr != nil {
			log.Warnf(
				"release previous session ownership failed, cid: %d uid: %d generation: %d err: %v",
				cid,
				localToken.UID,
				localToken.Generation,
				releaseErr,
			)
		}
	}

	return token, nil
}

func (g *Gate) releaseSession(ctx context.Context, token session.Token) (bool, error) {
	if g.opts.ownershipStore == nil || token.UID == 0 || token.Generation == 0 {
		return false, nil
	}
	return g.opts.ownershipStore.Release(ctx, token.UID, token)
}
