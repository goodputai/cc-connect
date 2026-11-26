package core

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// FinalReplyNotifier is an optional platform capability for notifying the
// requester after a terminal, visible agent reply has been delivered. replyCtx
// belongs to that turn, including when messages are queued in a shared session.
// It is not called for intermediate progress, commands, or cancelled turns.
type FinalReplyNotifier interface {
	NotifyFinalReply(ctx context.Context, replyCtx any, message string) error
}

// FinalReplyTailNotifier is an optional extension of FinalReplyNotifier that
// also receives the tail of the turn's final text (or the error text of a
// failed turn). Platforms use it when the requester cannot see the reply
// itself, e.g. a bot that is only woken by mentions.
type FinalReplyTailNotifier interface {
	NotifyFinalReplyWithTail(ctx context.Context, replyCtx any, message, tail string) error
}

// finalReplyTailRunes bounds the excerpt attached to completion mentions.
const finalReplyTailRunes = 300

func finalReplyTail(content string) string {
	content = strings.TrimSpace(content)
	if stripped, ok := stripTrailingSilent(content); ok {
		content = strings.TrimSpace(stripped)
	}
	r := []rune(content)
	if len(r) > finalReplyTailRunes {
		r = r[len(r)-finalReplyTailRunes:]
	}
	return string(r)
}

func (e *Engine) notifyFinalReply(p Platform, replyCtx any, content string) {
	e.notifyTurnEnd(p, replyCtx, e.i18n.T(MsgFinalReplyComplete), content)
}

// notifyTurnFailed tells the requester that the turn ended with an error, so
// an orchestrating bot is woken by failures as well as by final replies.
func (e *Engine) notifyTurnFailed(p Platform, replyCtx any, errText string) {
	e.notifyTurnEnd(p, replyCtx, e.i18n.T(MsgTurnFailedNotice), errText)
}

func (e *Engine) notifyTurnEnd(p Platform, replyCtx any, message, content string) {
	ctx, cancel := context.WithTimeout(e.ctx, 15*time.Second)
	defer cancel()
	var err error
	if tn, ok := p.(FinalReplyTailNotifier); ok {
		err = tn.NotifyFinalReplyWithTail(ctx, replyCtx, message, finalReplyTail(content))
	} else if notifier, ok := p.(FinalReplyNotifier); ok {
		err = notifier.NotifyFinalReply(ctx, replyCtx, message)
	} else {
		return
	}
	if err != nil {
		// A failed notification must not cause the final answer to be resent.
		slog.Warn("final reply notification failed", "platform", p.Name(), "error", err)
	}
}

func hasVisibleFinalReply(content string) bool {
	if isSilentReply(content) {
		return false
	}
	if stripped, ok := stripTrailingSilent(content); ok {
		content = stripped
	}
	return strings.TrimSpace(content) != ""
}
