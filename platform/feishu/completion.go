package feishu

import (
	"context"
	"fmt"
	"html"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

var _ core.FinalReplyNotifier = (*Platform)(nil)
var _ core.FinalReplyNotifier = (*interactivePlatform)(nil)
var _ core.FinalReplyTailNotifier = (*Platform)(nil)

// NotifyFinalReply sends a separate native text mention. Finalizing an existing
// progress card does not reliably trigger a new mention notification in Feishu.
func (p *Platform) NotifyFinalReply(ctx context.Context, replyCtx any, message string) error {
	return p.NotifyFinalReplyWithTail(ctx, replyCtx, message, "")
}

// NotifyFinalReplyWithTail is NotifyFinalReply plus, for bot requesters, an
// excerpt of the reply: a bot is only woken by the mention itself and does
// not see the card above it.
func (p *Platform) NotifyFinalReplyWithTail(ctx context.Context, replyCtx any, message, tail string) error {
	rc, ok := replyCtx.(replyContext)
	if !ok || !rc.groupChat || rc.chatID == "" || rc.requesterID == "" || strings.EqualFold(rc.requesterID, "all") {
		return nil
	}
	if rc.requesterIsBot {
		if !p.mentionBotRequesterOnCompletion {
			return nil
		}
		if tail != "" {
			message = message + "\n末尾摘录：\n" + tail
		}
	} else if !p.mentionRequesterOnCompletion {
		return nil
	}
	name := rc.requesterName
	if name == "" {
		name = rc.requesterID
	}
	content := fmt.Sprintf(`<at user_id="%s">%s</at> %s`, html.EscapeString(rc.requesterID), html.EscapeString(name), html.EscapeString(message))
	// Reuse normal routing so thread replies and reply_to_trigger retain their
	// configured behavior. The native <at> tag selects Feishu's text message API.
	if err := p.Reply(ctx, rc, content); err != nil {
		return fmt.Errorf("%s: notify final reply: %w", p.tag(), err)
	}
	return nil
}
