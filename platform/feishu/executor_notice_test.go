package feishu

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func executorInbound(t *testing.T, p *Platform, senderType, sender, msgID, text string) *core.Message {
	t.Helper()
	received := make(chan *core.Message, 1)
	p.handler = func(_ core.Platform, m *core.Message) { received <- m }
	body, _ := json.Marshal(map[string]string{"text": text})
	err := p.onMessage(context.Background(), &larkim.P2MessageReceiveV1{Event: &larkim.P2MessageReceiveV1Data{
		Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: strPtr(sender)}, SenderType: strPtr(senderType)},
		Message: &larkim.EventMessage{MessageId: strPtr(msgID), ChatId: strPtr("oc_group"), ChatType: strPtr("group"), MessageType: strPtr("text"), Content: strPtr(string(body))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-received:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("message was not dispatched")
		return nil
	}
}

func newExecutorTestPlatform() *Platform {
	p := &Platform{platformName: "feishu", dedup: &core.MessageDedup{}, groupReplyAll: true}
	p.chatNameCache.Store("oc_group", "Test group")
	for _, id := range []string{"ou_exec", "ou_other_bot", "ou_owner", "ou_claude"} {
		p.userNameCache.Store(id, "Name "+id)
	}
	return p
}

// Executor side: an order from a bot records that bot as requester only when
// mention_bot_requester_on_completion is on. Negative control: off (the old
// behaviour) leaves requesterID empty, so the orchestrating bot is never
// mentioned and never woken.
func TestExecutorNotice_BotRequesterRecordedOnlyWhenEnabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		p := newExecutorTestPlatform()
		p.mentionBotRequesterOnCompletion = enabled
		m := executorInbound(t, p, "app", "ou_claude", "om_order_"+map[bool]string{false: "off", true: "on"}[enabled], "do the job")
		rc := m.ReplyCtx.(replyContext)
		if enabled && (rc.requesterID != "ou_claude" || !rc.requesterIsBot) {
			t.Fatalf("enabled: requester not recorded: %+v", rc)
		}
		if !enabled && (rc.requesterID != "" || rc.requesterIsBot) {
			t.Fatalf("disabled: bot recorded as requester: %+v", rc)
		}
	}
}

// Executor side: the completion mention for a bot requester carries the
// reply tail; for a human it stays the short notice.
func TestExecutorNotice_BotMentionCarriesTail(t *testing.T) {
	texts := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/auth/") {
			writeJSON(t, w, map[string]any{"code": 0, "expire": 7200, "tenant_access_token": "test-token"})
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		var content map[string]string
		_ = json.Unmarshal([]byte(body["content"].(string)), &content)
		texts <- content["text"]
		writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_result"}})
	}))
	defer srv.Close()
	p := &Platform{platformName: "feishu", mentionBotRequesterOnCompletion: true,
		client: lark.NewClient("exec-notice", "test-secret", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client()))}
	rc := replyContext{messageID: "om_trigger", chatID: "oc_group", groupChat: true, requesterID: "ou_claude", requesterName: "claude", requesterIsBot: true}
	if err := p.NotifyFinalReplyWithTail(context.Background(), rc, "本轮回复已完成，请查看上方结果。", "停手：第 3 步退出 1"); err != nil {
		t.Fatal(err)
	}
	got := <-texts
	if !strings.Contains(got, `<at user_id="ou_claude">`) || !strings.Contains(got, "停手：第 3 步退出 1") {
		t.Fatalf("bot completion mention missing mention or tail: %q", got)
	}
	// Human requester with only the bot option on: no mention (human option off).
	rc.requesterIsBot = false
	if err := p.NotifyFinalReplyWithTail(context.Background(), rc, "done", "tail"); err != nil {
		t.Fatal(err)
	}
	if len(texts) != 0 {
		t.Fatal("human requester mentioned although mention_requester_on_completion is off")
	}
}

// Claude side: a listed executor bot's turn-end notice is routed to the chat's
// main session; other messages from it, and notices from unlisted bots, keep
// the per-sender session. Negative control: without route_bot_senders_to_main
// the notice lands in feishu:oc_group:ou_exec, which the main session never sees.
func TestExecutorNotice_RoutedToMainSession(t *testing.T) {
	const main = "feishu:oc_group:ou_owner"
	for _, tc := range []struct {
		name, sender, text string
		routed             bool
		want               string
	}{
		{"off: notice stays in executor session", "ou_exec", "本轮回复已完成，请查看上方结果。", false, "feishu:oc_group:ou_exec"},
		{"completion notice", "ou_exec", "本轮回复已完成，请查看上方结果。\n末尾摘录：\n交付 PR", true, main},
		{"failed notice", "ou_exec", "本轮执行失败，请查看上方错误。", true, main},
		{"deliver marker", "ou_exec", "【执行方交付：请外网claude审完总结】结果在 ops/x.log", true, main},
		{"ask for decision", "ou_exec", "两条路请你定", true, main},
		{"ordinary chatter", "ou_exec", "正在跑第 2 步", true, "feishu:oc_group:ou_exec"},
		{"unlisted bot", "ou_other_bot", "本轮回复已完成，请查看上方结果。", true, "feishu:oc_group:ou_other_bot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newExecutorTestPlatform()
			if tc.routed {
				p.routeBotSendersToMain = map[string]bool{"ou_exec": true}
				p.mainSessionUser = "ou_owner"
			}
			m := executorInbound(t, p, "app", tc.sender, "om_"+strings.ReplaceAll(tc.name, " ", "_"), tc.text)
			if m.SessionKey != tc.want {
				t.Fatalf("session=%q, want %q", m.SessionKey, tc.want)
			}
			if rc := m.ReplyCtx.(replyContext); rc.requesterID != "" {
				t.Fatalf("claude side must not mention the executor back (loop): %+v", rc)
			}
		})
	}
}

func TestExecutorNotice_RouteRequiresMainUser(t *testing.T) {
	_, err := newPlatform("feishu", lark.FeishuBaseUrl, map[string]any{"app_id": "x", "app_secret": "y", "route_bot_senders_to_main": []any{"ou_exec"}})
	if err == nil {
		t.Fatal("route_bot_senders_to_main without main_session_user accepted")
	}
}

// Live Feishu im.message.receive_v1 events carry sender_type "bot" for bot
// senders (11-26 henry topic_only: order from the orchestrating bot left no
// requester, so no completion mention). Both "app" and "bot" must count.
// Negative control: "user"-less unknown types still record nothing.
func TestExecutorNotice_LiveBotSenderType(t *testing.T) {
	for _, st := range []string{"app", "bot"} {
		p := newExecutorTestPlatform()
		p.mentionBotRequesterOnCompletion = true
		rc := executorInbound(t, p, st, "ou_claude", "om_req_"+st, "do the job").ReplyCtx.(replyContext)
		if rc.requesterID != "ou_claude" || !rc.requesterIsBot {
			t.Fatalf("sender_type=%s: bot requester not recorded: %+v", st, rc)
		}
		p = newExecutorTestPlatform()
		p.routeBotSendersToMain = map[string]bool{"ou_exec": true}
		p.mainSessionUser = "ou_owner"
		m := executorInbound(t, p, st, "ou_exec", "om_route_"+st, "本轮回复已完成，请查看上方结果。")
		if m.SessionKey != "feishu:oc_group:ou_owner" {
			t.Fatalf("sender_type=%s: notice not routed to main: %q", st, m.SessionKey)
		}
	}
	p := newExecutorTestPlatform()
	p.mentionBotRequesterOnCompletion = true
	if rc := executorInbound(t, p, "anonymous", "ou_claude", "om_anon", "hi").ReplyCtx.(replyContext); rc.requesterID != "" {
		t.Fatalf("anonymous sender recorded as requester: %+v", rc)
	}
}
