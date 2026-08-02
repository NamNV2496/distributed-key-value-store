package redis

import (
	"fmt"
	"testing"
)

// subscribe registers a subscriber and returns its id.
func subscribe(t *testing.T, s *redisStore, channel string, id string) string {
	t.Helper()
	args := map[string]string{"channel": channel}
	if id != "" {
		args["id"] = id
	}
	res := eval(t, s, "SUBSCRIBE", args).(map[string]any)
	return res["subscriber_id"].(string)
}

func psubscribe(t *testing.T, s *redisStore, pattern string, id string) string {
	t.Helper()
	args := map[string]string{"pattern": pattern}
	if id != "" {
		args["id"] = id
	}
	res := eval(t, s, "PSUBSCRIBE", args).(map[string]any)
	return res["subscriber_id"].(string)
}

func poll(t *testing.T, s *redisStore, id string) []PubSubMessage {
	t.Helper()
	res := eval(t, s, "POLL", map[string]string{"id": id}).(map[string]any)
	return res["messages"].([]PubSubMessage)
}

// The headline bug: SUBSCRIBE created a channel nothing ever read, so a
// published message could never reach any client. PUBLISH reported a delivery
// count for messages that went into a buffer and were eventually discarded.
func TestPublishedMessageIsActuallyDelivered(t *testing.T) {
	s := newStore(t)
	id := subscribe(t, s, "news", "")

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "news", "message": "hello"}); got != int64(1) {
		t.Fatalf("PUBLISH = %v, want 1 delivery", got)
	}

	msgs := poll(t, s, id)
	if len(msgs) != 1 {
		t.Fatalf("POLL returned %d messages, want 1 — the message was never delivered", len(msgs))
	}
	if msgs[0].Payload != "hello" || msgs[0].Channel != "news" || msgs[0].Kind != "message" {
		t.Fatalf("delivered message = %+v", msgs[0])
	}

	// The mailbox must be drained by the poll.
	if again := poll(t, s, id); len(again) != 0 {
		t.Fatalf("second POLL returned %d messages, want 0", len(again))
	}
}

// The subscriber index was keyed by channel name on both levels, so a second
// subscriber to a channel silently replaced the first.
func TestMultipleSubscribersEachReceiveTheMessage(t *testing.T) {
	s := newStore(t)
	a := subscribe(t, s, "news", "")
	b := subscribe(t, s, "news", "")
	c := subscribe(t, s, "news", "")

	if a == b || b == c || a == c {
		t.Fatalf("subscribers share an id: %s %s %s", a, b, c)
	}

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "news", "message": "hi"}); got != int64(3) {
		t.Fatalf("PUBLISH = %v, want 3 deliveries", got)
	}
	for _, id := range []string{a, b, c} {
		if msgs := poll(t, s, id); len(msgs) != 1 || msgs[0].Payload != "hi" {
			t.Fatalf("subscriber %s got %+v, want one copy of the message", id, msgs)
		}
	}
}

// One client can hold several subscriptions by passing its id back.
func TestSubscriberCanJoinSeveralChannels(t *testing.T) {
	s := newStore(t)
	id := subscribe(t, s, "sports", "")
	same := subscribe(t, s, "weather", id)
	if same != id {
		t.Fatalf("re-subscribing with id %s created a new subscriber %s", id, same)
	}

	eval(t, s, "PUBLISH", map[string]string{"channel": "sports", "message": "goal"})
	eval(t, s, "PUBLISH", map[string]string{"channel": "weather", "message": "rain"})

	if msgs := poll(t, s, id); len(msgs) != 2 {
		t.Fatalf("expected messages from both channels, got %+v", msgs)
	}
}

// PSUBSCRIBE was byte-identical to SUBSCRIBE and shared one map, so an exact
// subscription to a name containing '*' behaved as a wildcard.
func TestExactSubscriptionIsNotTreatedAsAPattern(t *testing.T) {
	s := newStore(t)
	literal := subscribe(t, s, "abc*", "") // a channel literally named "abc*"

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "abc123", "message": "x"}); got != int64(0) {
		t.Fatalf("PUBLISH abc123 reached %v subscribers; an exact subscription to \"abc*\" must not match", got)
	}
	if msgs := poll(t, s, literal); len(msgs) != 0 {
		t.Fatalf("exact subscriber received %+v for a non-matching channel", msgs)
	}

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "abc*", "message": "x"}); got != int64(1) {
		t.Fatalf("PUBLISH to the literal channel = %v, want 1", got)
	}
}

func TestPatternSubscriptionReceivesMatchingChannels(t *testing.T) {
	s := newStore(t)
	id := psubscribe(t, s, "news.*", "")

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "news.eu", "message": "a"}); got != int64(1) {
		t.Fatalf("PUBLISH news.eu = %v, want 1", got)
	}
	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "sports.eu", "message": "b"}); got != int64(0) {
		t.Fatalf("PUBLISH sports.eu = %v, want 0", got)
	}

	msgs := poll(t, s, id)
	if len(msgs) != 1 {
		t.Fatalf("got %+v, want one pmessage", msgs)
	}
	if msgs[0].Kind != "pmessage" || msgs[0].Pattern != "news.*" || msgs[0].Channel != "news.eu" {
		t.Fatalf("pmessage = %+v", msgs[0])
	}
}

// Redis delivers one copy per matching subscription.
func TestSubscriberMatchingBothWaysGetsBothCopies(t *testing.T) {
	s := newStore(t)
	id := subscribe(t, s, "news.eu", "")
	psubscribe(t, s, "news.*", id)

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "news.eu", "message": "x"}); got != int64(2) {
		t.Fatalf("PUBLISH = %v, want 2 deliveries (exact + pattern)", got)
	}
	if msgs := poll(t, s, id); len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2: %+v", len(msgs), msgs)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	s := newStore(t)
	id := subscribe(t, s, "news", "")
	eval(t, s, "UNSUBSCRIBE", map[string]string{"id": id, "channel": "news"})

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "news", "message": "x"}); got != int64(0) {
		t.Fatalf("PUBLISH after UNSUBSCRIBE = %v, want 0", got)
	}
}

func TestPUnsubscribeStopsPatternDelivery(t *testing.T) {
	s := newStore(t)
	id := psubscribe(t, s, "news.*", "")
	eval(t, s, "PUNSUBSCRIBE", map[string]string{"id": id, "pattern": "news.*"})

	if got := eval(t, s, "PUBLISH", map[string]string{"channel": "news.eu", "message": "x"}); got != int64(0) {
		t.Fatalf("PUBLISH after PUNSUBSCRIBE = %v, want 0", got)
	}
}

// A subscriber that stops polling must not grow without bound; overflow is
// dropped and reported rather than retained forever.
func TestSlowSubscriberOverflowIsBoundedAndReported(t *testing.T) {
	s := newStore(t)
	id := subscribe(t, s, "flood", "")

	for i := 0; i < subscriberMailbox+10; i++ {
		eval(t, s, "PUBLISH", map[string]string{"channel": "flood", "message": fmt.Sprintf("m%d", i)})
	}

	res := eval(t, s, "POLL", map[string]string{"id": id, "max": "1000"}).(map[string]any)
	msgs := res["messages"].([]PubSubMessage)
	if len(msgs) != subscriberMailbox {
		t.Fatalf("mailbox held %d messages, want the %d-message bound", len(msgs), subscriberMailbox)
	}
	if res["dropped"].(uint64) != 10 {
		t.Fatalf("dropped = %v, want 10", res["dropped"])
	}
}

// Errors from these commands used to be returned as successful results.
func TestPubSubErrorsSurfaceAsErrors(t *testing.T) {
	s := newStore(t)
	cases := []struct {
		cmd  string
		args map[string]string
	}{
		{"PUBLISH", map[string]string{}},
		{"SUBSCRIBE", map[string]string{}},
		{"PSUBSCRIBE", map[string]string{}},
		{"POLL", map[string]string{}},
		{"POLL", map[string]string{"id": "sub-999"}},
		{"UNSUBSCRIBE", map[string]string{}},
		{"PUNSUBSCRIBE", map[string]string{}},
	}
	for _, tc := range cases {
		if _, err := s.EvalAndResponse(&Command{Cmd: tc.cmd, Args: tc.args}); err == nil {
			t.Errorf("%s %v returned no error", tc.cmd, tc.args)
		}
	}
}

func TestPubSubChannelsListsActiveSubscriptions(t *testing.T) {
	s := newStore(t)
	subscribe(t, s, "b", "")
	subscribe(t, s, "a", "")
	psubscribe(t, s, "p.*", "")

	res := eval(t, s, "PUBSUB_CHANNELS", map[string]string{}).(map[string]any)
	channels := res["channels"].([]string)
	if len(channels) != 2 || channels[0] != "a" || channels[1] != "b" {
		t.Fatalf("channels = %v, want [a b]", channels)
	}
	if patterns := res["patterns"].([]string); len(patterns) != 1 || patterns[0] != "p.*" {
		t.Fatalf("patterns = %v, want [p.*]", patterns)
	}
}

// The old matcher only understood a trailing '*'.
func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, input string
		want           bool
	}{
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"*", "anything", true},
		{"*", "", true},
		{"abc*", "abc123", true},
		{"abc*", "abd123", false},
		{"*abc", "xxabc", true},
		{"*abc", "xxabd", false},
		{"news.*.eu", "news.sports.eu", true}, // interior '*' — never matched before
		{"news.*.eu", "news.sports.us", false},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxcyyb", false},
		{"user?", "user1", true},
		{"user?", "user", false},
		{"user?", "user12", false},
		{"[abc]d", "ad", true},
		{"[abc]d", "dd", false},
		{"[a-z]1", "q1", true},
		{"[a-z]1", "Q1", false},
		{"[^a-z]1", "Q1", true},
		{"[^a-z]1", "q1", false},
		{`a\*b`, "a*b", true},
		{`a\*b`, "axb", false},
		{"", "", true},
		{"", "x", false},
		{"a[", "a[", true}, // unterminated class is a literal
	}
	for _, tc := range cases {
		if got := globMatch(tc.pattern, tc.input); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pattern, tc.input, got, tc.want)
		}
	}
}
