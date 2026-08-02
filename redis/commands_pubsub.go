package redis

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
)

const (
	subscriberMailbox = 128
	subscriberIdleTTL = 5 * time.Minute
	defaultPollBatch  = 64
)

// PubSubMessage is one delivered message.
type PubSubMessage struct {
	Kind    string `json:"kind"`              // "message" or "pmessage"
	Channel string `json:"channel"`           // the channel it was published to
	Pattern string `json:"pattern,omitempty"` // set when Kind == "pmessage"
	Payload string `json:"payload"`
}

// subscriber is one client's mailbox plus what it is subscribed to.
type subscriber struct {
	id       string
	mailbox  chan PubSubMessage
	channels map[string]bool
	patterns map[string]bool
	lastSeen time.Time
	dropped  uint64 // messages discarded because the mailbox was full
}

func (s *redisStore) newSubscriber() *subscriber {
	s.nextSubID++
	sub := &subscriber{
		id:       fmt.Sprintf("sub-%d", s.nextSubID),
		mailbox:  make(chan PubSubMessage, subscriberMailbox),
		channels: make(map[string]bool),
		patterns: make(map[string]bool),
		lastSeen: time.Now(),
	}
	s.subscribers[sub.id] = sub
	return sub
}

func (s *redisStore) resolveSubscriber(id string) *subscriber {
	if id != "" {
		if sub, ok := s.subscribers[id]; ok {
			sub.lastSeen = time.Now()
			return sub
		}
	}
	return s.newSubscriber()
}

func addIndex(index map[string]map[string]struct{}, key, subID string) {
	if index[key] == nil {
		index[key] = make(map[string]struct{})
	}
	index[key][subID] = struct{}{}
}

func removeIndex(index map[string]map[string]struct{}, key, subID string) {
	subs, ok := index[key]
	if !ok {
		return
	}
	delete(subs, subID)
	if len(subs) == 0 {
		delete(index, key)
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// reapIdleSubscribers drops subscribers that have not polled within the TTL.
// Called from the pub/sub entry points rather than a background goroutine, so
// there is no extra thread to coordinate with shutdown.
func (s *redisStore) reapIdleSubscribers() {
	cutoff := time.Now().Add(-subscriberIdleTTL)
	for id, sub := range s.subscribers {
		if sub.lastSeen.After(cutoff) {
			continue
		}
		for channel := range sub.channels {
			removeIndex(s.channelSubs, channel, id)
		}
		for pattern := range sub.patterns {
			removeIndex(s.patternSubs, pattern, id)
		}
		delete(s.subscribers, id)
	}
}

func (s *redisStore) cmdSubscribe(args map[string]string) any {
	channel := args["channel"]
	if channel == "" {
		return errors.New("ERR missing channel")
	}
	s.reapIdleSubscribers()

	sub := s.resolveSubscriber(args["id"])
	sub.channels[channel] = true
	addIndex(s.channelSubs, channel, sub.id)

	return map[string]any{
		"subscriber_id": sub.id,
		"channel":       channel,
		"subscribed":    true,
		"channels":      sortedKeys(sub.channels),
	}
}

func (s *redisStore) cmdPSubscribe(args map[string]string) any {
	pattern := args["pattern"]
	if pattern == "" {
		pattern = args["channel"]
	}
	if pattern == "" {
		return errors.New("ERR missing pattern")
	}
	s.reapIdleSubscribers()

	sub := s.resolveSubscriber(args["id"])
	sub.patterns[pattern] = true
	addIndex(s.patternSubs, pattern, sub.id)

	return map[string]any{
		"subscriber_id": sub.id,
		"pattern":       pattern,
		"subscribed":    true,
		"patterns":      sortedKeys(sub.patterns),
	}
}

func (s *redisStore) cmdUnsubscribe(args map[string]string) any {
	id := args["id"]
	if id == "" {
		return errors.New("ERR missing subscriber id")
	}
	sub, ok := s.subscribers[id]
	if !ok {
		return errors.New("ERR unknown subscriber")
	}
	sub.lastSeen = time.Now()

	channel := args["channel"]
	if channel != "" {
		delete(sub.channels, channel)
		removeIndex(s.channelSubs, channel, id)
	} else {
		for c := range sub.channels {
			removeIndex(s.channelSubs, c, id)
		}
		sub.channels = make(map[string]bool)
	}

	remaining := sortedKeys(sub.channels)
	s.dropIfInactive(sub)
	return map[string]any{
		"subscriber_id": id,
		"channel":       channel,
		"subscribed":    false,
		"channels":      remaining,
	}
}

// cmdPUnsubscribe removes one pattern subscription, or all of them.
func (s *redisStore) cmdPUnsubscribe(args map[string]string) any {
	id := args["id"]
	if id == "" {
		return errors.New("ERR missing subscriber id")
	}
	sub, ok := s.subscribers[id]
	if !ok {
		return errors.New("ERR unknown subscriber")
	}
	sub.lastSeen = time.Now()

	pattern := args["pattern"]
	if pattern == "" {
		pattern = args["channel"]
	}
	if pattern != "" {
		delete(sub.patterns, pattern)
		removeIndex(s.patternSubs, pattern, id)
	} else {
		for p := range sub.patterns {
			removeIndex(s.patternSubs, p, id)
		}
		sub.patterns = make(map[string]bool)
	}

	remaining := sortedKeys(sub.patterns)
	s.dropIfInactive(sub)
	return map[string]any{
		"subscriber_id": id,
		"pattern":       pattern,
		"subscribed":    false,
		"patterns":      remaining,
	}
}

func (s *redisStore) dropIfInactive(sub *subscriber) {
	if len(sub.channels) == 0 && len(sub.patterns) == 0 && len(sub.mailbox) == 0 {
		delete(s.subscribers, sub.id)
	}
}

func (s *redisStore) cmdPublish(args map[string]string) any {
	channel := args["channel"]
	if channel == "" {
		return errors.New("ERR missing channel")
	}
	message := args["message"] // an empty payload is legal
	s.reapIdleSubscribers()

	delivered := int64(0)

	for id := range s.channelSubs[channel] {
		if s.deliver(id, PubSubMessage{Kind: "message", Channel: channel, Payload: message}) {
			delivered++
		}
	}

	for pattern, subs := range s.patternSubs {
		if !globMatch(pattern, channel) {
			continue
		}
		for id := range subs {
			if s.deliver(id, PubSubMessage{
				Kind: "pmessage", Channel: channel, Pattern: pattern, Payload: message,
			}) {
				delivered++
			}
		}
	}

	return delivered
}

func (s *redisStore) deliver(id string, msg PubSubMessage) bool {
	sub, ok := s.subscribers[id]
	if !ok {
		return false
	}
	select {
	case sub.mailbox <- msg:
		return true
	default:
		sub.dropped++
		return false
	}
}

func (s *redisStore) cmdPoll(args map[string]string) any {
	id := args["id"]
	if id == "" {
		return errors.New("ERR missing subscriber id")
	}
	sub, ok := s.subscribers[id]
	if !ok {
		return errors.New("ERR unknown subscriber")
	}
	sub.lastSeen = time.Now()

	limit := defaultPollBatch
	if raw, ok := args["max"]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return errors.New("ERR max must be a positive integer")
		}
		limit = n
	}

	messages := make([]PubSubMessage, 0, limit)
drain:
	for len(messages) < limit {
		select {
		case msg := <-sub.mailbox:
			messages = append(messages, msg)
		default:
			break drain
		}
	}

	dropped := sub.dropped
	sub.dropped = 0

	return map[string]any{
		"subscriber_id": id,
		"messages":      messages,
		"pending":       len(sub.mailbox),
		"dropped":       dropped,
	}
}

// cmdPubSubChannels lists channels that currently have at least one subscriber.
func (s *redisStore) cmdPubSubChannels(_ map[string]string) any {
	s.reapIdleSubscribers()
	channels := make([]string, 0, len(s.channelSubs))
	for channel := range s.channelSubs {
		channels = append(channels, channel)
	}
	sort.Strings(channels)

	patterns := make([]string, 0, len(s.patternSubs))
	for pattern := range s.patternSubs {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns)

	return map[string]any{
		"channels":    channels,
		"patterns":    patterns,
		"subscribers": len(s.subscribers),
	}
}

func globMatch(pattern, str string) bool {
	var (
		p, s     int
		starP    = -1
		starS    int
		haveStar bool
	)

	for s < len(str) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				starP, starS, haveStar = p, s, true
				p++
				continue
			case '?':
				p++
				s++
				continue
			case '[':
				if next, matched, wellFormed := matchClass(pattern, p, str[s]); wellFormed {
					if matched {
						p = next
						s++
						continue
					}
					// Parsed but did not match — fall through and backtrack.
				} else if pattern[p] == str[s] {
					// Unterminated class: treat '[' as a literal.
					p++
					s++
					continue
				}
			case '\\':
				if p+1 < len(pattern) {
					if pattern[p+1] == str[s] {
						p += 2
						s++
						continue
					}
				} else if pattern[p] == str[s] { // trailing backslash is literal
					p++
					s++
					continue
				}
			default:
				if pattern[p] == str[s] {
					p++
					s++
					continue
				}
			}
		}

		if haveStar {
			starS++
			s = starS
			p = starP + 1
			continue
		}
		return false
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

func matchClass(pattern string, p int, c byte) (next int, matched bool, wellFormed bool) {
	i := p + 1
	negate := false
	if i < len(pattern) && (pattern[i] == '^' || pattern[i] == '!') {
		negate = true
		i++
	}

	for i < len(pattern) {
		switch {
		case pattern[i] == ']':
			if negate {
				matched = !matched
			}
			return i + 1, matched, true

		case pattern[i] == '\\' && i+1 < len(pattern):
			if pattern[i+1] == c {
				matched = true
			}
			i += 2

		case i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']':
			lo, hi := pattern[i], pattern[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			i += 3

		default:
			if pattern[i] == c {
				matched = true
			}
			i++
		}
	}
	return 0, false, false // no closing ']'
}
