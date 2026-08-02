package redis

import (
	"errors"
	"strings"
)

func (s *redisStore) cmdPublish(args map[string]string) any {
	channel := args["channel"]
	message := args["message"]
	if channel == "" || message == "" {
		return errors.New("ERR missing channel or message")
	}

	count := int64(0)
	for pattern, subs := range s.pubSub {
		if !matchPattern(pattern, channel) {
			continue
		}
		for _, ch := range subs {
			select {
			case ch <- message:
				count++
			default:
			}
		}
	}
	return count
}

func (s *redisStore) cmdSubscribe(args map[string]string) any {
	channel := args["channel"]
	if channel == "" {
		return errors.New("ERR missing channel")
	}
	ch := make(chan string, 16)
	if s.pubSub[channel] == nil {
		s.pubSub[channel] = make(map[string]chan string)
	}
	s.pubSub[channel][channel] = ch
	return map[string]any{"channel": channel, "subscribed": true}
}

func (s *redisStore) cmdUnsubscribe(args map[string]string) any {
	channel := args["channel"]
	if channel == "" {
		return errors.New("ERR missing channel")
	}
	if subs, ok := s.pubSub[channel]; ok {
		delete(subs, channel)
		if len(subs) == 0 {
			delete(s.pubSub, channel)
		}
	}
	return map[string]any{"channel": channel, "subscribed": false}
}

func (s *redisStore) cmdPSubscribe(args map[string]string) any {
	channel := args["channel"]
	if channel == "" {
		return errors.New("ERR missing channel")
	}
	ch := make(chan string, 16)
	if s.pubSub[channel] == nil {
		s.pubSub[channel] = make(map[string]chan string)
	}
	s.pubSub[channel][channel] = ch
	return map[string]any{"pattern": channel, "subscribed": true}
}

func matchPattern(pattern, channel string) bool {
	if pattern == "" || channel == "" {
		return false
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(channel, prefix)
	}
	return pattern == channel
}

func (s *redisStore) cmdPUnsubscribe(args map[string]string) any {
	channel := args["channel"]
	if channel == "" {
		return errors.New("ERR missing channel")
	}
	if subs, ok := s.pubSub[channel]; ok {
		delete(subs, channel)
		if len(subs) == 0 {
			delete(s.pubSub, channel)
		}
	}
	return map[string]any{"pattern": channel, "subscribed": false}
}
