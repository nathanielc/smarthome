package smarthome

import (
	"errors"
	"path"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Client interface {
	// Set publishes a set message with the value
	Set(toplevel, item string, value string) error
	// Get publishes a get request message.
	Get(toplevel, item string) (Value, error)
	// Command publishes a command to the toplevel topic.
	Command(toplevel string, cmd []byte) error

	// Subscribe to receive callbacks whenever a status message is received.
	Subscribe(toplevel, item string) *Subscription

	// Close disconnects the client.
	Close()
}

type client struct {
	mu         sync.RWMutex
	c          mqtt.Client
	disconnect bool

	closed  bool
	closing chan struct{}

	subs []*Subscription
}

func NewClient(opts *mqtt.ClientOptions) (Client, error) {
	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	return newClient(c, true)
}

func newClient(c mqtt.Client, disconnect bool) (Client, error) {
	cli := &client{
		c:          c,
		disconnect: disconnect,
		closing:    make(chan struct{}),
		subs:       make([]*Subscription, 0, 10),
	}
	if token := c.Subscribe(allStatusTopic, 0, cli.handleStatusMessage); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	return cli, nil
}

func (c *client) Set(toplevel, item string, value string) error {
	topic := path.Join(toplevel, setPath, item)
	token := c.c.Publish(topic, 0, false, value)
	token.Wait()
	return token.Error()
}

func (c *client) Get(toplevel, item string) (Value, error) {
	s := c.Subscribe(toplevel, item)
	defer s.Unsubscribe()

	getTopic := path.Join(toplevel, getPath, item)
	if token := c.c.Publish(getTopic, 0, false, "?"); token.Wait() && token.Error() != nil {
		return Value{}, token.Error()
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-c.closing:
		return Value{}, errors.New("client closed")
	case <-timer.C:
		return Value{}, errors.New("timed out waiting for get response")
	case sm := <-s.C:
		return sm.Value, nil
	}
}

func (c *client) Command(toplevel string, cmd []byte) error {
	topic := path.Join(toplevel, commandPath)
	token := c.c.Publish(topic, 0, false, cmd)
	token.Wait()
	return token.Error()
}

func (c *client) Subscribe(toplevel, item string) *Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()

	statusTopic := path.Join(toplevel, statusPath, item)
	ch := make(chan StatusMessage, 1000)
	sub := &Subscription{
		c:       c,
		pattern: statusTopic,
		ch:      ch,
		C:       ch,
	}
	c.subs = append(c.subs, sub)
	return sub
}

func (c *client) unsubscribe(s *Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()

	filtered := c.subs[0:0]
	for _, sub := range c.subs {
		if sub != s {
			filtered = append(filtered, sub)
		}
	}
	c.subs = filtered
	return
}

func (c *client) handleStatusMessage(_ mqtt.Client, m mqtt.Message) {
	topic := m.Topic()
	i := strings.Index(topic, statusPathComplete)
	sm := StatusMessage{
		Toplevel: topic[:i],
		Item:     topic[i+len(statusPathComplete):],
		Value:    PayloadToValue(m.Payload()),
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, sub := range c.subs {
		if topicMatch(sub.pattern, topic) {
			select {
			case sub.ch <- sm:
			default:
			}
		}
	}
}

func (c *client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.closing)
	if c.disconnect {
		c.c.Disconnect(defaultDisconnectQuiesce)
	}
}

type Subscription struct {
	c       *client
	pattern string
	ch      chan StatusMessage
	C       <-chan StatusMessage
}

func (s *Subscription) Unsubscribe() {
	s.c.unsubscribe(s)
}

func topicMatch(pattern, topic string) bool {
	pat := strings.Split(pattern, "/")
	top := strings.Split(topic, "/")

	for i := range pat {
		if i >= len(top) {
			return false
		}
		switch pat[i] {
		case "+":
		case "#":
			return true
		default:
			if pat[i] != top[i] {
				return false
			}
		}
	}
	return true
}
