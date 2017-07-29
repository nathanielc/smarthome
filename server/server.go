package server

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/nathanielc/smarthome"
)

const (
	defaultDisconnectQuiesce = 250 //ms
)

type Handler interface {
	// Set handles a set request.
	Set(toplevel, item string, value interface{})
	// Get handles a get request.
	Get(toplevel, item string) (smarthome.Value, bool)
	// Command handles a command request.
	Command(toplevel string, cmd []byte)
}

type ConnectionState int

const (
	Connected ConnectionState = iota
	Disconnected
)

type Server interface {
	// Connect to the MQTT broker.
	Connect() error
	// Disconnect from the MQTT broker.
	Disconnect()

	// Publish the state of the hardware connection.
	PublishHWStatus(ConnectionState) error

	// Publish a status message.
	PublishStatus(item string, value smarthome.Value) error
	// Publish a one-shot message.
	PublishOneShotStatus(item string, value smarthome.Value) error
}

type server struct {
	toplevel string

	connectTopic,
	setTopic,
	setTopicAnchored,
	getTopic,
	commandTopic,
	statusTopic string

	h Handler

	opts *mqtt.ClientOptions
	c    mqtt.Client
}

func New(toplevel string, h Handler, opts *mqtt.ClientOptions) Server {
	ct := path.Join(toplevel, smarthome.Connect)
	// Setup Will
	opts.SetWill(ct, "0", 0, false)
	st := path.Join(toplevel, smarthome.Set)
	sta := st + "/"
	return &server{
		toplevel:         toplevel,
		connectTopic:     ct,
		setTopic:         st,
		setTopicAnchored: sta,
		getTopic:         path.Join(toplevel, smarthome.Get),
		commandTopic:     path.Join(toplevel, smarthome.Command),
		statusTopic:      path.Join(toplevel, smarthome.Status),
		h:                h,
		opts:             opts,
	}
}

func (s *server) Connect() error {
	s.c = mqtt.NewClient(s.opts)
	if token := s.c.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	// Subscribe to Set, Get and Command topics

	if token := s.c.Subscribe(s.setTopic, 0, s.handleSet); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	if token := s.c.Subscribe(s.getTopic, 0, s.handleGet); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	if token := s.c.Subscribe(s.commandTopic, 0, s.handleCommand); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (s *server) handleSet(c mqtt.Client, m mqtt.Message) {
	item := strings.TrimPrefix(m.Topic(), s.setTopicAnchored)
	v := smarthome.PayloadToValue(m.Payload())
	s.h.Set(s.toplevel, item, v.Value)
}

func (s *server) handleGet(c mqtt.Client, m mqtt.Message) {
	item := strings.TrimPrefix(m.Topic(), s.setTopicAnchored)
	v, ok := s.h.Get(s.toplevel, item)
	if ok {
		s.publishStatus(item, v, true)
	}
}

func (s *server) handleCommand(c mqtt.Client, m mqtt.Message) {
	s.h.Command(s.toplevel, m.Payload())
}

func (s *server) Disconnect() {
	s.c.Disconnect(defaultDisconnectQuiesce)
}

func (s *server) PublishHWStatus(state ConnectionState) error {
	var value string
	switch state {
	case Disconnected:
		value = "1"
	case Connected:
		value = "2"
	}
	token := s.c.Publish(s.connectTopic, 0, false, value)
	token.Wait()
	return token.Error()
}

func (s *server) PublishStatus(item string, value smarthome.Value) error {
	return s.publishStatus(item, value, false)
}
func (s *server) PublishOneShotStatus(item string, value smarthome.Value) error {
	return s.publishStatus(item, value, true)
}
func (s *server) publishStatus(item string, value smarthome.Value, oneshot bool) error {
	var payload []byte
	if value.Time.IsZero() && value.LastChanged.IsZero() {
		payload = []byte(fmt.Sprintf("%v", value.Value))
	} else {
		p, err := json.Marshal(value)
		if err != nil {
			return err
		}
		payload = p
	}
	token := s.c.Publish(path.Join(s.statusTopic, item), 0, !oneshot, payload)
	token.Wait()
	return token.Error()
}
