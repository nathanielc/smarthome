package client

import (
	"path"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/nathanielc/smarthome"
)

type SubscribeCallback func(toplevel, item string, value smarthome.Value)

type Client interface {
	// Set publishes a set message with the value
	Set(toplevel, item string, value string) error
	// Get publishes a get request message.
	// You must Subscribe before calls to Get in order to receive the response.
	Get(toplevel, item string) error
	// Command publishes a command to the toplevel topic.
	Command(toplevel string, cmd []byte) error

	// Subscribe to receive callbacks whenever a status message is received.
	Subscribe(toplevel, item string, callback SubscribeCallback) error
	// Unsubscribe to stop receiving subscription callbacks.
	Unsubscribe(toplevel, item string)
}

type client struct {
	opts *mqtt.ClientOptions
	c    mqtt.Client

	statusSubed bool
}

func (c *client) Set(toplevel, item string, value string) error {
	topic := path.Join(toplevel, smarthome.Set, item)
	token := c.c.Publish(topic, 0, false, value)
	token.Wait()
	return token.Error()
}

func (c *client) Get(toplevel, item string) error {
	getTopic := path.Join(toplevel, smarthome.Get, item)
	token := c.c.Publish(getTopic, 0, false, "?")
	token.Wait()
	return token.Error()
}

func (c *client) Command(toplevel string, cmd []byte) error {
	topic := path.Join(toplevel, smarthome.Command)
	token := c.c.Publish(topic, 0, false, cmd)
	token.Wait()
	return token.Error()
}

func (c *client) Subscribe(toplevel, item string, callback SubscribeCallback) error {
	topic := path.Join(toplevel, smarthome.Status, item)
	token := c.c.Subscribe(topic, 0, func(c mqtt.Client, m mqtt.Message) {
		value := smarthome.PayloadToValue(m.Payload())
		callback(toplevel, item, value)
	})
	token.Wait()
	return token.Error()
}
func (c *client) Unsubscribe(toplevel, item string) error {
	topic := path.Join(toplevel, smarthome.Status, item)
	token := c.c.Unsubscribe(topic)
	token.Wait()
	return token.Error()
}
