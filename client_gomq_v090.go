// +build !goczmq

package boomer

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"

	"github.com/zeromq/gomq"
	"github.com/zeromq/gomq/zmtp"
)

type gomqSocketV090Client struct {
	masterHost string
	masterPort int
	pushPort int
	pullPort int

	pushSocket     *gomq.PushSocket
	pullSocket     *gomq.PullSocket

	dealerSocket gomq.Dealer

	fromMaster             chan *message
	toMaster               chan *message
	disconnectedFromMaster chan bool
	shutdownChan           chan bool
}

func newV090Client(masterHost string, masterPort int) (client *gomqSocketV090Client) {
	log.Println("Boomer is built with gomq support.")
	log.Println("Using the Locust 0.9.0 gomq protocol.")
	client = &gomqSocketV090Client{
		masterHost:             masterHost,
		pushPort:             masterPort,
		pullPort:             masterPort + 1,
		fromMaster:             make(chan *message, 100),
		toMaster:               make(chan *message, 100),
		disconnectedFromMaster: make(chan bool),
		shutdownChan:           make(chan bool),
	}
	return client
}

func (c *gomqSocketV090Client) connect() (err error) {
	pushAddr := fmt.Sprintf("tcp://%s:%d", c.masterHost, c.pushPort)
	pullAddr := fmt.Sprintf("tcp://%s:%d", c.masterHost, c.pullPort)

	pushSocket := gomq.NewPush(zmtp.NewSecurityNull())
	c.pushSocket = pushSocket
	pullSocket := gomq.NewPull(zmtp.NewSecurityNull())
	c.pullSocket = pullSocket

	c.pushSocket.Connect(pushAddr)
	c.pullSocket.Connect(pullAddr)
	log.Printf("Boomer is connected to master(%s:%d|%d) press Ctrl+c to quit.\n", masterHost, masterPort, masterPort+1)
	go c.recv()
	go c.send()
	return nil
}

func (c *gomqSocketV090Client) close() {
	close(c.shutdownChan)
}

func (c *gomqSocketV090Client) recvChannel() chan *message {
	return c.fromMaster
}

func (c *gomqSocketV090Client) recv() {
	defer func() {
		// Temporary work around for https://github.com/zeromq/gomq/issues/75
		err := recover()
		if err != nil {
			log.Printf("%v\n", err)
			debug.PrintStack()
			log.Printf("The underlying socket connected to master(%s:%d) may be broken, please restart both locust and boomer\n", masterHost, masterPort+1)
			runtime.Goexit()
		}
	}()
	for {
		msg, err := c.pullSocket.Recv()
		if err != nil {
			log.Printf("Error reading: %v\n", err)
		} else {
			msgFromMaster, err := newMessageFromBytes(msg)
			if err != nil {
				log.Printf("Msgpack decode fail: %v\n", err)
			} else {
				c.fromMaster <- msgFromMaster
			}
		}
	}
}

func (c *gomqSocketV090Client) sendChannel() chan *message {
	return c.toMaster
}

func (c *gomqSocketV090Client) send() {
	for {
		select {
		case <-c.shutdownChan:
			return
		case msg := <-c.toMaster:
			c.sendMessage(msg)
			if msg.Type == "quit" {
				c.disconnectedFromMaster <- true
			}
		}
	}
}

func (c *gomqSocketV090Client) sendMessage(msg *message) {
	serializedMessage, err := msg.serialize()
	if err != nil {
		log.Printf("Msgpack encode fail: %v\n", err)
		return
	}
	err = c.pushSocket.Send(serializedMessage)
	if err != nil {
		log.Printf("Error sending: %v\n", err)
	}
}

func (c *gomqSocketV090Client) disconnectedChannel() chan bool {
	return c.disconnectedFromMaster
}
