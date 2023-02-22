package MQTTClient

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Config struct {
	Message 		 *string
	MessageCount *int
	MessageSize  *int
	Interval     *int
	TargetTopic  *string
	Username     *string
	Password     *string
	Host         *string
	Schedule     *string
	Port         *int
	IdAsSubTopic *bool
	QoS          *int
	Mutator 		 *string
	MutationRate *float64
	Debug 			 *bool
	Disallowed   *string
}

type Client struct {
	ID             int
	Config         Config
	Connection     mqtt.Client
	Updates        chan int
	ConnectionDone chan struct{}
}

func (c *Client) Connect() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", *c.Config.Host, *c.Config.Port))
	opts.SetClientID(fmt.Sprintf("mqtt-load-generator-%d", c.ID))
	opts.SetUsername(*c.Config.Username)
	opts.SetPassword(*c.Config.Password)
	opts.CleanSession = true
	// We use a closure so we can have access to the scope if required
	opts.OnConnect = func(client mqtt.Client) {
		c.ConnectionDone <- struct{}{}
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		optionsReader := client.OptionsReader()
		fmt.Printf("Connection lost for client '%s' message: %v\n", optionsReader.ClientID(), err.Error())
	}

	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		// We just send a 1 for each received message
		c.Updates <- 1
	})

	mqttClient := mqtt.NewClient(opts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error establishing MQTT connection:", token.Error().Error())
		os.Exit(1)
	}

	c.Connection = mqttClient
}

func (c Client) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	var payload string
	if *c.Config.Message == "" {
		payloadBytes := make([]byte, *c.Config.MessageSize)
		rand.Read(payloadBytes)
		payload = string(payloadBytes)
	} else {
		payload = *c.Config.Message
	}

	var topic string
	if *c.Config.IdAsSubTopic {
		topic = fmt.Sprintf("%s/%d", *c.Config.TargetTopic, c.ID)
	} else {
		topic = *c.Config.TargetTopic
	}

	for i := 0; i < *c.Config.MessageCount; i++ {

		mutant_topic := c.Mutate(topic)
		mutant_payload := c.Mutate(payload)

		if *c.Config.Debug {
			optionsReader := c.Connection.OptionsReader()
			fmt.Printf("%s Pub: %s %s\n", optionsReader.ClientID(), mutant_topic, mutant_payload)
		}
		token := c.Connection.Publish(mutant_topic, byte(*c.Config.QoS), false, mutant_payload)
		token.Wait()

		// If the interval is zero skip this logic
		interval := float64(*c.Config.Interval)
		if interval > 0 {
			// Default case is flat
			sleepTime := interval
			if *c.Config.Schedule == "normal" {
				sleepTime = interval + interval*rand.NormFloat64()/2

			} else if *c.Config.Schedule == "random" {
				sleepTime = interval * 2 * rand.Float64()
			}
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		}
		c.Updates <- 1
	}
	c.Connection.Disconnect(1)
}

func (c Client) Subscribe(topic string) {
	token := c.Connection.Subscribe(topic, 1, nil)
	token.Wait()
	fmt.Printf("Subscribed to topic '%s'\n", topic)
}

// Mutates a message based on client config
// alfa Replace only alpha numeric characters by other alpha numeric characters.
// sym Replace any character by other ascii printable characters.
// bin Replace any character by any random byte.
// *any other value* Does nothing. Safe default.
func (c Client) Mutate(message string) string {
	const alfanum = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	mutator := *c.Config.Mutator

	if  mutator != "alfa" && mutator != "sym" && mutator != "bin" { return message }

	mutant := ""
	for _, ch := range message {
		if rand.Float64() > *c.Config.MutationRate {
			mutant += string(ch)
			continue
		}

		var mutant_ch byte
		if mutator == "alfa" {
			mutant_ch = alfanum[rand.Intn(len(alfanum))]
		}
		if mutator == "sym" {
			mutant_ch = byte(rand.Intn(95) + 32)
		}
		if mutator == "bin" {
			mutant_ch = byte(int(ch) ^ rand.Intn(256))
		}

		// If the character is not allowed return original one.
		if *c.Config.Disallowed != "" && strings.Contains(*c.Config.Disallowed, string(mutant_ch)) {
			mutant_ch = byte(ch)
		}

		mutant += string(mutant_ch)
	}

	return mutant
}