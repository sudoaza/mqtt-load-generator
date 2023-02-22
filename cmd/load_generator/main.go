package main

import (
	"flag"
	"fmt"
	"sync"

	MQTTClient "github.com/pablitovicente/mqtt-load-generator/pkg/MQTTClient"
	"github.com/schollz/progressbar/v3"
)

func main() {
	// Argument parsing
	message := flag.String("m", "", "Message to send and maybe mutate.")
	messageCount := flag.Int("c", 1000, "Number of messages to send")
	messageSize := flag.Int("s", 100, "Size in bytes of the message payload")
	interval := flag.Int("i", 1, "Milliseconds to wait between messages")
	schedule := flag.String("z", "normal", "Distribution of time between messages: 'flat': always wait Interval between messages, 'normal': wait a random amount between messages with mean equal to the interval and stdev to half interval, 'random': wait a random amount between messages with mean equal to the interval.")
	targetTopic := flag.String("t", "/load", "Target MQTT topic to publish messages to")
	username := flag.String("u", "", "MQTT username")
	password := flag.String("P", "", "MQTT password")
	host := flag.String("h", "localhost", "MQTT host")
	port := flag.Int("p", 1883, "MQTT port")
	numberOfClients := flag.Int("n", 1, "Number of concurrent MQTT clients")
	idAsSubTopic := flag.Bool("suffix", false, "If set to true the MQTT client ID will be used as an additional level to the topic specified by 't'")
	qos := flag.Int("q", 1, "MQTT QoS used by all clients")
	mutator := flag.String("M", "", "Mutate the topic and message: 'alfa': only replace alfa numeric characters by others (usually safe). 'sym': replace any character by any printable ASCII (less safe). 'bin': flip bits at random (can crash stuff).")
	mutationRate := flag.Float64("Mr", 0.07, "Mutation rate, probability of mutating a character.")
	debug := flag.Bool("debug", false, "Show debug output.")
	disallowed := flag.String("disallowed", "", "List of characters not to mutate into. HiveMQ does not like '+#'.")

	flag.Parse()

	if *qos < 0 || *qos > 2 {
		panic("QoS should be any of [0, 1, 2]")
	}

	// General Client Config
	mqttClientConfig := MQTTClient.Config{
		Message: message,
		MessageCount: messageCount,
		MessageSize:  messageSize,
		Interval:     interval,
		Schedule:     schedule,
		TargetTopic:  targetTopic,
		Username:     username,
		Password:     password,
		Host:         host,
		Port:         port,
		IdAsSubTopic: idAsSubTopic,
		QoS:          qos,
		Mutator: mutator,
		MutationRate: mutationRate,
		Debug: debug,
		Disallowed: disallowed,
	}

	updates := make(chan int)

	pool := MQTTClient.Pool{
		SetupDone:   make(chan struct{}),
		MqttClients: make([]*MQTTClient.Client, 0),
	}
	fmt.Printf("Setting up %d MQTT clients\n", *numberOfClients)
	pool.New(numberOfClients, mqttClientConfig, updates)
	// Wait until all the setup is done
	<-pool.SetupDone
	fmt.Println("All clients connected, starting publishing messages")
	var wg sync.WaitGroup
	pool.Start(&wg)

	bar := progressbar.Default(int64(*messageCount) * int64(*numberOfClients))

	go func(updates chan int) {
		for update := range updates {
			// Updating the progress bar while outputing debug is messy
			if !*debug {
				bar.Add(update)
			}
		}
	}(updates)

	// Hacky way of avoiding the progress bar going away.
	// Todo: check why this happens
	bar.Add(0)

	wg.Wait()
}
