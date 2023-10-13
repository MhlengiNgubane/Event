package kafka

import (
	"log"
	"time"

	"github.com/IBM/sarama"
)

func RetryConnect(brokers []string, retryInterval time.Duration) chan sarama.Client {
	result := make(chan sarama.Client)

	go func() {
		defer close(result)
		for {
			config := sarama.NewConfig()
			conn, err := sarama.NewClient(brokers, config)
			if err == nil {
				log.Println("connection successfully established")
				result <- conn
				return
			}

			log.Printf("Kafka connection failed with error (retrying in %s): %s", retryInterval.String(), err)
			time.Sleep(retryInterval)
		}
	}()

	return result
}
