/*
Copyright 2023 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package notifier

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/IBM/sarama"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
)


type Kafka struct {
	producer sarama.SyncProducer
	topic string
}

// ensure *GooglePubSub implements Interface.
var _ Interface = &Kafka{}


func NewKafka(opts *notifierOptions) (*Kafka, error) {
	if opts.URL == "" {
		return nil, errors.New("Kafka brokers cannot be empty")
	}

	if opts.Channel == "" {
		return nil, errors.New("Kafka topic cannot be empty")
	}

	brokers := strings.Split(strings.TrimSpace(opts.URL), ",")

	// Create Sarama config
	config := sarama.NewConfig()
	config.ClientID = opts.ProviderName

	// Add TLS config if provided
	if opts.TLSConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = opts.TLSConfig
	}

	// Create producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &Kafka{
		producer: producer,
		topic:    opts.Channel,
	}, nil
}

// Post posts Flux events to a Google Pub/Sub topic.
func (g *Kafka) Post(ctx context.Context, event eventv1.Event) error {
	// Skip Git commit status update event.
	if event.HasMetadata(eventv1.MetaCommitStatusKey, eventv1.MetaCommitStatusUpdateValue) {
		return nil
	}

	return nil

}

