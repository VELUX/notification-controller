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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/IBM/sarama"

	"sigs.k8s.io/controller-runtime/pkg/log"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
)

type Kafka struct {
	producer sarama.SyncProducer
	topic    string
}

// ensure *Kafka implements Interface.
var _ Interface = &Kafka{}

func NewKafka(brokers, topic, clientID, username, password string, tlsConfig *tls.Config, secretData map[string][]byte) (*Kafka, error) {
	if brokers == "" {
		return nil, errors.New("Kafka brokers cannot be empty")
	}

	if topic == "" {
		return nil, errors.New("Kafka topic cannot be empty")
	}

	// Create Sarama config
	config := sarama.NewConfig()
	config.ClientID = clientID

	// Required for SyncProducer
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	// Add TLS config if provided
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	// Configure SASL if credentials provided
	if err := configureSASL(config, secretData, username, password); err != nil {
		return nil, fmt.Errorf("failed to configure SASL: %w", err)
	}

	configureSASL(config, secretData, username, password)

	// Create producer
	producer, err := sarama.NewSyncProducer(strings.Split(strings.TrimSpace(brokers), ","), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &Kafka{
		producer: producer,
		topic:    topic,
	}, nil
}

// Post posts Flux events to a Google Pub/Sub topic.
func (g *Kafka) Post(ctx context.Context, event eventv1.Event) error {
	// Skip Git commit status update event.
	if event.HasMetadata(eventv1.MetaCommitStatusKey, eventv1.MetaCommitStatusUpdateValue) {
		return nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: g.topic,
		Value: sarama.ByteEncoder(payload),
	}

	partition, offset, err := g.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.FromContext(ctx).V(1).Info("Event published to Kafka Topic",
		"topic", g.topic,
		"offset", offset,
		"partition", partition)

	return nil
}

// configureSASL configures SASL authentication on the provided Sarama config
func configureSASL(config *sarama.Config, secretData map[string][]byte, username, password string) error {
	// If no username/password provided, assume no SASL
	if username == "" || password == "" {
		return nil
	}

	// Enable SASL
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password

	// Parse mechanism (default to PLAIN)
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	if mechanism, exists := secretData["sasl-mechanism"]; exists {
		switch strings.ToUpper(string(mechanism)) {
		case "PLAIN":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "OAUTHBEARER":
			config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s", string(mechanism))
		}
	}

	// Parse version (default to V1)
	if version, exists := secretData["sasl-version"]; exists {
		v, err := strconv.Atoi(string(version))
		if err != nil {
			return fmt.Errorf("invalid SASL version: %s", string(version))
		}
		config.Net.SASL.Version = int16(v)
	}

	// Parse handshake (default to true)
	if handshake, exists := secretData["sasl-handshake"]; exists {
		if strings.ToLower(string(handshake)) == "false" {
			config.Net.SASL.Handshake = false
		}
	}

	// Parse auth identity (optional)
	if authIdentity, exists := secretData["sasl-auth-identity"]; exists {
		config.Net.SASL.AuthIdentity = string(authIdentity)
	}

	return nil
}
