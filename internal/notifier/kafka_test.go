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
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name        string
		brokers     string
		expected    []string
		expectedErr string
	}{
		{
			name:        "empty string",
			brokers:     "",
			expectedErr: "Kafka brokers cannot be empty",
		},
		{
			name:     "single broker",
			brokers:  "localhost:9092",
			expected: []string{"localhost:9092"},
		},
		{
			name:     "multiple brokers",
			brokers:  "broker1:9092,broker2:9092,broker3:9092",
			expected: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name:     "brokers with whitespace",
			brokers:  " broker1:9092 , broker2:9092 ",
			expected: []string{"broker1:9092", "broker2:9092"},
		},
		{
			name:        "only commas",
			brokers:     ",,,",
			expectedErr: "Kafka brokers cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			result, err := parseBrokers(tt.brokers)
			if tt.expectedErr != "" {
				g.Expect(err).To(HaveOccurred())
				g.Expect(err.Error()).To(Equal(tt.expectedErr))
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(result).To(Equal(tt.expected))
			}
		})
	}
}

func TestBuildKafkaConfig(t *testing.T) {
	tests := []struct {
		name        string
		clientID    string
		username    string
		password    string
		tlsConfig   *tls.Config
		secretData  map[string][]byte
		expectedErr string
		validate    func(g Gomega, config *sarama.Config)
	}{
		{
			name:     "basic config",
			clientID: "test-client",
			validate: func(g Gomega, config *sarama.Config) {
				g.Expect(config.ClientID).To(Equal("test-client"))
				g.Expect(config.Producer.Return.Errors).To(BeTrue())
				g.Expect(config.Producer.Return.Successes).To(BeTrue())
				g.Expect(config.Net.TLS.Enable).To(BeFalse())
				g.Expect(config.Net.SASL.Enable).To(BeFalse())
			},
		},
		{
			name:      "with TLS",
			clientID:  "test-client",
			tlsConfig: &tls.Config{},
			validate: func(g Gomega, config *sarama.Config) {
				g.Expect(config.Net.TLS.Enable).To(BeTrue())
				g.Expect(config.Net.TLS.Config).NotTo(BeNil())
			},
		},
		{
			name:     "with SASL plain",
			clientID: "test-client",
			username: "user",
			password: "pass",
			validate: func(g Gomega, config *sarama.Config) {
				g.Expect(config.Net.SASL.Enable).To(BeTrue())
				g.Expect(config.Net.SASL.User).To(Equal("user"))
				g.Expect(config.Net.SASL.Password).To(Equal("pass"))
				g.Expect(config.Net.SASL.Mechanism).To(Equal(sarama.SASLMechanism("PLAIN")))
			},
		},
		{
			name:     "with SASL oauth",
			clientID: "test-client",
			username: "user",
			password: "pass",
			secretData: map[string][]byte{
				"sasl-mechanism": []byte("OAUTHBEARER"),
			},
			validate: func(g Gomega, config *sarama.Config) {
				g.Expect(config.Net.SASL.Enable).To(BeTrue())
				g.Expect(config.Net.SASL.User).To(Equal("user"))
				g.Expect(config.Net.SASL.Password).To(Equal("pass"))
				g.Expect(config.Net.SASL.Mechanism).To(Equal(sarama.SASLMechanism("OAUTHBEARER")))
			},
		},
		{
			name:     "unsupported SASL mechanism",
			username: "user",
			password: "pass",
			secretData: map[string][]byte{
				"sasl-mechanism": []byte("SCRAM"),
			},
			expectedErr: "failed to configure SASL: unsupported SASL mechanism: SCRAM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			config, err := buildKafkaConfig(tt.clientID, tt.username, tt.password, tt.tlsConfig, tt.secretData)
			if tt.expectedErr != "" {
				g.Expect(err).To(HaveOccurred())
				g.Expect(err.Error()).To(Equal(tt.expectedErr))
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				tt.validate(g, config)
			}
		})
	}
}

func TestMapToRecordHeaders(t *testing.T) {
	g := NewWithT(t)

	headers := mapToRecordHeaders(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})

	g.Expect(headers).To(HaveLen(2))
	g.Expect(headers).To(ContainElement(sarama.RecordHeader{
		Key: []byte("key1"), Value: []byte("value1"),
	}))
	g.Expect(headers).To(ContainElement(sarama.RecordHeader{
		Key: []byte("key2"), Value: []byte("value2"),
	}))
}

func TestMapToRecordHeaders_Empty(t *testing.T) {
	g := NewWithT(t)
	headers := mapToRecordHeaders(nil)
	g.Expect(headers).To(BeEmpty())
}

func TestKafka_Post(t *testing.T) {
	tests := []struct {
		name          string
		event         eventv1.Event
		topic         string
		headers       []sarama.RecordHeader
		expectedError bool
		skipEvent     bool
	}{
		{
			name:  "successful post",
			topic: "test-topic",
			event: eventv1.Event{
				InvolvedObject: corev1.ObjectReference{
					Kind: "Kustomization",
					Name: "gitops-system",
				},
				Reason: "ApplySucceeded",
			},
			expectedError: false,
		},
		{
			name:  "post with headers",
			topic: "test-topic",
			headers: []sarama.RecordHeader{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
			event: eventv1.Event{
				InvolvedObject: corev1.ObjectReference{
					Kind: "Kustomization",
					Name: "gitops-system",
				},
				Reason: "ApplySucceeded",
			},
			expectedError: false,
		},
		{
			name:  "skip commit status event",
			topic: "test-topic",
			event: eventv1.Event{
				Metadata: map[string]string{
					"commit_status": "update",
				},
			},
			skipEvent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)

			mockProducer := mocks.NewSyncProducer(t, nil)

			if !tt.skipEvent {
				mockProducer.ExpectSendMessageAndSucceed()
			}

			kafka := &Kafka{
				producer: mockProducer,
				topic:    tt.topic,
				headers:  tt.headers,
			}

			err := kafka.Post(context.Background(), tt.event)

			if tt.expectedError {
				g.Expect(err).To(HaveOccurred())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
			}

			err = mockProducer.Close()
			g.Expect(err).NotTo(HaveOccurred())
		})
	}
}
