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
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

// func TestNewKafka(t *testing.T) {
// 	tests := []struct {
// 		name          string
// 		brokers       string
// 		topic         string
// 		expectedErr   error
// 		expectedTopic string
// 	}{
// 		{
// 			name:        "empty topic is not allowed",
// 			brokers:     "localhost:9092",
// 			topic:       "",
// 			expectedErr: errors.New("Kafka topic cannot be empty"),
// 		},
// 		{
// 			name:          "valid inputs",
// 			brokers:       "localhost:9092",
// 			topic:         "topic",
// 			expectedErr:   nil,
// 			expectedTopic: "topic",
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := NewWithT(t)
// 			provider, err := NewKafka(tt.brokers, tt.topic, "", "", "", nil, nil, nil)
//
// 			if tt.expectedErr != nil {
// 				g.Expect(err).To(Equal(tt.expectedErr))
// 				g.Expect(provider).To(BeNil())
// 			} else {
// 				g.Expect(err).To(BeNil())
// 				g.Expect(provider).NotTo(BeNil())
//
// 				g.Expect(provider.topic).To(Equal(tt.expectedTopic))
//
// 			}
// 		})
// 	}
// }

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

			// Create mock producer
			mockProducer := mocks.NewSyncProducer(t, nil)

			if !tt.skipEvent {
				// Configure expected behavior - expect message to be sent successfully
				mockProducer.ExpectSendMessageAndSucceed()
			}

			// Create Kafka instance with mock producer
			kafka := &Kafka{
				producer: mockProducer,
				topic:    tt.topic,
				headers:  tt.headers,
			}

			// Test the Post method
			err := kafka.Post(context.Background(), tt.event)

			if tt.expectedError {
				g.Expect(err).To(HaveOccurred())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
			}

			// Close the mock producer to verify all expectations were met
			err = mockProducer.Close()
			g.Expect(err).NotTo(HaveOccurred())
		})
	}
}
