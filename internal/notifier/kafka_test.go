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
	"errors"
	"testing"

	. "github.com/onsi/gomega"
)

func TestNewKafka(t *testing.T) {
	tests := []struct {
		name          string
		brokers       string
		topic         string
		expectedErr   error
		expectedTopic string
	}{
		{
			name:        "empty topic is not allowed",
			brokers:     "localhost:9092",
			topic:       "",
			expectedErr: errors.New("Kafka topic cannot be empty"),
		},
		{
			name:          "valid inputs",
			brokers:       "localhost:9092",
			topic:         "topic",
			expectedErr:   nil,
			expectedTopic: "topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			provider, err := NewKafka(tt.brokers, tt.topic, "", "", "", nil, nil, nil)

			if tt.expectedErr != nil {
				g.Expect(err).To(Equal(tt.expectedErr))
				g.Expect(provider).To(BeNil())
			} else {
				g.Expect(err).To(BeNil())
				g.Expect(provider).NotTo(BeNil())

				g.Expect(provider.topic).To(Equal(tt.expectedTopic))

			}
		})
	}
}
