package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/siddontang/go-log/log"
	"github.com/stretchr/testify/mock"
)

// Setup function to silence logs during tests
func init() {
	// Set up a null logger to discard logs during tests
	nullHandler, _ := log.NewNullHandler()
	logger := log.NewDefault(nullHandler)
	log.SetDefaultLogger(logger)
}

// MockSQSClient is a mock implementation of the SQS client
type MockSQSClient struct {
	mock.Mock
}

// SendMessage mocks the SendMessage method
func (m *MockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
}

func TestHandleSchemaEventWithoutSQS(t *testing.T) {
	// Test when no SQS queue URL is provided
	schema := "test_schema"
	timestamp := uint32(time.Now().Unix())

	// This should just log and not cause any errors
	handleSchemaEvent(nil, "", schema, timestamp)
	// No assertion needed as we're just making sure it doesn't crash
}

func TestHandleSchemaEventWithSQS(t *testing.T) {
	// Create a mock SQS client
	mockClient := new(MockSQSClient)

	schema := "test_schema"
	timestamp := uint32(time.Now().Unix())
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"

	// Create the expected event
	eventTime := time.Unix(int64(timestamp), 0)
	expectedEvent := SchemaEvent{
		Schema:    schema,
		Timestamp: eventTime,
	}

	// Convert to JSON to match what the function would do
	messageBody, _ := json.Marshal(expectedEvent)
	messageBodyStr := string(messageBody)

	// Set up the expectation on the mock
	mockClient.On("SendMessage", mock.Anything, &sqs.SendMessageInput{
		QueueUrl:               &queueURL,
		MessageBody:            &messageBodyStr,
		MessageGroupId:         &schema,
		MessageDeduplicationId: &schema,
	}).Return(&sqs.SendMessageOutput{
		MessageId: new(string),
	}, nil)

	// Call the function with our mock
	handleSchemaEvent(mockClient, queueURL, schema, timestamp)

	// Verify that our expectations were met
	mockClient.AssertExpectations(t)
}
