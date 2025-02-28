package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/siddontang/go-log/log"
	"github.com/stretchr/testify/assert"
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

// Tests for the original handleSchemaEvent function

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

// New tests for the SQSWorker

func TestSQSWorkerEnqueue(t *testing.T) {
	// Create a worker with a small buffer to test enqueue behavior
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"
	worker := NewSQSWorker(mockClient, queueURL, 2, 1)

	// Test successful enqueue (no backpressure)
	schema := "test_schema"
	timestamp := uint32(time.Now().Unix())
	backpressure := worker.EnqueueEvent(schema, timestamp)
	assert.False(t, backpressure, "Should not apply backpressure on first enqueue")

	// The event should be in the queue now, worker not started so we can check directly
	assert.Equal(t, 1, len(worker.eventQueue), "Queue should have one event")
}

func TestSQSWorkerBackpressure(t *testing.T) {
	// Create a worker with a very small buffer to test backpressure
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"
	worker := NewSQSWorker(mockClient, queueURL, 1, 1)

	// Fill the queue
	worker.EnqueueEvent("schema1", uint32(time.Now().Unix()))

	// This will trigger our backpressure logic
	// Use a goroutine with timeout because it might block
	var backpressure bool
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		backpressure = worker.EnqueueEvent("schema2", uint32(time.Now().Unix()))
	}()

	// Wait for the enqueue operation to complete or timeout
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		// Completed
	case <-time.After(500 * time.Millisecond):
		t.Fatal("EnqueueEvent took too long, likely deadlocked")
	}

	// We should have applied backpressure
	assert.True(t, backpressure, "Should apply backpressure when queue is full")
}

func TestSQSWorkerProcessing(t *testing.T) {
	// Create mock client that will actually handle SQS calls
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"

	// Create a worker and start it
	worker := NewSQSWorker(mockClient, queueURL, 5, 1)
	worker.Start()
	defer worker.Stop()

	// Setup the mock expectations
	mockClient.On("SendMessage", mock.Anything, mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		// Our match function is basic here but could be more specific
		return input.QueueUrl != nil && *input.QueueUrl == queueURL
	})).Return(&sqs.SendMessageOutput{
		MessageId: new(string),
	}, nil)

	// Enqueue an event
	schema := "test_schema"
	timestamp := uint32(time.Now().Unix())
	worker.EnqueueEvent(schema, timestamp)

	// Give some time for the worker to process
	time.Sleep(100 * time.Millisecond)

	// Verify a message was sent
	mockClient.AssertNumberOfCalls(t, "SendMessage", 1)
}

func TestSQSWorkerGracefulShutdown(t *testing.T) {
	// Create mock client with a delayed response to test graceful shutdown
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"

	mockClient.On("SendMessage",
		mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("*sqs.SendMessageInput")).
		Run(func(args mock.Arguments) {
			// Simulate work with a delay
			time.Sleep(50 * time.Millisecond)
		}).
		Return(&sqs.SendMessageOutput{MessageId: new(string)}, nil)

	// Create a worker and start it
	worker := NewSQSWorker(mockClient, queueURL, 10, 2)
	worker.Start()

	// Enqueue multiple events
	for i := 0; i < 5; i++ {
		worker.EnqueueEvent(fmt.Sprintf("schema%d", i), uint32(time.Now().Unix()))
	}

	// Start shutdown - this should wait for queued messages to process
	start := time.Now()
	worker.Stop()
	elapsed := time.Since(start)

	// With 5 messages and 2 workers, should take at least 100-150ms
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(100),
		"Shutdown should wait for messages to be processed")

	// Verify all messages were processed
	mockClient.AssertNumberOfCalls(t, "SendMessage", 5)
}
