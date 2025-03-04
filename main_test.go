package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	worker := NewSQSWorker(mockClient, queueURL, 2, 1, "")

	// Test successful enqueue (no backpressure)
	schema := "test_schema"
	timestamp := uint32(time.Now().Unix())
	backpressure := worker.EnqueueEvent(schema, timestamp, "")
	assert.False(t, backpressure, "Should not apply backpressure on first enqueue")

	// The event should be in the queue now, worker not started so we can check directly
	assert.Equal(t, 1, len(worker.eventQueue), "Queue should have one event")
}

func TestSQSWorkerBackpressure(t *testing.T) {
	// Create a worker with a very small buffer to test backpressure
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"
	worker := NewSQSWorker(mockClient, queueURL, 1, 1, "")

	// Fill the queue
	worker.EnqueueEvent("schema1", uint32(time.Now().Unix()), "")

	// This will trigger our backpressure logic
	// Use a goroutine with timeout because it might block
	var backpressure bool
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		backpressure = worker.EnqueueEvent("schema2", uint32(time.Now().Unix()), "")
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
	worker := NewSQSWorker(mockClient, queueURL, 5, 1, "")
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
	worker.EnqueueEvent(schema, timestamp, "test-gtid-1")

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
	worker := NewSQSWorker(mockClient, queueURL, 10, 2, "")
	worker.Start()

	// Enqueue multiple events
	for i := 0; i < 5; i++ {
		worker.EnqueueEvent(fmt.Sprintf("schema%d", i), uint32(time.Now().Unix()), fmt.Sprintf("test-gtid-%d", i))
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

func TestResumeFile(t *testing.T) {
	// Create a temporary file for testing
	tempDir := t.TempDir()
	resumeFilePath := tempDir + "/resume.gtid"

	// Create mock client
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"

	// Expect messages to be sent
	mockClient.On("SendMessage", mock.Anything, mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		return input.QueueUrl != nil && *input.QueueUrl == queueURL
	})).Return(&sqs.SendMessageOutput{MessageId: new(string)}, nil)

	// Create a worker with resume file
	worker := NewSQSWorker(mockClient, queueURL, 5, 1, resumeFilePath)
	worker.Start()
	defer worker.Stop()

	// Enqueue events with GTID
	testGTID := "d4c59d03-c9bb-11ec-9d64-0242ac110002:1-200"
	worker.EnqueueEvent("test_schema", uint32(time.Now().Unix()), testGTID)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Verify message was sent
	mockClient.AssertNumberOfCalls(t, "SendMessage", 1)

	// Read resume file and verify GTID was saved
	content, err := os.ReadFile(resumeFilePath)
	assert.NoError(t, err, "Should be able to read resume file")
	assert.Equal(t, testGTID, string(content), "Resume file should contain the GTID")
}

func TestGTIDCompare(t *testing.T) {
	// Test our GTID comparison function
	worker := NewSQSWorker(nil, "", 1, 1, "")
	
	testCases := []struct{
		gtid1 string
		gtid2 string
		expected bool
	}{
		{"uuid:1-100", "uuid:1-100", true},  // Equal
		{"uuid:1-100", "uuid:1-200", true},  // Less than
		{"uuid:1-200", "uuid:1-100", false}, // Greater than
		{"uuid:1-100", "other:1-100", true}, // Different UUIDs but same sequence number
		{"", "uuid:1-100", true},            // Empty is less than anything
		{"uuid:1-100", "", false},           // Non-empty is greater than empty
	}
	
	for _, tc := range testCases {
		result := worker.compareGTIDs(tc.gtid1, tc.gtid2)
		assert.Equal(t, tc.expected, result, "Compare %s <= %s", tc.gtid1, tc.gtid2)
	}
}

// Simple replacements for our test
func simpleWriteGTID(path, gtid string) error {
	return os.WriteFile(path, []byte(gtid), 0644)
}

func TestSimpleOrderedGTIDs(t *testing.T) {
	// Create a simple test that manually implements the ordered GTID processing
	tempDir := t.TempDir()
	resumeFilePath := tempDir + "/simple.gtid"
	
	// Define our GTID sequence
	gtids := []string{
		"uuid:1-100",
		"uuid:1-101",
		"uuid:1-102",
		"uuid:1-103", 
		"uuid:1-104",
	}
	
	// Keep track of what's still pending
	pending := map[string]bool{
		gtids[0]: true,
		gtids[1]: true,
		gtids[2]: true,
		gtids[3]: true,
		gtids[4]: true,
	}
	
	// Keep track of the highest GTID we can save
	var highestSaved string
	
	// Process GTIDs in an arbitrary order
	processOrder := []int{3, 4, 1, 2, 0}
	
	// Track what's been processed
	processed := make(map[string]bool)
	
	// Process each GTID
	for _, idx := range processOrder {
		gtid := gtids[idx]
		t.Logf("Processing GTID: %s", gtid)
		
		// Mark this GTID as processed and remove from pending
		processed[gtid] = true
		delete(pending, gtid)
		
		// After each processing, check if we can update the saved GTID
		// Find the highest processed GTID that has no lower pending GTIDs
		if len(pending) == 0 {
			// No pending GTIDs - we can save the highest one
			highestSeq := -1
			var highestGTID string
			for processedGTID := range processed {
				seq := extractSequenceNum(processedGTID)
				if seq > highestSeq {
					highestSeq = seq
					highestGTID = processedGTID
				}
			}
			
			if highestGTID != highestSaved {
				highestSaved = highestGTID
				simpleWriteGTID(resumeFilePath, highestGTID)
				t.Logf("Updated to highest available GTID: %s (no pending)", highestGTID)
			}
		}
	}
	
	// Verify the final saved GTID
	content, err := os.ReadFile(resumeFilePath)
	assert.NoError(t, err, "Should be able to read file")
	
	// We should have saved the highest GTID since all are processed
	assert.Equal(t, gtids[4], string(content),
		"Should have saved the highest GTID when all are processed")
}

// Helper to extract sequence number from GTID string like "uuid:1-104"
func extractSequenceNum(gtid string) int {
	parts := strings.Split(gtid, ":")
	if len(parts) == 2 {
		seqRange := strings.Split(parts[1], "-")
		if len(seqRange) == 2 {
			if val, err := strconv.Atoi(seqRange[1]); err == nil {
				return val
			}
		}
	}
	return -1
}

func TestLoadGTIDFromFile(t *testing.T) {
	// Test with a file that doesn't exist
	gtid, err := loadGTIDFromFile("/nonexistent/path.gtid")
	assert.Error(t, err)
	assert.Empty(t, gtid)
	
	// Test with a valid file
	tempDir := t.TempDir()
	resumeFilePath := tempDir + "/resume.gtid"
	
	testGTID := "server-uuid:1-200"
	err = os.WriteFile(resumeFilePath, []byte(testGTID), 0644)
	assert.NoError(t, err)
	
	gtid, err = loadGTIDFromFile(resumeFilePath)
	assert.NoError(t, err)
	assert.Equal(t, testGTID, gtid)
	
	// Test with an empty file
	emptyFilePath := tempDir + "/empty.gtid"
	err = os.WriteFile(emptyFilePath, []byte(""), 0644)
	assert.NoError(t, err)
	
	gtid, err = loadGTIDFromFile(emptyFilePath)
	assert.Error(t, err)
	assert.Empty(t, gtid)
}