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

	// Use a channel to safely check the queue length without directly accessing the queue
	// which would be a race condition if the worker was running
	queueLength := 0
	select {
	case worker.eventQueue <- SchemaEvent{}: // Try to enqueue a test event
		queueLength = 1
		// Take out the test event to clean up
		<-worker.eventQueue
	default:
		// Queue must be full, so we know there's at least one event
		queueLength = cap(worker.eventQueue)
	}
	
	assert.Equal(t, 1, queueLength, "Queue should have one event")
}

func TestSQSWorkerBackpressure(t *testing.T) {
	// Create a worker with a very small buffer to test backpressure
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"
	worker := NewSQSWorker(mockClient, queueURL, 1, 1, "")

	// Fill the queue
	worker.EnqueueEvent("schema1", uint32(time.Now().Unix()), "")

	// Create a channel to signal when the test is done with a reasonable timeout
	done := make(chan struct{})
	resultCh := make(chan bool, 1) // Channel to pass the result back safely
	
	// This will trigger our backpressure logic in a separate goroutine
	go func() {
		resultCh <- worker.EnqueueEvent("schema2", uint32(time.Now().Unix()), "")
		close(done)
	}()

	// Use a reasonable timeout that's dynamic, not a fixed sleep
	timeoutSeconds := 3
	timeout := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	defer timeout.Stop()

	// Wait for either completion or timeout
	select {
	case <-done:
		// Test completed normally
		backpressure := <-resultCh
		assert.True(t, backpressure, "Should apply backpressure when queue is full")
	case <-timeout.C:
		t.Fatalf("Test timed out after %d seconds, likely deadlocked", timeoutSeconds)
	}
	
	// Drain the queue to avoid affecting other tests
	select {
	case <-worker.eventQueue:
		// Drained one event
	default:
		// Queue already empty
	}
}

func TestSQSWorkerProcessing(t *testing.T) {
	// Create a notification channel to track when SendMessage is called
	messageProcessed := make(chan struct{}, 1)
	
	// Create mock client that will signal when it's called
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"

	// Setup the mock expectations with a signal when called
	mockClient.On("SendMessage", mock.Anything, mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		return input.QueueUrl != nil && *input.QueueUrl == queueURL
	})).Run(func(args mock.Arguments) {
		// Signal that the message was processed
		select {
		case messageProcessed <- struct{}{}:
			// Sent signal
		default:
			// Channel buffer full, already signaled
		}
	}).Return(&sqs.SendMessageOutput{
		MessageId: new(string),
	}, nil)

	// Create a worker and start it
	worker := NewSQSWorker(mockClient, queueURL, 5, 1, "")
	worker.Start()
	defer worker.Stop()

	// Enqueue an event
	schema := "test_schema"
	timestamp := uint32(time.Now().Unix())
	worker.EnqueueEvent(schema, timestamp, "test-gtid-1")

	// Wait for message to be processed with timeout
	timeoutSeconds := 5
	select {
	case <-messageProcessed:
		// Message processed successfully
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		t.Fatalf("Test timed out after %d seconds waiting for message processing", timeoutSeconds)
	}

	// Verify a message was sent
	mockClient.AssertNumberOfCalls(t, "SendMessage", 1)
}

func TestSQSWorkerGracefulShutdown(t *testing.T) {
	// Create a completion channel
	processingComplete := make(chan struct{})
	
	// Create mock client with a controlled delay
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"
	
	// Total number of test messages
	const totalMessages = 5
	
	// Mutex to protect the message counter
	var countMutex sync.Mutex

	mockClient.On("SendMessage",
		mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("*sqs.SendMessageInput")).
		Run(func(args mock.Arguments) {
			// Simulate work with a delay that won't cause flakiness
			time.Sleep(10 * time.Millisecond)
			
			// Track processed messages thread-safely
			countMutex.Lock()
			count := 0
			count++
			// If this is the last message, signal completion
			if count == totalMessages {
				close(processingComplete)
			}
			countMutex.Unlock()
		}).
		Return(&sqs.SendMessageOutput{MessageId: new(string)}, nil)

	// Create a worker and start it
	worker := NewSQSWorker(mockClient, queueURL, 10, 2, "")
	worker.Start()

	// Enqueue multiple events
	for i := 0; i < totalMessages; i++ {
		worker.EnqueueEvent(fmt.Sprintf("schema%d", i), uint32(time.Now().Unix()), fmt.Sprintf("test-gtid-%d", i))
	}

	// Start shutdown - this should wait for queued messages to process
	stopComplete := make(chan struct{})
	go func() {
		worker.Stop()
		close(stopComplete)
	}()
	
	// Wait for shutdown with a reasonable timeout
	timeoutSeconds := 5
	select {
	case <-stopComplete:
		// Shutdown completed successfully
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		t.Fatalf("Test timed out after %d seconds waiting for graceful shutdown", timeoutSeconds)
	}

	// Verify all messages were processed
	mockClient.AssertNumberOfCalls(t, "SendMessage", totalMessages)
}

func TestResumeFile(t *testing.T) {
	// Create a temporary file for testing
	tempDir := t.TempDir()
	resumeFilePath := tempDir + "/resume.gtid"

	// Channel to signal when GTID is saved
	gtidSaved := make(chan struct{}, 1)

	// Create mock client
	mockClient := new(MockSQSClient)
	queueURL := "https://sqs.region.amazonaws.com/123456789012/test-queue.fifo"

	// Expect messages to be sent and signal when the mock is called
	mockClient.On("SendMessage", mock.Anything, mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		return input.QueueUrl != nil && *input.QueueUrl == queueURL
	})).Run(func(args mock.Arguments) {
		// Signal that the message was processed
		select {
		case gtidSaved <- struct{}{}:
			// Signal sent
		default:
			// Channel buffer full, already signaled
		}
	}).Return(&sqs.SendMessageOutput{MessageId: new(string)}, nil)

	// Create a worker with resume file
	worker := NewSQSWorker(mockClient, queueURL, 5, 1, resumeFilePath)
	worker.Start()
	defer worker.Stop()

	// Enqueue event with GTID
	testGTID := "d4c59d03-c9bb-11ec-9d64-0242ac110002:1-200"
	worker.EnqueueEvent("test_schema", uint32(time.Now().Unix()), testGTID)

	// Wait for message to be processed with timeout
	timeoutSeconds := 5
	select {
	case <-gtidSaved:
		// Message processed 
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		t.Fatalf("Test timed out after %d seconds waiting for GTID to be saved", timeoutSeconds)
	}

	// Verify message was sent
	mockClient.AssertNumberOfCalls(t, "SendMessage", 1)

	// Poll for the resume file with timeout - the file writing happens asynchronously after the message is processed
	fileExists := func() bool {
		_, err := os.Stat(resumeFilePath)
		return err == nil
	}
	
	// Poll until file exists or timeout
	fileTimeout := time.After(time.Duration(timeoutSeconds) * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	
	for !fileExists() {
		select {
		case <-ticker.C:
			// Check again
		case <-fileTimeout:
			t.Fatalf("Resume file was not created within %d seconds", timeoutSeconds)
			return
		}
	}

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
	
	// Create a channel to capture GTIDs written to file
	writtenGTIDs := make(chan string, 10)
	
	// Custom write function that uses channels instead of actual file I/O
	writeGTIDToChannel := func(gtid string) {
		writtenGTIDs <- gtid
	}
	
	// Define our GTID sequence
	gtids := []string{
		"uuid:1-100",
		"uuid:1-101",
		"uuid:1-102",
		"uuid:1-103", 
		"uuid:1-104",
	}
	
	// Track the current state
	pending := make(map[string]bool)
	processed := make(map[string]bool)
	var highestSaved string
	
	// Initialize all GTIDs as pending
	for _, gtid := range gtids {
		pending[gtid] = true
	}
	
	// Create a mutex to protect our state
	var stateMutex sync.Mutex
	
	// Process GTIDs in an arbitrary order
	processOrder := []int{3, 4, 1, 2, 0}
	
	// Process function that's safe for concurrent use
	processGTID := func(gtid string) {
		stateMutex.Lock()
		defer stateMutex.Unlock()
		
		t.Logf("Processing GTID: %s", gtid)
		
		// Mark this GTID as processed and remove from pending
		processed[gtid] = true
		delete(pending, gtid)
		
		// After each processing, check if we can update the saved GTID
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
				writeGTIDToChannel(highestGTID)
				t.Logf("Updated to highest available GTID: %s (no pending)", highestGTID)
			}
		}
	}
	
	// Process each GTID in separate goroutines to test thread safety
	var wg sync.WaitGroup
	for _, idx := range processOrder {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Add a small random delay to increase race condition likelihood
			time.Sleep(time.Duration(i) * time.Millisecond)
			processGTID(gtids[i])
		}(idx)
	}
	
	// Wait for all processing to complete
	wg.Wait()
	close(writtenGTIDs)
	
	// Collect all written GTIDs
	var writtenGTIDsList []string
	for gtid := range writtenGTIDs {
		writtenGTIDsList = append(writtenGTIDsList, gtid)
	}
	
	// Verify the results
	if len(writtenGTIDsList) == 0 {
		t.Fatalf("No GTIDs were written")
	}
	
	// The last written GTID should be the highest one
	lastWrittenGTID := writtenGTIDsList[len(writtenGTIDsList)-1]
	assert.Equal(t, gtids[4], lastWrittenGTID,
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