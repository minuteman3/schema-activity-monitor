package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestBinlogSync performs an integration test with a real MySQL database
// This test requires a running MySQL instance with binlog enabled
// Skip this test if the environment isn't set up for it
func TestIntegrationBinlogSync(t *testing.T) {
	// Check if integration tests should be run
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run")
	}

	// Test MySQL connection parameters - these could be environment variables
	host := getEnvOrDefault("TEST_MYSQL_HOST", "localhost")
	port := 3306
	username := getEnvOrDefault("TEST_MYSQL_USER", "root")
	password := getEnvOrDefault("TEST_MYSQL_PASSWORD", "")
	testDB := "binlog_test_db"
	
	// Test timeouts
	const timeoutSeconds = 30 // Longer timeout for integration test

	// Connect to MySQL
	conn, err := client.Connect(fmt.Sprintf("%s:%d", host, port), username, password, "")
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer conn.Close()

	// Create a test database and table
	setupTestDatabase(t, conn, testDB)
	defer cleanupTestDatabase(t, conn, testDB)

	// Create a binlog syncer
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Host:     host,
		Port:     uint16(port),
		User:     username,
		Password: password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	// Get current position
	result, err := conn.Execute("SELECT @@GLOBAL.GTID_EXECUTED")
	if err != nil {
		t.Fatalf("Error getting current GTID position: %v", err)
	}
	gtidStr := string(result.Values[0][0].AsString())

	currentGTIDSet, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidStr)
	if err != nil {
		t.Fatalf("Error parsing GTID set: %v", err)
	}

	// Start syncing
	streamer, err := syncer.StartSyncGTID(currentGTIDSet)
	if err != nil {
		t.Fatalf("Error starting sync: %v", err)
	}

	// Create channels for coordination
	streamerReady := make(chan struct{})
	insertDone := make(chan struct{})
	rowEventReceived := make(chan struct{})
	
	// Create a context for the test with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	// Create a separate connection for the data insertion
	insertConn, err := client.Connect(fmt.Sprintf("%s:%d", host, port), username, password, "")
	if err != nil {
		t.Fatalf("Failed to create second MySQL connection: %v", err)
	}
	defer insertConn.Close()

	// Run the event processor in a goroutine
	go func() {
		// Signal that we're ready to receive events
		close(streamerReady)
		
		for {
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				t.Logf("Stopping event processing: %v", err)
				return // Context cancelled or error
			}

			// Look for our RowsEvent
			if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
				schema := string(rowsEvent.Table.Schema)
				table := string(rowsEvent.Table.Table)
				t.Logf("Found rows event for schema: %s, table: %s", schema, table)
				
				if schema == testDB && table == "test_table" {
					// We found our event, signal success
					close(rowEventReceived)
					return
				}
			}
		}
	}()

	// Wait for the streamer to be ready before making data changes
	select {
	case <-streamerReady:
		// Streamer is ready
	case <-ctx.Done():
		t.Fatalf("Context cancelled while waiting for streamer to be ready: %v", ctx.Err())
	}

	// Make a data change in another goroutine
	go func() {
		defer close(insertDone)
		
		_, err := insertConn.Execute(fmt.Sprintf("INSERT INTO %s.test_table (name) VALUES ('test1')", testDB))
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
		}
	}()

	// Wait for the insert to complete with timeout
	select {
	case <-insertDone:
		// Insert completed successfully
	case <-ctx.Done():
		t.Fatalf("Context cancelled while waiting for insert to complete: %v", ctx.Err())
	}

	// Wait for the row event to be received with timeout
	select {
	case <-rowEventReceived:
		// Row event was received successfully
		assert.True(t, true, "Successfully captured a rows event for our test database")
	case <-ctx.Done():
		t.Fatalf("Timed out or context cancelled waiting for row event: %v", ctx.Err())
	}
}

// New test for the async SQS worker with binlog integration
func TestIntegrationAsyncSQSWorker(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run")
	}

	// Create a channel to track message processing
	messageReceived := make(chan struct{}, 20)
	
	// Create a mock SQS client to track calls
	mockClient := new(MockSQSClient)
	queueURL := "https://example.com/test-queue.fifo"

	// Track received schemas for verification
	var receivedSchemas []string
	var schemasMutex sync.Mutex

	// Setup the mock to capture schemas and signal when messages are received
	mockClient.On("SendMessage", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		input := args.Get(1).(*sqs.SendMessageInput)
		var event SchemaEvent
		if err := json.Unmarshal([]byte(*input.MessageBody), &event); err == nil {
			schemasMutex.Lock()
			receivedSchemas = append(receivedSchemas, event.Schema)
			schemasMutex.Unlock()
			
			// Signal that a message was received
			select {
			case messageReceived <- struct{}{}:
				// Signal sent
			default:
				// Channel buffer full, ignore
			}
		}
	}).Return(&sqs.SendMessageOutput{MessageId: new(string)}, nil)

	// Setup MySQL connection
	host := getEnvOrDefault("TEST_MYSQL_HOST", "localhost")
	port := 3306
	username := getEnvOrDefault("TEST_MYSQL_USER", "root")
	password := getEnvOrDefault("TEST_MYSQL_PASSWORD", "")
	testDB := "binlog_test_async"

	// Connect to MySQL
	conn, err := client.Connect(fmt.Sprintf("%s:%d", host, port), username, password, "")
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer conn.Close()

	// Create a test database and table
	setupTestDatabase(t, conn, testDB)
	defer cleanupTestDatabase(t, conn, testDB)

	// Create and start the SQS worker with a small queue
	worker := NewSQSWorker(mockClient, queueURL, 5, 2, "")
	worker.Start()
	defer worker.Stop()

	// Create binlog syncer
	cfg := replication.BinlogSyncerConfig{
		ServerID: 101,
		Host:     host,
		Port:     uint16(port),
		User:     username,
		Password: password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	// Get current GTID position
	result, err := conn.Execute("SELECT @@GLOBAL.GTID_EXECUTED")
	if err != nil {
		t.Fatalf("Error getting current GTID position: %v", err)
	}
	gtidStr := string(result.Values[0][0].AsString())

	currentGTIDSet, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidStr)
	if err != nil {
		t.Fatalf("Error parsing GTID set: %v", err)
	}

	// Start syncing
	streamer, err := syncer.StartSyncGTID(currentGTIDSet)
	if err != nil {
		t.Fatalf("Error starting sync: %v", err)
	}

	// Test configuration
	const numTestInserts = 10
	const timeoutSeconds = 30 // Longer timeout for integration test
	
	// Create a barrier to synchronize between event processor and data inserter
	processorReady := make(chan struct{})
	processingDone := make(chan struct{})
	
	// Create a goroutine to process binlog events
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	go func() {
		defer close(processingDone)
		
		// Signal that we're ready to process events
		close(processorReady)

		for {
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				t.Logf("Stopping event processing: %v", err)
				return
			}

			// Process row events
			if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
				schema := string(rowsEvent.Table.Schema)
				// Get the current GTID position if available
				gtidStr := "test-gtid-position" // Use a static position for testing
				worker.EnqueueEvent(schema, ev.Header.Timestamp, gtidStr)
			}
		}
	}()

	// Wait for processor to be ready before inserting data
	<-processorReady
	
	// Execute multiple operations to generate binlog events
	// Use a consistent insertion loop without arbitrary sleeps
	insertsDone := make(chan struct{})
	go func() {
		defer close(insertsDone)
		
		for i := 1; i <= numTestInserts; i++ {
			_, err := conn.Execute(fmt.Sprintf("INSERT INTO %s.test_table (name) VALUES ('test%d')", testDB, i))
			if err != nil {
				t.Errorf("Failed to insert test data: %v", err)
				return
			}
		}
	}()
	
	// Wait for inserts to complete with timeout
	select {
	case <-insertsDone:
		// Inserts completed successfully
	case <-time.After(time.Duration(timeoutSeconds/2) * time.Second):
		t.Fatalf("Timed out waiting for test inserts to complete")
	}

	// Now wait for at least one message to be received with timeout
	numReceived := 0
	timeoutTimer := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	defer timeoutTimer.Stop()
	
	// We want to receive at least one message to confirm things are working
MinMessagesLoop:
	for numReceived < 1 {
		select {
		case <-messageReceived:
			numReceived++
		case <-timeoutTimer.C:
			t.Logf("Timed out waiting for more messages, received %d so far", numReceived)
			break MinMessagesLoop
		}
	}
	
	// Stop the context to stop the processing goroutine
	cancel()
	
	// Wait for processing to complete with timeout
	select {
	case <-processingDone:
		// Processing completed
	case <-time.After(time.Duration(timeoutSeconds/4) * time.Second):
		t.Logf("Timed out waiting for processing to complete, continuing")
	}

	// Verify that events were processed - even if we didn't get all events,
	// we should have at least one, and all should be for the test database
	schemasMutex.Lock()
	numSchemas := len(receivedSchemas)
	schemasMutex.Unlock()

	assert.Greater(t, numSchemas, 0, "Should have processed at least one schema event")
	
	schemasMutex.Lock()
	defer schemasMutex.Unlock()
	
	for _, schema := range receivedSchemas {
		assert.Equal(t, testDB, schema, "Schema should match the test database")
	}
}

func setupTestDatabase(t *testing.T, conn *client.Conn, dbName string) {
	// Drop database if it exists
	_, err := conn.Execute(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to drop test database: %v", err)
	}

	// Create database
	_, err = conn.Execute(fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create table
	_, err = conn.Execute(fmt.Sprintf("CREATE TABLE %s.test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", dbName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
}

func cleanupTestDatabase(t *testing.T, conn *client.Conn, dbName string) {
	_, err := conn.Execute(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Logf("Warning: Failed to drop test database during cleanup: %v", err)
	}
}

func getEnvOrDefault(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}
