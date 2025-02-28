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

	// Create a channel to signal that streamer is ready
	streamerReady := make(chan struct{})
	// Create a channel to signal that data insertion is complete
	insertDone := make(chan struct{})

	// Create a separate connection for the data insertion goroutine
	insertConn, err := client.Connect(fmt.Sprintf("%s:%d", host, port), username, password, "")
	if err != nil {
		t.Fatalf("Failed to create second MySQL connection: %v", err)
	}
	defer insertConn.Close()

	// Make a data change in another goroutine
	go func() {
		defer close(insertDone)
		// Wait for signal that streamer is ready
		<-streamerReady

		_, err := insertConn.Execute(fmt.Sprintf("INSERT INTO %s.test_table (name) VALUES ('test1')", testDB))
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
		}
	}()

	// Signal that streamer is ready to receive events
	close(streamerReady)

	// Read events with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	foundRowsEvent := false
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			t.Logf("Stopping event processing: %v", err)
			break // Timeout or error
		}

		// Look for our RowsEvent
		if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			t.Logf("Found rows event for schema: %s, table: %s", string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
			if string(rowsEvent.Table.Schema) == testDB {
				foundRowsEvent = true
				assert.Equal(t, "test_table", string(rowsEvent.Table.Table))
				break
			}
		}
	}

	// Wait for the insert operation to complete before cleaning up
	<-insertDone

	assert.True(t, foundRowsEvent, "Should have captured a rows event for our test database")
}

// New test for the async SQS worker with binlog integration
func TestIntegrationAsyncSQSWorker(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run")
	}

	// Create a mock SQS client to track calls
	mockClient := new(MockSQSClient)
	queueURL := "https://example.com/test-queue.fifo"

	// Track received schemas for verification
	var receivedSchemas []string
	var schemasMutex sync.Mutex

	// Setup the mock to capture schemas
	mockClient.On("SendMessage", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		input := args.Get(1).(*sqs.SendMessageInput)
		var event SchemaEvent
		if err := json.Unmarshal([]byte(*input.MessageBody), &event); err == nil {
			schemasMutex.Lock()
			receivedSchemas = append(receivedSchemas, event.Schema)
			schemasMutex.Unlock()
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
	worker := NewSQSWorker(mockClient, queueURL, 5, 2)
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

	// Create a goroutine to process binlog events
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	processingDone := make(chan struct{})
	go func() {
		defer close(processingDone)

		for {
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				t.Logf("Stopping event processing: %v", err)
				return
			}

			// Process row events
			if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
				schema := string(rowsEvent.Table.Schema)
				worker.EnqueueEvent(schema, ev.Header.Timestamp)
			}
		}
	}()

	// Execute multiple operations to generate binlog events
	for i := 1; i <= 10; i++ {
		_, err := conn.Execute(fmt.Sprintf("INSERT INTO %s.test_table (name) VALUES ('test%d')", testDB, i))
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Space out the operations
	}

	// Wait a bit for processing to complete
	time.Sleep(500 * time.Millisecond)

	// Stop the context to stop the processing goroutine
	cancel()
	<-processingDone

	// Wait for worker to process queued events
	time.Sleep(500 * time.Millisecond)

	// Verify that events were processed
	schemasMutex.Lock()
	defer schemasMutex.Unlock()

	assert.Greater(t, len(receivedSchemas), 0, "Should have processed some schema events")
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
