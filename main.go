package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
)

type SchemaEvent struct {
	Schema    string    `json:"schema"`
	Timestamp time.Time `json:"timestamp"`
}

type SQSClientInterface interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSWorker is responsible for processing schema events asynchronously
type SQSWorker struct {
	client      SQSClientInterface
	queueURL    string
	eventQueue  chan SchemaEvent
	workerCount int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex // Protects the stopped flag
	stopped     bool         // Indicates if the worker is stopping/stopped
}

// NewSQSWorker creates a new SQS worker pool
func NewSQSWorker(client SQSClientInterface, queueURL string, bufferSize int, workerCount int) *SQSWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &SQSWorker{
		client:      client,
		queueURL:    queueURL,
		eventQueue:  make(chan SchemaEvent, bufferSize),
		workerCount: workerCount,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start launches the worker pool
func (w *SQSWorker) Start() {
	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}
	log.Infof("Started %d SQS workers", w.workerCount)
}

// Stop gracefully shuts down the worker pool
func (w *SQSWorker) Stop() {
	log.Info("Stopping SQS workers, waiting for queued messages to be processed...")

	// Mark as stopping to prevent new events from being enqueued
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return // Already stopped
	}
	w.stopped = true
	w.mu.Unlock()

	// Signal workers to stop accepting new work
	w.cancel()

	// Close the channel to signal workers to exit after processing remaining events
	close(w.eventQueue)

	// Wait for all workers to finish
	w.wg.Wait()
	log.Info("All SQS workers stopped")
}

// EnqueueEvent adds a schema event to the processing queue
func (w *SQSWorker) EnqueueEvent(schema string, timestamp uint32) bool {
	// Check if worker is stopping/stopped before attempting to enqueue
	w.mu.RLock()
	if w.stopped {
		w.mu.RUnlock()
		log.Warnf("SQS worker is stopped, dropping event for schema %s", schema)
		return true
	}
	w.mu.RUnlock()

	eventTime := time.Unix(int64(timestamp), 0)
	event := SchemaEvent{
		Schema:    schema,
		Timestamp: eventTime,
	}

	// Try to enqueue the event, drop it if queue is full
	select {
	case w.eventQueue <- event:
		return false // No backpressure needed
	default:
		// Queue is getting full, signal for backpressure
		queueFillPercentage := float64(len(w.eventQueue)) / float64(cap(w.eventQueue))
		if queueFillPercentage > 0.9 {
			log.Warnf("SQS event queue is over 90%% full (%d/%d), applying backpressure",
					  len(w.eventQueue), cap(w.eventQueue))
			time.Sleep(100 * time.Millisecond) // Apply backpressure by sleeping

			// Try to add it with a short timeout to avoid deadlock
			select {
			case w.eventQueue <- event:
				return true // Indicate backpressure was applied
			case <-time.After(200 * time.Millisecond):
				log.Warnf("SQS event queue is still full after backpressure, dropping event for schema %s", schema)
				return true
			}
		}
		// Still try to add it non-blocking one more time
		select {
		case w.eventQueue <- event:
			return false
		default:
			log.Warnf("SQS event queue is full, dropping event for schema %s", schema)
			return true
		}
	}
}

// worker processes events from the queue
func (w *SQSWorker) worker(id int) {
	defer w.wg.Done()

	log.Debugf("SQS worker %d started", id)

	// Process until channel is closed
	for event := range w.eventQueue {
		// Always process events from the queue, even during shutdown
		w.sendEventToSQS(event)
	}

	log.Debugf("SQS worker %d stopped", id)
}

// sendEventToSQS sends a single event to SQS with retries
func (w *SQSWorker) sendEventToSQS(event SchemaEvent) {
	if w.client == nil || w.queueURL == "" {
		// Just log the event if no SQS is configured
		messageBody, _ := json.Marshal(event)
		log.Infof("would have sent to SQS: %s", string(messageBody))
		return
	}

	// Convert event to JSON
	messageBody, err := json.Marshal(event)
	if err != nil {
		log.Errorf("failed to marshal event for schema %s: %v", event.Schema, err)
		return
	}

	messageBodyStr := string(messageBody)
	maxRetries := 3
	retryDelay := 200 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
		}

		// Create context with timeout for the request
		ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)

		// Send message to SQS
		_, err = w.client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               &w.queueURL,
			MessageBody:            &messageBodyStr,
			MessageGroupId:         &event.Schema,
			MessageDeduplicationId: &event.Schema,
		})

		cancel() // Clean up the context

		if err == nil {
			// Success
			log.Debugf("sent schema event to SQS: %s at %s", event.Schema, event.Timestamp.Format(time.RFC3339))
			return
		}

		log.Warnf("failed to send message to SQS for schema %s (attempt %d/%d): %v",
			event.Schema, attempt+1, maxRetries, err)
	}

	log.Errorf("failed to send message to SQS for schema %s after %d attempts",
		event.Schema, maxRetries)
}

func main() {
	// Define command line flags
	username := flag.String("user", "root", "MySQL username")
	password := flag.String("password", "", "MySQL password")
	host := flag.String("host", "localhost", "MySQL host")
	port := flag.Int("port", 3306, "MySQL port")
	serverID := flag.Uint("server-id", 42897, "Unique server ID for binlog sync")
	gtidSet := flag.String("gtid-set", "", "GTID set to start syncing from")
	queueURL := flag.String("queue-url", "", "SQS FIFO queue URL (optional)")
	workerCount := flag.Int("workers", 5, "Number of SQS worker goroutines")
	queueSize := flag.Int("queue-size", 10000, "Size of the internal event queue")
	verbose := flag.Bool("verbose", false, "Enable debug logging")
	flag.Parse()

	// Configure logging
	if *verbose {
		log.SetLevel(log.LevelDebug)
	} else {
		log.SetLevel(log.LevelInfo)
	}

	// Initialize AWS SQS client if queue URL is provided
	var sqsClient *sqs.Client
	if *queueURL != "" {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			log.Errorf("failed to load AWS config: %v", err)
			os.Exit(1)
		}
		sqsClient = sqs.NewFromConfig(cfg)
	}

	// Create and start the SQS worker pool
	sqsWorker := NewSQSWorker(sqsClient, *queueURL, *queueSize, *workerCount)
	sqsWorker.Start()
	defer sqsWorker.Stop()

	// Create a new binlog syncer
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(*serverID),
		Host:     *host,
		Port:     uint16(*port),
		User:     *username,
		Password: *password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	// Start syncing from the specified position
	var streamer *replication.BinlogStreamer
	var err error
	if *gtidSet == "" {
		// Connect to MySQL to get current position
		conn, err := client.Connect(fmt.Sprintf("%s:%d", *host, *port), *username, *password, "")
		if err != nil {
			log.Errorf("Error connecting to MySQL: %v", err)
			os.Exit(1)
		}
		defer conn.Close()

		// Get current GTID set
		var gtidStr string
		result, err := conn.Execute("SELECT @@GLOBAL.GTID_EXECUTED")
		if err != nil {
			log.Errorf("Error getting current GTID position: %v", err)
			os.Exit(1)
		}
		gtidStr = string(result.Values[0][0].AsString())

		currentGTIDSet, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidStr)
		if err != nil {
			log.Errorf("Error parsing GTID set: %v", err)
			os.Exit(1)
		}
		streamer, err = syncer.StartSyncGTID(currentGTIDSet)
	} else {
		// Parse provided GTID set and start from there
		mysqlGTIDSet, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, *gtidSet)
		if err != nil {
			log.Errorf("Error parsing GTID set: %v", err)
			os.Exit(1)
		}
		streamer, err = syncer.StartSyncGTID(mysqlGTIDSet)
	}

	if err != nil {
		log.Errorf("Error starting sync: %v", err)
		os.Exit(1)
	}

	// Track processing statistics
	statsInterval := 60 * time.Second
	lastStatTime := time.Now()
	eventCount := 0
	lastEventCount := 0

	// Read events from the binlog
	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			log.Errorf("failed to get event: %v", err)
			break
		}

		// Process different event types
		switch e := ev.Event.(type) {
		case *replication.RowsEvent:
			eventCount++

			// Instead of blocking on SQS, simply enqueue the event for async processing
			schema := string(e.Table.Schema)
			if !sqsWorker.EnqueueEvent(schema, ev.Header.Timestamp) {
				// Log processing rate periodically
				now := time.Now()
				if now.Sub(lastStatTime) >= statsInterval {
					rate := float64(eventCount-lastEventCount) / now.Sub(lastStatTime).Seconds()
					log.Infof("Processing rate: %.2f events/second (total: %d)", rate, eventCount)
					lastStatTime = now
					lastEventCount = eventCount
				}
			}
		default:
			// Ignore all other events
			continue
		}
	}
}

// The handleSchemaEvent is kept for backward compatibility with tests
func handleSchemaEvent(sqsClient SQSClientInterface, queueURL string, schema string, timestamp uint32) {
	eventTime := time.Unix(int64(timestamp), 0)
	event := SchemaEvent{
		Schema:    schema,
		Timestamp: eventTime,
	}

	// Convert event to JSON
	messageBody, err := json.Marshal(event)
	if err != nil {
		log.Errorf("failed to marshal event for schema %s: %v", schema, err)
		return
	}

	messageBodyStr := string(messageBody)
	// If no SQS queue URL is provided, just log
	if queueURL == "" {
		log.Infof("would have sent to SQS: %s", messageBodyStr)
		return
	}

	// Send message to SQS
	_, err = sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               &queueURL,
		MessageBody:            &messageBodyStr,
		MessageGroupId:         &schema,
		MessageDeduplicationId: &schema,
	})
	if err != nil {
		log.Errorf("failed to send message to SQS for schema %s: %v", schema, err)
		return
	}

	log.Debugf("sent schema event to SQS: %s at %s", schema, eventTime.Format(time.RFC3339))
}
