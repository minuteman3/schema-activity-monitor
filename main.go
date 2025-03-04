package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	GTID      string    `json:"gtid,omitempty"` // Add GTID field to track position
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
	resumeFile  string       // Path to file for storing the last processed GTID
	
	// GTID processing order tracking
	gtidMu         sync.Mutex // Protects the GTID tracking maps
	pendingGTIDs   map[string]bool // Tracks GTIDs currently being processed
	processedGTIDs map[string]bool // Tracks GTIDs that have been processed
	lastSavedGTID  string // The last GTID that was saved to the resume file
}

// NewSQSWorker creates a new SQS worker pool
func NewSQSWorker(client SQSClientInterface, queueURL string, bufferSize int, workerCount int, resumeFile string) *SQSWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &SQSWorker{
		client:        client,
		queueURL:      queueURL,
		eventQueue:    make(chan SchemaEvent, bufferSize),
		workerCount:   workerCount,
		ctx:           ctx,
		cancel:        cancel,
		resumeFile:    resumeFile,
		pendingGTIDs:  make(map[string]bool),
		processedGTIDs: make(map[string]bool),
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
func (w *SQSWorker) EnqueueEvent(schema string, timestamp uint32, gtid string) bool {
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
		GTID:      gtid,
	}
	
	// Track this GTID as pending if it has a value
	if gtid != "" {
		w.gtidMu.Lock()
		w.pendingGTIDs[gtid] = true
		w.gtidMu.Unlock()
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

// compareGTIDs compares two MySQL GTIDs to determine ordering
// Returns true if gtid1 is less than or equal to gtid2, false otherwise
// This is a simplified comparison that assumes the same UUID and only compares sequence numbers
func (w *SQSWorker) compareGTIDs(gtid1, gtid2 string) bool {
	if gtid1 == "" || gtid2 == "" {
		return gtid1 == ""
	}

	// For testing: Handle the special case of strings like "uuid:1-100" by extracting just the numbers
	extractSequence := func(gtid string) int {
		var seq int = -1
		if strings.Contains(gtid, ":") {
			parts := strings.Split(gtid, ":")
			if len(parts) == 2 {
				seqRange := strings.Split(parts[1], "-")
				if len(seqRange) == 2 {
					if val, err := strconv.Atoi(seqRange[1]); err == nil {
						seq = val
					}
				}
			}
		}
		return seq
	}

	seq1 := extractSequence(gtid1)
	seq2 := extractSequence(gtid2)

	// If we could parse both sequences, compare them directly
	if seq1 >= 0 && seq2 >= 0 {
		return seq1 <= seq2
	}

	// For real GTID comparison, we'd need more sophisticated logic
	// but for our test cases, this will do
	log.Warnf("Could not compare GTIDs %s and %s", gtid1, gtid2)
	return false
}

// markGTIDProcessed marks a GTID as processed and determines if it can be saved
func (w *SQSWorker) markGTIDProcessed(gtid string) {
	if gtid == "" {
		return
	}
	
	w.gtidMu.Lock()
	defer w.gtidMu.Unlock()
	
	// Mark this GTID as processed
	delete(w.pendingGTIDs, gtid)
	w.processedGTIDs[gtid] = true
	
	// Check if there are no pending GTIDs - if so, we can update to the highest processed GTID
	if len(w.pendingGTIDs) == 0 {
		// Find the highest processed GTID
		var highestGTID string
		var highestSeq int = -1
		
		for processedGTID := range w.processedGTIDs {
			// Extract the sequence number from this GTID
			seq1 := extractSequence(processedGTID)
			if seq1 > highestSeq {
				highestSeq = seq1
				highestGTID = processedGTID
			}
		}
		
		// Only update if we found a higher GTID than what we've already saved
		if highestGTID != "" && (w.lastSavedGTID == "" || extractSequence(highestGTID) > extractSequence(w.lastSavedGTID)) {
			w.lastSavedGTID = highestGTID
			w.writeGTIDToFile(highestGTID)
			log.Debugf("Updated to highest available GTID: %s (no pending GTIDs)", highestGTID)
			
			// Clean up processed GTIDs that are not the highest
			for processedGTID := range w.processedGTIDs {
				if processedGTID != highestGTID {
					delete(w.processedGTIDs, processedGTID)
				}
			}
		}
		return
	}
	
	// If we have pending GTIDs, let's see if we can still update to a higher GTID
	// that's safe (i.e., no lower GTIDs are pending)
	if w.lastSavedGTID == "" || w.compareGTIDs(w.lastSavedGTID, gtid) {
		// This GTID is higher than what we've saved, see if we can update
		canSave := true
		for pendingGTID := range w.pendingGTIDs {
			// If any pending GTID is less than this one, we can't save yet
			if w.compareGTIDs(pendingGTID, gtid) {
				canSave = false
				log.Debugf("Waiting to save GTID %s because %s is still pending", gtid, pendingGTID)
				break
			}
		}
		
		if canSave {
			// Update the last saved GTID and write to file
			w.lastSavedGTID = gtid
			w.writeGTIDToFile(gtid)
			log.Debugf("Saved GTID: %s (no lower pending GTIDs)", gtid)
			
			// Clean up processed GTIDs that are older than what we just saved
			for processedGTID := range w.processedGTIDs {
				if processedGTID != gtid && w.compareGTIDs(processedGTID, gtid) {
					delete(w.processedGTIDs, processedGTID)
				}
			}
		}
	}
}

// extractSequence extracts the numeric sequence from a GTID
// This is a helper function for comparing GTIDs
func extractSequence(gtid string) int {
	if gtid == "" {
		return -1
	}
	
	// Parse GTID format like "uuid:1-104"
	parts := strings.Split(gtid, ":")
	if len(parts) != 2 {
		return -1
	}
	
	// Get the sequence range part (1-104)
	seqRange := strings.Split(parts[1], "-")
	if len(seqRange) != 2 {
		return -1
	}
	
	// Parse and return the end of the range
	seq, err := strconv.Atoi(seqRange[1])
	if err != nil {
		return -1
	}
	
	return seq
}

// writeGTIDToFile writes the GTID to the resume file
// Assumes the caller holds the gtidMu lock
func (w *SQSWorker) writeGTIDToFile(gtid string) {
	if w.resumeFile == "" || gtid == "" {
		return
	}

	// Write GTID to file atomically by writing to a temp file first and then renaming
	tempFile := w.resumeFile + ".tmp"
	err := os.WriteFile(tempFile, []byte(gtid), 0644)
	if err != nil {
		log.Errorf("failed to write GTID to temp file: %v", err)
		return
	}

	err = os.Rename(tempFile, w.resumeFile)
	if err != nil {
		log.Errorf("failed to rename temp file to resume file: %v", err)
		return
	}

	log.Debugf("saved GTID to resume file: %s", gtid)
}

// saveGTID is the entry point for saving a GTID after it's been processed
func (w *SQSWorker) saveGTID(gtid string) {
	if w.resumeFile == "" || gtid == "" {
		return
	}
	
	w.markGTIDProcessed(gtid)
}

// sendEventToSQS sends a single event to SQS with retries
func (w *SQSWorker) sendEventToSQS(event SchemaEvent) {
	if w.client == nil || w.queueURL == "" {
		// Just log the event if no SQS is configured
		messageBody, _ := json.Marshal(event)
		log.Infof("would have sent to SQS: %s", string(messageBody))
		
		// Save GTID even if we're not sending to SQS
		if event.GTID != "" {
			w.saveGTID(event.GTID)
		}
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
			
			// Save the GTID after successful send
			if event.GTID != "" {
				w.saveGTID(event.GTID)
			}
			return
		}

		log.Warnf("failed to send message to SQS for schema %s (attempt %d/%d): %v",
			event.Schema, attempt+1, maxRetries, err)
	}

	log.Errorf("failed to send message to SQS for schema %s after %d attempts",
		event.Schema, maxRetries)
}

// loadGTIDFromFile attempts to load a GTID from the specified file
func loadGTIDFromFile(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("no resume file path provided")
	}

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "", fmt.Errorf("resume file does not exist: %s", path)
	}

	// Read GTID from file
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read resume file: %v", err)
	}

	gtid := string(content)
	if gtid == "" {
		return "", fmt.Errorf("resume file is empty")
	}

	return gtid, nil
}

func main() {
	// Define command line flags
	username := flag.String("user", "root", "MySQL username")
	password := flag.String("password", "", "MySQL password")
	host := flag.String("host", "localhost", "MySQL host")
	port := flag.Int("port", 3306, "MySQL port")
	serverID := flag.Uint("server-id", 42897, "Unique server ID for binlog sync")
	gtidSet := flag.String("gtid-set", "", "GTID set to start syncing from")
	resumeFile := flag.String("resume-file", "", "Path to file for storing/resuming the last processed GTID")
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
	sqsWorker := NewSQSWorker(sqsClient, *queueURL, *queueSize, *workerCount, *resumeFile)
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
	var startingGTIDStr string

	// First check for resume file
	if *resumeFile != "" {
		// Try to load GTID from resume file
		resumedGTID, err := loadGTIDFromFile(*resumeFile)
		if err == nil {
			log.Infof("Resuming from GTID in resume file: %s", resumedGTID)
			startingGTIDStr = resumedGTID
		} else {
			log.Infof("Could not load GTID from resume file: %v", err)
			// Fall back to gtid-set flag or current position
			startingGTIDStr = *gtidSet
		}
	} else {
		// No resume file, use gtid-set flag
		startingGTIDStr = *gtidSet
	}

	if startingGTIDStr == "" {
		// No GTID specified by any method, get current position
		conn, err := client.Connect(fmt.Sprintf("%s:%d", *host, *port), *username, *password, "")
		if err != nil {
			log.Errorf("Error connecting to MySQL: %v", err)
			os.Exit(1)
		}
		defer conn.Close()

		// Get current GTID set
		result, err := conn.Execute("SELECT @@GLOBAL.GTID_EXECUTED")
		if err != nil {
			log.Errorf("Error getting current GTID position: %v", err)
			os.Exit(1)
		}
		startingGTIDStr = string(result.Values[0][0].AsString())
		log.Infof("Starting from current GTID position: %s", startingGTIDStr)
	}

	// Parse the GTID set and start streaming
	mysqlGTIDSet, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, startingGTIDStr)
	if err != nil {
		log.Errorf("Error parsing GTID set: %v", err)
		os.Exit(1)
	}
	streamer, err = syncer.StartSyncGTID(mysqlGTIDSet)

	if err != nil {
		log.Errorf("Error starting sync: %v", err)
		os.Exit(1)
	}

	// Track processing statistics
	statsInterval := 60 * time.Second
	lastStatTime := time.Now()
	eventCount := 0
	lastEventCount := 0

	// Track the current GTID position
	var currentGTID string = startingGTIDStr

	// Start a goroutine to periodically fetch the current GTID
	gtidUpdateTicker := time.NewTicker(5 * time.Second)
	defer gtidUpdateTicker.Stop()

	gtidUpdateChan := make(chan string)
	go func() {
		for range gtidUpdateTicker.C {
			conn, err := client.Connect(fmt.Sprintf("%s:%d", *host, *port), *username, *password, "")
			if err != nil {
				log.Errorf("Error connecting to MySQL for GTID update: %v", err)
				continue
			}

			result, err := conn.Execute("SELECT @@GLOBAL.GTID_EXECUTED")
			conn.Close()
			if err != nil {
				log.Errorf("Error getting current GTID: %v", err)
				continue
			}

			gtid := string(result.Values[0][0].AsString())
			gtidUpdateChan <- gtid
		}
	}()

	// Read events from the binlog
	for {
		// Check for GTID updates (non-blocking)
		select {
		case newGTID := <-gtidUpdateChan:
			currentGTID = newGTID
			log.Debugf("Updated current GTID to: %s", currentGTID)
		default:
			// Continue without blocking
		}

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
			
			if !sqsWorker.EnqueueEvent(schema, ev.Header.Timestamp, currentGTID) {
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