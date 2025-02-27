package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "os"
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

func main() {
    // Define command line flags
    username := flag.String("user", "root", "MySQL username")
    password := flag.String("password", "", "MySQL password")
    host := flag.String("host", "localhost", "MySQL host")
    port := flag.Int("port", 3306, "MySQL port")
    serverID := flag.Uint("server-id", 42897, "Unique server ID for binlog sync")
    gtidSet := flag.String("gtid-set", "", "GTID set to start syncing from")
    queueURL := flag.String("queue-url", "", "SQS FIFO queue URL (optional)")
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
            log.Debugf("Data change in %s.%s", string(e.Table.Schema), string(e.Table.Table))
            handleSchemaEvent(sqsClient, *queueURL, string(e.Table.Schema), ev.Header.Timestamp)
        default:
            // Ignore all other events
            continue
        }
    }
}

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
