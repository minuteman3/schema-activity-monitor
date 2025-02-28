# Schema Activity Monitor

A tool to monitor MySQL database schema activity by tracking binlog events and optionally sending notifications to Amazon SQS.

## Overview

Schema Activity Monitor watches MySQL binary logs for data modification events and tracks which schemas (databases) are actively receiving changes. It uses an asynchronous worker pool to efficiently process events and can either log these events locally or send them to an Amazon SQS FIFO queue for further processing.

## Features

- Monitors MySQL binlog events in real-time
- Tracks schema-level activity
- Supports GTID-based replication positioning
- Asynchronous event processing with configurable worker pool
- Backpressure handling for high event volumes
- Optional integration with Amazon SQS FIFO queues
- Configurable logging levels

## Usage

Basic usage:

```bash
./schema-activity-monitor -user root -password secret -host localhost
```

With SQS integration:

```bash
./schema-activity-monitor -user root -password secret -host localhost -queue-url https://sqs.region.amazonaws.com/123456789012/MyQueue.fifo
```

Start from specific GTID position:

```bash
./schema-activity-monitor -gtid-set "f56c3314-1d5e-11ee-9277-0242ac110002:1-200"
```

With custom worker configuration:

```bash
./schema-activity-monitor -workers 10 -queue-size 20000 -queue-url https://sqs.region.amazonaws.com/123456789012/MyQueue.fifo
```

## Command Line Options

| Option      | Description                      | Default   |
| ----------- | -------------------------------- | --------- |
| -user       | MySQL username                   | root      |
| -password   | MySQL password                   | -         |
| -host       | MySQL host                       | localhost |
| -port       | MySQL port                       | 3306      |
| -server-id  | Unique server ID for binlog sync | 42897     |
| -gtid-set   | GTID set to start syncing from   | -         |
| -queue-url  | SQS FIFO queue URL               | -         |
| -workers    | Number of SQS worker goroutines  | 5         |
| -queue-size | Size of the internal event queue | 10000     |
| -verbose    | Enable debug logging             | false     |

## Performance Tuning

For high-throughput environments, consider adjusting the following parameters:

- Increase `-workers` to handle more parallel SQS operations (e.g., 10-20)
- Increase `-queue-size` to better handle event bursts (e.g., 20000-50000)

If you experience backpressure (indicated by "applying backpressure" log messages), gradually increase the worker count until the system stabilizes.

## Output Format

When using SQS, events are sent as JSON messages:

```json
{
  "schema": "database_name",
  "timestamp": "2023-07-10T15:04:05Z"
}
```

## AWS Configuration

When using the SQS integration, ensure:

- AWS credentials are properly configured
- The SQS queue is a FIFO queue
- The executing environment has appropriate IAM permissions
- The application runs in the same AWS region as your SQS queue to minimize latency

## Monitoring

The application logs processing rates periodically, reporting the number of events processed per second. Use this information to tune the worker and queue settings for your specific workload.
