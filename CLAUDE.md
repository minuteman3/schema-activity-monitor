# Schema Activity Monitor Dev Guide

## Commands
- Build: `go build -o schema-activity-monitor`
- Run all tests: `go test ./...`
- Run integration tests: `RUN_INTEGRATION_TESTS=true go test ./...`
- Run single test: `go test -run TestName` 
- Format code: `go fmt ./...`
- Vet code: `go vet ./...`

## Code Style
- **Naming**: camelCase variables, PascalCase exported items
- **Imports**: Standard lib first, third-party next, local imports last
- **Error handling**: Return errors, don't panic; log at appropriate levels
- **Testing**: Use testify for assertions, create mocks for interfaces
- **Comments**: Document all exported functions with comment blocks
- **Concurrency**: Use worker pools with context for cancellation
- **Interfaces**: Create interfaces for external dependencies for testability
- **Types**: Use explicit typing, avoid interface{} when possible

## Architecture
The project monitors MySQL schema changes by reading binlogs and optionally sends notifications to AWS SQS queues. It uses asynchronous processing with backpressure handling.