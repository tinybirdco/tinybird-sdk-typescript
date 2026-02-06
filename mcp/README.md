# @tinybirdco/devtools

MCP (Model Context Protocol) server for Tinybird DevTools. Enables LLMs like Claude to interact with your Tinybird workspace during development.

## Installation

```bash
npm install @tinybirdco/devtools
```

## Requirements

- Node.js >= 20.0.0
- A `tinybird.json` config file in your project (created by `npx tinybird init`)
- Valid Tinybird API token

## Usage

### As an MCP Server (Streamable HTTP)

Run the server:

```bash
npx tinybird-devtools
```

By default, the server listens on port 8000. Use `--port` to change:

```bash
npx tinybird-devtools --port 3000
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "tinybird": {
      "command": "npx",
      "args": ["tinybird-devtools"],
      "cwd": "/path/to/your/project"
    }
  }
}
```

Make sure `cwd` points to a directory containing your `tinybird.json` config file.

## Available Tools

### execute_query

Execute SQL queries against your Tinybird workspace.

```
Query datasources, test SQL, or explore data.
```

**Parameters:**
- `query` (string): The SQL query to execute

### list_connections

List all connections (Kafka, etc.) in your Tinybird workspace.

```
View available data source connections for streaming ingestion.
```

### list_kafka_topics

List available Kafka topics for a connection.

**Parameters:**
- `connection_id` (string): The ID of the Kafka connection

### preview_kafka_topic

Preview data from a Kafka topic.

**Parameters:**
- `connection_id` (string): The ID of the Kafka connection
- `topic` (string): The Kafka topic name to preview
- `group_id` (string, optional): Kafka consumer group ID

## Configuration

The server reads configuration from `tinybird.json`:

```json
{
  "token": "${TINYBIRD_TOKEN}",
  "baseUrl": "https://api.tinybird.co"
}
```

Environment variables in `${VAR}` format are automatically resolved.

## License

MIT
