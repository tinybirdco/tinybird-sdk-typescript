# Tinybird DevTools MCP

`@tinybirdco/devtools-mcp` is a Model Context Protocol (MCP) server that provides Tinybird development tools and utilities for coding agents like Claude, Cursor, and Codex.

## Getting Started

### Requirements

- [Node.js](https://nodejs.org/) v20.0.0 or newer
- A Tinybird config file: either `tinybird.json` (from `npx @tinybirdco/sdk init`) or `.tinyb` (from `tb login`)

Add the following config to your MCP client:

```json
{
  "mcpServers": {
    "tinybird": {
      "command": "npx",
      "args": ["-y", "@tinybirdco/devtools-mcp@latest"],
      "cwd": "/path/to/your/project"
    }
  }
}
```

> [!NOTE]
> The `cwd` must point to your project root where your Tinybird config file is located. The server looks for `tinybird.json` (created by `npx @tinybirdco/sdk init`) or `.tinyb` (created by `tb login`). These files contain your Tinybird API token and base URL for authentication.

### MCP Client Configuration

<details>
<summary>Claude Code</summary>

Use the Claude Code CLI to add the Tinybird DevTools MCP server:

```bash
claude mcp add tinybird -- npx -y @tinybirdco/devtools-mcp@latest
```

Alternatively, manually configure Claude by editing your MCP settings file and adding the configuration shown above.

</details>

<details>
<summary>Claude Desktop</summary>

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "tinybird": {
      "command": "npx",
      "args": ["-y", "@tinybirdco/devtools-mcp@latest"],
      "cwd": "/path/to/your/project"
    }
  }
}
```

</details>

<details>
<summary>Cursor</summary>

Go to `Cursor Settings` → `MCP` → `New MCP Server`. Use the config provided above.

</details>

<details>
<summary>Codex</summary>

**Using Codex CLI:**

```bash
codex mcp add tinybird -- npx -y @tinybirdco/devtools-mcp@latest
```

</details>

<details>
<summary>VS Code / Copilot</summary>

**Using VS Code CLI:**

```bash
code --add-mcp '{"name":"tinybird","command":"npx","args":["-y","@tinybirdco/devtools-mcp@latest"]}'
```

</details>

## Quick Start

Once configured, your coding agent can interact with your Tinybird workspace:

```
Tinybird, show me all resources in my workspace
```

```
Tinybird, run a SQL query to count rows in my events table
```

```
Tinybird, list all my Kafka connections
```

```
Tinybird, preview data from my Kafka topic
```

## MCP Tools

<details>
<summary><code>execute_query</code></summary>

Execute SQL queries against your Tinybird workspace.

**When to use:**
- Query datasources and explore data
- Test SQL queries before creating pipes
- Debug data issues
- Verify data ingestion

**Input:**
- `query` (required) - The SQL query to execute

**Output:**
- Query results in JSON format

**Example prompts:**
- "Tinybird, count the rows in my events table"
- "Tinybird, show me the schema of the users datasource"
- "Tinybird, what's the average response time from my api_logs?"

</details>

<details>
<summary><code>list_resources</code></summary>

List all resources in your Tinybird workspace with their full datafile definitions.

**When to use:**
- Explore workspace structure
- Understand existing datasources and pipes
- Review resource definitions
- Audit workspace configuration

**Input:**
- `type` (optional) - Filter by resource type: `'datasource'`, `'pipe'`, or `'connection'`

**Output:**
- JSON array with each resource's name, type, and full datafile definition

**Example prompts:**
- "Tinybird, show me all resources in my workspace"
- "Tinybird, list all my datasources"
- "Tinybird, what pipes do I have?"

</details>

<details>
<summary><code>list_kafka_topics</code></summary>

List available Kafka topics for a connection.

**When to use:**
- Discover available topics before creating streaming datasources
- Verify Kafka connection is working

**Input:**
- `connection_id` (required) - The ID of the Kafka connection (get from `list_connections`)

**Output:**
- JSON array of available Kafka topics

**Example prompts:**
- "Tinybird, what Kafka topics are available on my connection?"

</details>

<details>
<summary><code>preview_kafka_topic</code></summary>

Preview data from a Kafka topic to understand its schema and content.

**When to use:**
- Explore message structure before creating datasources
- Verify data is flowing through Kafka
- Debug ingestion issues

**Input:**
- `connection_id` (required) - The ID of the Kafka connection
- `topic` (required) - The Kafka topic name to preview
- `group_id` (optional) - Kafka consumer group ID

**Output:**
- JSON with topic schema, sample messages, and metadata

**Example prompts:**
- "Tinybird, preview data from the events topic"
- "Tinybird, show me the schema of my Kafka messages"

</details>

## Configuration

The server supports two configuration file formats:

### Option 1: `tinybird.json` (TypeScript SDK)

Created by `npx @tinybirdco/sdk init`:

```json
{
  "token": "${TINYBIRD_TOKEN}",
  "baseUrl": "https://api.tinybird.co"
}
```

Environment variables in `${VAR}` format are automatically resolved from your environment or `.env` files.

| Field | Description | Default |
|-------|-------------|---------|
| `token` | Tinybird API token (supports env var interpolation) | Required |
| `baseUrl` | Tinybird API base URL | `https://api.tinybird.co` |

### Option 2: `.tinyb` (Tinybird CLI)

Created by `tb login`:

```json
{
  "host": "https://api.tinybird.co",
  "token": "p.eyJ..."
}
```

The server will search for `tinybird.json` first, then `.tinyb` if not found.

## Local Development

To run the MCP server locally for development:

1. Clone the repository
2. Install dependencies:
   ```bash
   pnpm install
   pnpm build
   ```
3. Configure your MCP client to use the local version:
   ```json
   {
     "mcpServers": {
       "tinybird": {
         "command": "node",
         "args": ["/absolute/path/to/mcp/dist/index.js"],
         "cwd": "/path/to/project/with/tinybird.json"
       }
     }
   }
   ```

## License

MIT
