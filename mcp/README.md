# Tinybird DevTools MCP

> **Note:** This package is experimental. APIs may change between versions.

`@tinybirdco/devtools-mcp` is a Model Context Protocol (MCP) server that provides Tinybird development tools and utilities for coding agents like Claude, Cursor, and Codex.

## Getting Started

### Requirements

- [Node.js](https://nodejs.org/) v20.0.0 or newer
- A Tinybird API token

Add the following config to your MCP client:

```json
{
  "mcpServers": {
    "tinybird": {
      "command": "npx",
      "args": ["-y", "@tinybirdco/devtools-mcp@latest"],
      "env": {
        "TINYBIRD_TOKEN": "p.your-token-here"
      }
    }
  }
}
```

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `TINYBIRD_TOKEN` | Tinybird API token | Required |
| `TINYBIRD_URL` | Tinybird API base URL | `https://api.tinybird.co` |

> [!TIP]
> For US region, set `TINYBIRD_URL` to `https://api.us-east.tinybird.co`.

### MCP Client Configuration

<details>
<summary>Claude Code</summary>

Use the Claude Code CLI to add the Tinybird DevTools MCP server:

```bash
claude mcp add tinybird -e TINYBIRD_TOKEN=p.your-token-here -- npx -y @tinybirdco/devtools-mcp@latest
```

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
      "env": {
        "TINYBIRD_TOKEN": "p.your-token-here"
      }
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

```bash
codex mcp add tinybird -e TINYBIRD_TOKEN=p.your-token-here -- npx -y @tinybirdco/devtools-mcp@latest
```

</details>

<details>
<summary>VS Code / Copilot</summary>

```bash
code --add-mcp '{"name":"tinybird","command":"npx","args":["-y","@tinybirdco/devtools-mcp@latest"],"env":{"TINYBIRD_TOKEN":"p.your-token-here"}}'
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

```
Tinybird, build and deploy my resources
```

```
Tinybird, log me in to Tinybird
```

```
Tinybird, show me my workspace info
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

List all resources in your Tinybird workspace.

**When to use:**
- Explore workspace structure
- Get an overview of datasources, pipes, and connections
- Find resource names before fetching full definitions

**Input:**
- `type` (optional) - Filter by resource type: `'datasource'`, `'pipe'`, or `'connection'`
- `environment` (optional) - Environment to query: `'cloud'` (default), `'local'`, or a branch name

**Output:**
- JSON array with each resource's id, name, type, and description

**Example prompts:**
- "Tinybird, show me all resources in my workspace"
- "Tinybird, list all my datasources"
- "Tinybird, what pipes do I have?"
- "Tinybird, list resources in my feature_branch"
- "Tinybird, show me datasources in local"

</details>

<details>
<summary><code>get_resource</code></summary>

Get the full datafile content of a specific resource.

**When to use:**
- View the complete definition of a datasource, pipe, or connection
- Understand resource schemas and configurations
- Review SQL transformations in pipes

**Input:**
- `name` (required) - The name of the resource to fetch
- `type` (required) - The resource type: `'datasource'`, `'pipe'`, or `'connection'`
- `environment` (optional) - Environment to query: `'cloud'` (default), `'local'`, or a branch name

**Output:**
- Raw datafile content as text

**Example prompts:**
- "Tinybird, show me the definition of my events datasource"
- "Tinybird, get the full content of the analytics_api pipe"
- "Tinybird, what's the schema of my users datasource?"
- "Tinybird, get the events datasource from my feature_branch"

</details>

<details>
<summary><code>list_branches</code></summary>

List all branches (environments) in your Tinybird workspace.

**When to use:**
- See available branches before querying resources from a specific branch
- Check which development branches exist
- Find branch names to use with the `environment` parameter

**Input:**
- None

**Output:**
- JSON array with each branch's id, name, and created_at timestamp

**Example prompts:**
- "Tinybird, list all my branches"
- "Tinybird, what branches do I have?"
- "Tinybird, show me available environments"

</details>

<details>
<summary><code>list_kafka_topics</code></summary>

List available Kafka topics for a connection.

**When to use:**
- Discover available topics before creating streaming datasources
- Verify Kafka connection is working

**Input:**
- `connection_id` (required) - The ID of the Kafka connection (get from `list_resources` with type `connection`)

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

<details>
<summary><code>login</code></summary>

Authenticate with Tinybird via browser OAuth flow.

**When to use:**
- Set up authentication for a new project
- Re-authenticate after token expiration
- Switch to a different Tinybird workspace

**Input:**
- `cwd` (optional) - Working directory containing tinybird.json (defaults to current directory)
- `api_host` (optional) - API host/region override (e.g., `https://api.us-east.tinybird.co`)

**Output:**
- JSON with `success`, `workspaceName`, `userEmail`, `baseUrl`, or error details

**Example prompts:**
- "Tinybird, log me in"
- "Tinybird, authenticate with the US region"
- "Tinybird, set up authentication for this project"

</details>

<details>
<summary><code>build</code></summary>

Build and deploy Tinybird resources from TypeScript definitions to a development branch.

**When to use:**
- Deploy datasources and pipes defined in your TypeScript code
- Preview what would be deployed with dry run mode
- Deploy to a local Tinybird instance for testing

**Input:**
- `cwd` (optional) - Working directory containing tinybird.json (defaults to current directory)
- `dry_run` (optional) - If true, generate resources without pushing to API
- `dev_mode` (optional) - Override devMode: `'branch'` (Tinybird cloud) or `'local'` (localhost)

**Output:**
- JSON with build results including:
  - `success` - Whether the build succeeded
  - `durationMs` - Build duration in milliseconds
  - `resources` - Count of datasources, pipes, and connections generated
  - `branch` - Git and Tinybird branch info, dashboard URL
  - `deploy` - Deployment results with counts and changes

**Example prompts:**
- "Tinybird, build and deploy my resources"
- "Tinybird, do a dry run build to see what would be deployed"
- "Tinybird, build to local"
- "Tinybird, deploy my datasources and pipes"

</details>

<details>
<summary><code>get_info</code></summary>

Get information about the current Tinybird project and workspace.

**When to use:**
- View workspace details (name, ID, user email)
- Check API configuration and endpoints
- List available branches
- Understand current project context

**Input:**
- None

**Output:**
- JSON with workspace info, API configuration, and available branches

**Example prompts:**
- "Tinybird, show me my workspace info"
- "Tinybird, what workspace am I connected to?"
- "Tinybird, get project information"

</details>

<details>
<summary><code>query_logs</code></summary>

Query Tinybird service logs for observability data. Returns unified logs from multiple service datasources.

**When to use:**
- Debug API calls, data ingestion, and query execution
- Investigate endpoint errors
- Monitor Kafka and sink operations
- Track job executions and LLM usage

**Input:**
- `start_time` (optional) - Start time (relative: `-1h`, `-30m`, `-1d`, `-7d` or ISO 8601). Default: `-1h`
- `end_time` (optional) - End time (relative or ISO 8601). Default: `now`
- `sources` (optional) - Array of sources to filter. Default: all sources
- `limit` (optional) - Maximum rows (1-1000). Default: `100`

**Available sources:**
- `pipe_stats_rt` - API call metrics
- `bi_stats_rt` - Query execution details
- `block_log` - Data ingestion blocks
- `datasources_ops_log` - Datasource operations
- `endpoint_errors` - Endpoint errors
- `kafka_ops_log` - Kafka operations
- `sinks_ops_log` - Sink operations
- `jobs_log` - Job executions
- `llm_usage` - LLM token usage

**Output:**
- JSON with `source`, `timestamp`, and `data` (JSON containing all columns) for each log entry

**Example prompts:**
- "Tinybird, show me recent logs"
- "Tinybird, what errors happened in the last 24 hours?"
- "Tinybird, show me endpoint errors from the last hour"
- "Tinybird, query logs for pipe_stats_rt and bi_stats_rt"

</details>

## Configuration

The server resolves configuration in the following priority order:

### 1. Environment Variables (Recommended)

Set `TINYBIRD_TOKEN` and optionally `TINYBIRD_URL`:

```json
{
  "env": {
    "TINYBIRD_TOKEN": "p.your-token-here",
    "TINYBIRD_URL": "https://api.us-east.tinybird.co"
  }
}
```

### 2. Config Files (Fallback)

If environment variables are not set, the server looks for config files:

**`tinybird.json`** (created by `npx @tinybirdco/sdk init`):

```json
{
  "token": "${TINYBIRD_TOKEN}",
  "baseUrl": "https://api.tinybird.co"
}
```

**`.tinyb`** (created by `tb login`):

```json
{
  "host": "https://api.tinybird.co",
  "token": "p.eyJ..."
}
```

Environment variables in `${VAR}` format are automatically resolved from your environment or `.env` files.

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
         "env": {
           "TINYBIRD_TOKEN": "p.your-token-here"
         }
       }
     }
   }
   ```

## License

MIT
