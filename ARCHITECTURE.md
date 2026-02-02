# SDK Architecture

This document describes the internal architecture of the Tinybird TypeScript SDK.

## Overview

The SDK is organized into layers that handle different responsibilities:

```
┌─────────────────────────────────────────────────────────────┐
│                      User Code                              │
│   defineDatasource, definePipe, createKafkaConnection, etc. │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Schema Layer                            │
│   TypeScript definitions with branded types                 │
│   src/schema/                                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Generator Layer                          │
│   Converts definitions to Tinybird datafile format content  │
│   src/generator/                                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       API Layer                             │
│   Sends generated content to Tinybird API                   │
│   src/api/                                                  │
└─────────────────────────────────────────────────────────────┘
```

## Layers

### Schema Layer (`src/schema/`)

Provides TypeScript-first schema definitions with branded types for type safety.

**Files:**
- `types.ts` - Column type validators (`t.string()`, `t.dateTime()`, etc.)
- `params.ts` - Parameter validators for pipes (`p.string()`, `p.int32()`, etc.)
- `engines.ts` - Table engine configurations (`engine.mergeTree()`, etc.)
- `datasource.ts` - `defineDatasource()` for table definitions
- `pipe.ts` - `definePipe()`, `defineEndpoint()`, `defineMaterializedView()`, `defineCopyPipe()`
- `connection.ts` - `createKafkaConnection()` for external connections
- `project.ts` - `defineProject()` to aggregate all resources

**Key Patterns:**
- Branded types using Symbols (`DATASOURCE_BRAND`, `PIPE_BRAND`, `CONNECTION_BRAND`)
- Type guards for runtime checks (`isDatasourceDefinition()`, `isPipeDefinition()`)
- Full TypeScript inference for row types, params, and outputs

### Generator Layer (`src/generator/`)

Converts in-memory definitions to Tinybird datafile format content strings.

**Files:**
- `datasource.ts` - Generates `.datasource` file content
- `pipe.ts` - Generates `.pipe` file content
- `connection.ts` - Generates `.connection` file content
- `loader.ts` - Loads and bundles TypeScript schema files using esbuild
- `index.ts` - Orchestrates building all resources
- `client.ts` - Generates typed client code

**Key Concepts:**
- **Generated content is stored in memory**, not written to disk
- Each generator returns `{ name: string, content: string }`
- Content follows Tinybird's datafile format (KEY VALUE pairs)

**Example generated `.datasource` content:**
```
DESCRIPTION >
    Event tracking data

SCHEMA >
    timestamp DateTime `json:$.timestamp`,
    event_type String `json:$.event_type`

ENGINE "MergeTree"
ENGINE_SORTING_KEY "timestamp"
```

### API Layer (`src/api/`)

Sends generated resources to Tinybird's API and fetches existing resources.

**Files:**
- `build.ts` - `buildToTinybird()` deploys resources to branches via `/v1/build`
- `deploy.ts` - `deployToMain()` deploys resources to main workspace via `/v1/deploy`
- `branches.ts` - Branch management API
- `workspaces.ts` - Workspace discovery
- `resources.ts` - Fetch existing datasources and pipes from workspace
- `local.ts` - Local Tinybird container integration

**Key Patterns:**
- Resources sent as multipart form data
- File extensions indicate resource type (`.datasource`, `.pipe`, `.connection`)
- Form field `data_project://` signals Tinybird to process the file
- `/v1/build` for branches, `/v1/deploy` for main workspace

### Codegen Layer (`src/codegen/`)

Converts Tinybird API responses to TypeScript SDK code.

**Files:**
- `index.ts` - Main code generators (`generateDatasourceCode()`, `generatePipeCode()`, etc.)
- `type-mapper.ts` - Maps ClickHouse types to `t.*` validators
- `utils.ts` - Helpers for case conversion, escaping, engine code generation

**Key Patterns:**
- Maps ClickHouse types (String, DateTime, Nullable, LowCardinality, etc.) to SDK validators
- Generates proper import statements based on used types
- Handles all pipe types: endpoint, materialized, copy, regular

### CLI Layer (`src/cli/`)

User-facing commands for development workflow.

**Commands:**
- `tinybird init` - Initialize a new project (detects existing `.datasource`/`.pipe` files)
- `tinybird dev` - Watch mode with hot reload (feature branches only)
- `tinybird build` - Build and deploy to branches (not main)
- `tinybird deploy` - Deploy to main workspace (production)
- `tinybird login` - Authenticate with Tinybird
- `tinybird branch` - Branch management (list, status, delete)

**Key Files:**
- `index.ts` - Command definitions and entry point
- `commands/init.ts` - Project initialization with datafile detection
- `commands/build.ts` - Build to branches
- `commands/deploy.ts` - Deploy to main
- `commands/dev.ts` - Watch mode
- `commands/login.ts` - Authentication
- `commands/branch.ts` - Branch management
- `config.ts` - Configuration file management
- `env.ts` - Environment variable loading
- `git.ts` - Git integration for branch detection

**Safety Features:**
- `build` and `dev` commands are blocked on main branch in cloud mode
- Only `deploy` command can push to production
- Prevents accidental production deployments during development

### Client Layer (`src/client/`)

Runtime client for querying and ingesting data.

**Files:**
- `base.ts` - `TinybirdClient` class with query/ingest methods
- `types.ts` - Response types and error handling

**Usage:**
```typescript
const client = createClient({
  baseUrl: 'https://api.tinybird.co',
  token: 'p.xxx',
});

// Query a pipe
const result = await client.query('top_events', { limit: 10 });

// Ingest data
await client.ingest('events', { timestamp: new Date(), event_type: 'click' });
```

### Type Inference (`src/infer/`)

Utility types for inferring TypeScript types from schema definitions.

**Key Types:**
- `InferRow<T>` - Infer the row type of a datasource
- `InferParams<T>` - Infer the params type of a pipe
- `InferOutputRow<T>` - Infer the output row type of a pipe

## Data Flow

### Build Flow

```
1. User defines schema in TypeScript
   │
   ▼
2. loadSchema() bundles with esbuild and executes
   │
   ▼
3. generateResources() converts to datafile content
   │
   ▼
4. buildToTinybird() sends as multipart form to API
   │
   ▼
5. Tinybird API processes and creates resources
```

### Development Flow (tinybird dev)

```
1. Load schema files
   │
   ▼
2. Generate resources
   │
   ▼
3. Deploy to Tinybird (dev branch)
   │
   ▼
4. Watch for file changes ─────────┐
   │                               │
   ▼                               │
5. On change, reload and redeploy ─┘
```

## Connection Types

### Kafka Connection

Kafka connections are defined with `createKafkaConnection()` and referenced in datasources:

```typescript
// Define connection
const myKafka = createKafkaConnection('my_kafka', {
  bootstrapServers: 'kafka.example.com:9092',
  securityProtocol: 'SASL_SSL',
  saslMechanism: 'PLAIN',
  key: '{{ tb_secret("KAFKA_KEY") }}',
  secret: '{{ tb_secret("KAFKA_SECRET") }}',
});

// Reference in datasource
const kafkaEvents = defineDatasource('kafka_events', {
  schema: { /* ... */ },
  kafka: {
    connection: myKafka,
    topic: 'events',
    groupId: 'my-consumer-group',
    autoOffsetReset: 'earliest',
  },
});
```

Generated `.connection` content:
```
TYPE kafka
KAFKA_BOOTSTRAP_SERVERS kafka.example.com:9092
KAFKA_SECURITY_PROTOCOL SASL_SSL
KAFKA_SASL_MECHANISM PLAIN
KAFKA_KEY {{ tb_secret("KAFKA_KEY") }}
KAFKA_SECRET {{ tb_secret("KAFKA_SECRET") }}
```

Generated `.datasource` content includes:
```
KAFKA_CONNECTION_NAME my_kafka
KAFKA_TOPIC events
KAFKA_GROUP_ID my-consumer-group
KAFKA_AUTO_OFFSET_RESET earliest
```

## Testing

Tests are colocated with source files (`*.test.ts`).

Run all tests:
```bash
pnpm test
```

Run tests in watch mode:
```bash
pnpm test --watch
```
