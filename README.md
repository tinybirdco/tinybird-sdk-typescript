# @tinybirdco/sdk

> **Note:** This package is experimental. APIs may change between versions.

A TypeScript SDK for defining Tinybird resources with full type inference. Define your datasources, pipes, and queries in TypeScript and sync them directly to Tinybird.

## Installation

```bash
npm install @tinybirdco/sdk
```

## Requirements

TypeScript `>=4.9` is supported for consumers.

Officially supported runtime:
- Node.js 20 LTS or later non-EOL versions

Not officially supported (untested in this repository at this time):
- Deno `>=1.28.0`
- Bun `>=1.0.0`
- Cloudflare Workers
- Vercel Edge Runtime
- Jest `>=28` (including `"node"` environment)
- Nitro `>=2.6.0`

Web browsers are not supported. This SDK is designed for server-side usage and using it directly in the browser may expose Tinybird API credentials.

If you need support for other runtimes, please open or upvote an issue on GitHub.

## Quick Start

### 1. Initialize your project

```bash
npx tinybird init
```

This creates:
- `tinybird.json` - Configuration file
- `src/tinybird/datasources.ts` - Define your datasources
- `src/tinybird/pipes.ts` - Define your pipes/endpoints
- `src/tinybird/client.ts` - Your typed Tinybird client

### 2. Configure your token

Create a `.env.local` file:

```env
TINYBIRD_TOKEN=p.your_token_here
```

### 3. Define your datasources

```typescript
// src/tinybird/datasources.ts
import { defineDatasource, t, engine, type InferRow } from "@tinybirdco/sdk";

export const pageViews = defineDatasource("page_views", {
  description: "Page view tracking data",
  schema: {
    timestamp: t.dateTime(),
    pathname: t.string(),
    session_id: t.string(),
    country: t.string().lowCardinality().nullable(),
  },
  engine: engine.mergeTree({
    sortingKey: ["pathname", "timestamp"],
  }),
});

// Export row type for ingestion
export type PageViewsRow = InferRow<typeof pageViews>;
```

### 4. Define your endpoints

```typescript
// src/tinybird/pipes.ts
import { defineEndpoint, node, t, p, type InferParams, type InferOutputRow } from "@tinybirdco/sdk";

export const topPages = defineEndpoint("top_pages", {
  description: "Get the most visited pages",
  params: {
    start_date: p.dateTime(),
    end_date: p.dateTime(),
    limit: p.int32().optional(10),
  },
  nodes: [
    node({
      name: "aggregated",
      sql: `
        SELECT pathname, count() AS views
        FROM page_views
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
        GROUP BY pathname
        ORDER BY views DESC
        LIMIT {{Int32(limit, 10)}}
      `,
    }),
  ],
  output: {
    pathname: t.string(),
    views: t.uint64(),
  },
});

// Export endpoint types
export type TopPagesParams = InferParams<typeof topPages>;
export type TopPagesOutput = InferOutputRow<typeof topPages>;
```

### 5. Create your client

```typescript
// src/tinybird/client.ts
import { createTinybirdClient } from "@tinybirdco/sdk";
import { pageViews, type PageViewsRow } from "./datasources";
import { topPages, type TopPagesParams, type TopPagesOutput } from "./pipes";

export const tinybird = createTinybirdClient({
  datasources: { pageViews },
  pipes: { topPages },
});

// Re-export types for convenience
export type { PageViewsRow, TopPagesParams, TopPagesOutput };
export { pageViews, topPages };
```

### 6. Add path alias (for Next.js/TypeScript projects)

Add to your `tsconfig.json`:

```json
{
  "compilerOptions": {
    "paths": {
      "@tinybird/client": ["./src/tinybird/client.ts"]
    }
  }
}
```

### 7. Start development

```bash
npx tinybird dev
```

This watches your schema files and automatically syncs changes to Tinybird.

### 8. Use the typed client

```typescript
import { tinybird, type PageViewsRow } from "@tinybird/client";

// Type-safe data ingestion
await tinybird.ingest.pageViews({
  timestamp: new Date(),
  pathname: "/home",
  session_id: "abc123",
  country: "US",
});

// Type-safe queries with autocomplete
const result = await tinybird.query.topPages({
  start_date: new Date("2024-01-01"),
  end_date: new Date(),
  limit: 5,
});

// result.data is fully typed: { pathname: string, views: bigint }[]
```

## CLI Commands

### `npx tinybird init`

Initialize a new Tinybird TypeScript project. If you have existing `.datasource` and `.pipe` files in your repository, the CLI will detect them and ask if you want to include them in your configuration.

```bash
npx tinybird init
npx tinybird init --force       # Overwrite existing files
npx tinybird init --skip-login  # Skip browser login flow
```

This enables incremental migration for existing Tinybird projects - you can keep your `.datasource` and `.pipe` files alongside TypeScript definitions.

### `tinybird dev`

Watch for changes and sync with Tinybird automatically. Only works on feature branches (not main).

```bash
tinybird dev
tinybird dev --local   # Sync with local Tinybird container
tinybird dev --branch  # Explicitly use Tinybird cloud with branches
```

**Note:** `dev` mode is blocked on the main branch to prevent accidental production deployments. Use `tinybird deploy` for production, or switch to a feature branch.

### `tinybird build`

Build and push resources to a Tinybird branch (not main).

```bash
tinybird build
tinybird build --dry-run  # Preview without pushing
tinybird build --local    # Build to local Tinybird container
```

**Note:** `build` is blocked on the main branch. Use `tinybird deploy` for production deployments.

### `tinybird deploy`

Deploy resources to the main Tinybird workspace (production). This is the only way to deploy to main.

```bash
tinybird deploy
tinybird deploy --dry-run  # Preview without pushing
```

### `tinybird login`

Authenticate with Tinybird via browser.

```bash
tinybird login
```

### `tinybird branch`

Manage Tinybird branches.

```bash
tinybird branch list      # List all branches
tinybird branch status    # Show current branch status
tinybird branch delete <name>  # Delete a branch
```

## Configuration

Create a `tinybird.json` in your project root:

```json
{
  "include": [
    "src/tinybird/datasources.ts",
    "src/tinybird/pipes.ts",
    "src/tinybird/legacy.datasource",
    "src/tinybird/legacy.pipe"
  ],
  "token": "${TINYBIRD_TOKEN}",
  "baseUrl": "https://api.tinybird.co",
  "devMode": "branch"
}
```

You can mix TypeScript files with raw `.datasource` and `.pipe` files for incremental migration.

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `include` | `string[]` | *required* | Array of TypeScript files or raw `.datasource`/`.pipe` files to include |
| `token` | `string` | *required* | API token. Supports `${ENV_VAR}` interpolation for environment variables |
| `baseUrl` | `string` | `"https://api.tinybird.co"` | Tinybird API URL. Use `"https://api.us-east.tinybird.co"` for US region |
| `devMode` | `"branch"` \| `"local"` | `"branch"` | Development mode. `"branch"` uses Tinybird cloud with branches, `"local"` uses local Docker container |

### Local Development Mode

Use a local Tinybird container for development without affecting your cloud workspace:

1. Start the local container:
   ```bash
   docker run -d -p 7181:7181 --name tinybird-local tinybirdco/tinybird-local:latest
   ```

2. Configure your project:
   ```json
   {
     "devMode": "local"
   }
   ```

   Or use the CLI flag:
   ```bash
   npx tinybird dev --local
   ```

In local mode:
- Tokens are automatically obtained from the local container
- A workspace is created per git branch
- No cloud authentication required

## Defining Resources

### Datasources

```typescript
import { defineDatasource, t, engine, type InferRow } from "@tinybirdco/sdk";

export const events = defineDatasource("events", {
  description: "Event tracking data",
  schema: {
    timestamp: t.dateTime(),
    event_name: t.string().lowCardinality(),
    user_id: t.string().nullable(),
    properties: t.string(), // JSON as string
  },
  engine: engine.mergeTree({
    sortingKey: ["event_name", "timestamp"],
    partitionKey: "toYYYYMM(timestamp)",
    ttl: "timestamp + INTERVAL 90 DAY",
  }),
});

export type EventsRow = InferRow<typeof events>;
```

### Endpoints (API pipes)

```typescript
import { defineEndpoint, node, t, p, type InferParams, type InferOutputRow } from "@tinybirdco/sdk";

export const topEvents = defineEndpoint("top_events", {
  description: "Get the most frequent events",
  params: {
    start_date: p.dateTime(),
    end_date: p.dateTime(),
    limit: p.int32().optional(10),
  },
  nodes: [
    node({
      name: "aggregated",
      sql: `
        SELECT event_name, count() AS event_count
        FROM events
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT {{Int32(limit, 10)}}
      `,
    }),
  ],
  output: {
    event_name: t.string(),
    event_count: t.uint64(),
  },
});

export type TopEventsParams = InferParams<typeof topEvents>;
export type TopEventsOutput = InferOutputRow<typeof topEvents>;
```

### Internal Pipes (not exposed as API)

```typescript
import { definePipe, node } from "@tinybirdco/sdk";

export const filteredEvents = definePipe("filtered_events", {
  description: "Filter events by date range",
  params: {
    start_date: p.dateTime(),
    end_date: p.dateTime(),
  },
  nodes: [
    node({
      name: "filtered",
      sql: `
        SELECT * FROM events
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
      `,
    }),
  ],
});
```

### Materialized Views

```typescript
import { defineDatasource, defineMaterializedView, t, engine } from "@tinybirdco/sdk";

// Target datasource for the materialized view
export const dailyStats = defineDatasource("daily_stats", {
  description: "Daily aggregated statistics",
  schema: {
    date: t.date(),
    pathname: t.string(),
    views: t.simpleAggregateFunction("sum", t.uint64()),
    unique_sessions: t.aggregateFunction("uniq", t.string()),
  },
  engine: engine.aggregatingMergeTree({
    sortingKey: ["date", "pathname"],
  }),
});

// Materialized view that populates the datasource
export const dailyStatsMv = defineMaterializedView("daily_stats_mv", {
  description: "Materialize daily page view aggregations",
  datasource: dailyStats,
  nodes: [
    node({
      name: "aggregate",
      sql: `
        SELECT
          toDate(timestamp) AS date,
          pathname,
          count() AS views,
          uniqState(session_id) AS unique_sessions
        FROM page_views
        GROUP BY date, pathname
      `,
    }),
  ],
});
```

### Copy Pipes

```typescript
import { defineCopyPipe, node } from "@tinybirdco/sdk";

// Scheduled copy pipe
export const dailySnapshot = defineCopyPipe("daily_snapshot", {
  description: "Daily snapshot of statistics",
  datasource: snapshotDatasource,
  schedule: "0 0 * * *", // Run daily at midnight
  mode: "append",
  nodes: [
    node({
      name: "snapshot",
      sql: `
        SELECT today() AS snapshot_date, pathname, count() AS views
        FROM page_views
        WHERE toDate(timestamp) = today() - 1
        GROUP BY pathname
      `,
    }),
  ],
});

// On-demand copy pipe
export const manualReport = defineCopyPipe("manual_report", {
  description: "On-demand report generation",
  datasource: reportDatasource,
  schedule: "@on-demand",
  mode: "replace",
  nodes: [
    node({
      name: "report",
      sql: `SELECT * FROM events WHERE timestamp >= now() - interval 7 day`,
    }),
  ],
});
```

### Static Tokens

Define reusable tokens for resource access control:

```typescript
import { defineToken, defineDatasource, defineEndpoint, t, node } from "@tinybirdco/sdk";

// Define a token once
const appToken = defineToken("app_read");
const ingestToken = defineToken("ingest_token");

// Use in datasources with READ or APPEND scope
export const events = defineDatasource("events", {
  schema: {
    timestamp: t.dateTime(),
    event_name: t.string(),
  },
  tokens: [
    { token: appToken, scope: "READ" },
    { token: ingestToken, scope: "APPEND" },
  ],
});

// Use in endpoints with READ scope
export const topEvents = defineEndpoint("top_events", {
  nodes: [node({ name: "endpoint", sql: "SELECT * FROM events LIMIT 10" })],
  output: { timestamp: t.dateTime(), event_name: t.string() },
  tokens: [{ token: appToken, scope: "READ" }],
});
```

TypeScript provides autocomplete for the correct scopes:
- **Datasources**: `READ` (query access) or `APPEND` (ingest access)
- **Pipes**: `READ` only

## Type Validators

Use `t.*` to define column types with full TypeScript inference:

```typescript
import { t } from "@tinybirdco/sdk";

const schema = {
  // Strings
  name: t.string(),
  id: t.uuid(),
  code: t.fixedString(3),

  // Numbers
  count: t.int32(),
  amount: t.float64(),
  big_number: t.uint64(),
  price: t.decimal(10, 2),

  // Date/Time
  created_at: t.dateTime(),
  updated_at: t.dateTime64(3),
  birth_date: t.date(),

  // Boolean
  is_active: t.bool(),

  // Complex types
  tags: t.array(t.string()),
  metadata: t.map(t.string(), t.string()),

  // Aggregate functions (for materialized views)
  total: t.simpleAggregateFunction("sum", t.uint64()),
  unique_users: t.aggregateFunction("uniq", t.string()),

  // Modifiers
  optional_field: t.string().nullable(),
  category: t.string().lowCardinality(),
  status: t.string().default("pending"),
};
```

## Parameter Validators

Use `p.*` to define pipe query parameters:

```typescript
import { p } from "@tinybirdco/sdk";

const params = {
  // Required parameters
  start_date: p.dateTime(),
  user_id: p.string(),

  // Optional with defaults
  limit: p.int32().optional(10),
  offset: p.int32().optional(0),

  // With descriptions (for documentation)
  status: p.string().optional("active").describe("Filter by status"),
};
```

## Engine Configurations

```typescript
import { engine } from "@tinybirdco/sdk";

// MergeTree (default)
engine.mergeTree({
  sortingKey: ["user_id", "timestamp"],
  partitionKey: "toYYYYMM(timestamp)",
  ttl: "timestamp + INTERVAL 90 DAY",
});

// ReplacingMergeTree (for upserts)
engine.replacingMergeTree({
  sortingKey: ["id"],
  version: "updated_at",
});

// SummingMergeTree (for pre-aggregation)
engine.summingMergeTree({
  sortingKey: ["date", "category"],
  columns: ["count", "total"],
});

// AggregatingMergeTree (for complex aggregates)
engine.aggregatingMergeTree({
  sortingKey: ["date"],
});
```

## Next.js Integration

For Next.js projects, add these scripts to your `package.json`:

```json
{
  "scripts": {
    "dev": "concurrently -n next,tinybird \"next dev\" \"tinybird dev\"",
    "tinybird:build": "tinybird build"
  },
  "devDependencies": {
    "concurrently": "^9.0.0"
  }
}
```

The CLI automatically loads `.env.local` and `.env` files, so no additional setup is needed.

Add the path alias to `tsconfig.json`:

```json
{
  "compilerOptions": {
    "paths": {
      "@/*": ["./src/*"],
      "@tinybird/client": ["./src/tinybird/client.ts"]
    }
  }
}
```

Now `npm run dev` starts both Next.js and Tinybird sync together.

## Type Inference

The SDK provides full type inference for your schemas:

```typescript
import { type InferRow, type InferParams, type InferOutputRow } from "@tinybirdco/sdk";
import { pageViews, topPages } from "./tinybird/datasources";

// Infer the row type for a datasource
type PageViewRow = InferRow<typeof pageViews>;
// { timestamp: Date, pathname: string, session_id: string, country: string | null }

// Infer the parameters for a pipe
type TopPagesParams = InferParams<typeof topPages>;
// { start_date: Date, end_date: Date, limit?: number }

// Infer the output type for a pipe
type TopPagesOutput = InferOutputRow<typeof topPages>;
// { pathname: string, views: bigint }
```

## License

MIT
