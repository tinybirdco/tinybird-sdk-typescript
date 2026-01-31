# @tinybird/sdk

A TypeScript SDK for defining Tinybird resources with full type inference. Define your datasources, pipes, and queries in TypeScript and sync them directly to Tinybird.

## Installation

```bash
npm install @tinybird/sdk
```

## Quick Start

### 1. Initialize your project

```bash
npx tinybird init
```

This creates:
- `tinybird.json` - Configuration file
- `src/tinybird/schema.ts` - Your schema entry point

### 2. Configure your token

Create a `.env.local` file:

```env
TINYBIRD_TOKEN=p.your_token_here
```

### 3. Define your schema

```typescript
// src/tinybird/schema.ts
import { defineProject, defineDatasource, definePipe, node, t, p, engine } from "@tinybird/sdk";

// Define a datasource with full type inference
const pageViews = defineDatasource("page_views", {
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

// Define a pipe (API endpoint)
const topPages = definePipe("top_pages", {
  description: "Get the most visited pages",
  params: {
    start_date: p.dateTime(),
    end_date: p.dateTime(),
    limit: p.int32().optional(10),
  },
  nodes: [
    node({
      name: "endpoint",
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
  endpoint: true,
});

// Export your project
export default defineProject({
  datasources: { pageViews },
  pipes: { topPages },
});
```

### 4. Start development

```bash
npx tinybird dev
```

This watches your schema files and automatically syncs changes to Tinybird.

### 5. Use the typed client

```typescript
import { TinybirdClient } from "@tinybird/sdk";
import { pageViews, topPages } from "./tinybird/schema";

const client = new TinybirdClient({
  baseUrl: "https://api.tinybird.co",
  token: process.env.TINYBIRD_TOKEN!,
});

// Type-safe data ingestion
await client.ingest(pageViews, [
  {
    timestamp: new Date(),
    pathname: "/home",
    session_id: "abc123",
    country: "US",
  },
]);

// Type-safe queries with autocomplete
const result = await client.query(topPages, {
  start_date: new Date("2024-01-01"),
  end_date: new Date(),
  limit: 5,
});

// result.data is fully typed: { pathname: string, views: number }[]
```

## CLI Commands

### `npx tinybird init`

Initialize a new Tinybird TypeScript project.

```bash
npx tinybird init
npx tinybird init --force  # Overwrite existing files
```

### `npx tinybird build`

Build and push resources to Tinybird.

```bash
npx tinybird build
npx tinybird build --dry-run  # Preview without pushing
```

### `npx tinybird dev`

Watch for changes and sync with Tinybird automatically.

```bash
npx tinybird dev
```

## Configuration

Create a `tinybird.json` in your project root:

```json
{
  "schema": "src/tinybird/schema.ts",
  "token": "${TINYBIRD_TOKEN}",
  "baseUrl": "https://api.tinybird.co"
}
```

- `schema` - Path to your TypeScript schema entry point
- `token` - API token (supports environment variable interpolation)
- `baseUrl` - Tinybird API URL (defaults to EU region)

## Type Validators

Use `t.*` to define column types with full TypeScript inference:

```typescript
import { t } from "@tinybird/sdk";

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

  // Modifiers
  optional_field: t.string().nullable(),
  category: t.string().lowCardinality(),
  status: t.string().default("pending"),
};
```

## Parameter Validators

Use `p.*` to define pipe query parameters:

```typescript
import { p } from "@tinybird/sdk";

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
import { engine } from "@tinybird/sdk";

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
    "dev": "concurrently -n next,tinybird \"next dev\" \"dotenv -e .env.local -- npx tinybird dev\"",
    "tinybird:build": "dotenv -e .env.local -- npx tinybird build"
  },
  "devDependencies": {
    "concurrently": "^9.0.0",
    "dotenv-cli": "^11.0.0"
  }
}
```

Now `npm run dev` starts both Next.js and Tinybird sync together.

## Type Inference

The SDK provides full type inference for your schemas:

```typescript
import { InferDatasourceRow, InferPipeParams, InferPipeOutput } from "@tinybird/sdk";
import { pageViews, topPages } from "./tinybird/schema";

// Infer the row type for a datasource
type PageViewRow = InferDatasourceRow<typeof pageViews>;
// { timestamp: Date, pathname: string, session_id: string, country: string | null }

// Infer the parameters for a pipe
type TopPagesParams = InferPipeParams<typeof topPages>;
// { start_date: Date, end_date: Date, limit?: number }

// Infer the output type for a pipe
type TopPagesOutput = InferPipeOutput<typeof topPages>;
// { pathname: string, views: number }
```

## License

MIT
