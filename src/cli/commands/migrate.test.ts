import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runMigrate } from "./migrate.js";

function writeFile(dir: string, relativePath: string, content: string): void {
  const fullPath = path.join(dir, relativePath);
  fs.mkdirSync(path.dirname(fullPath), { recursive: true });
  fs.writeFileSync(fullPath, content);
}

function readExpectedFixture(name: string): string {
  return fs.readFileSync(
    path.resolve(process.cwd(), "test/fixtures/migrate", name),
    "utf-8"
  );
}

describe("runMigrate", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    for (const dir of tempDirs) {
      try {
        fs.rmSync(dir, { recursive: true });
      } catch {
        // Ignore cleanup failures
      }
    }
    tempDirs.length = 0;
  });

  it("migrates complex resources including endpoint, materialized, and copy pipes", async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-migrate-"));
    tempDirs.push(tempDir);

    writeFile(
      tempDir,
      "stream.connection",
      `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
KAFKA_SECURITY_PROTOCOL SASL_SSL
KAFKA_SASL_MECHANISM PLAIN
KAFKA_KEY api-key
KAFKA_SECRET api-secret
KAFKA_SSL_CA_PEM ca-pem-content
`
    );

    writeFile(
      tempDir,
      "events.datasource",
      `DESCRIPTION >
    Events from Kafka stream
SCHEMA >
    event_id String \`json:$.event_id\`,
    user_id UInt64 \`json:$.user.id\`,
    env String \`json:$.env\` DEFAULT 'prod',
    is_test Bool \`json:$.meta.is_test\` DEFAULT 0,
    updated_at DateTime \`json:$.updated_at\`,
    payload String \`json:$.payload\` DEFAULT '{}' CODEC(ZSTD(1))

ENGINE "ReplacingMergeTree"
ENGINE_SORTING_KEY "event_id, user_id"
ENGINE_PARTITION_KEY "toYYYYMM(updated_at)"
ENGINE_PRIMARY_KEY "event_id"
ENGINE_TTL "updated_at + toIntervalDay(30)"
ENGINE_VER "updated_at"
ENGINE_SETTINGS "index_granularity=8192, enable_mixed_granularity_parts=true"
KAFKA_CONNECTION_NAME stream
KAFKA_TOPIC events_topic
KAFKA_GROUP_ID events-consumer
KAFKA_AUTO_OFFSET_RESET earliest
TOKEN events_read READ
TOKEN events_append APPEND
SHARED_WITH >
    workspace_a,
    workspace_b
FORWARD_QUERY >
    SELECT *
    FROM events_mv
`
    );

    writeFile(
      tempDir,
      "events_rollup.datasource",
      `SCHEMA >
    user_id UInt64,
    total UInt64

ENGINE "SummingMergeTree"
ENGINE_SORTING_KEY "user_id"
ENGINE_SUMMING_COLUMNS "total"
`
    );

    writeFile(
      tempDir,
      "events_endpoint.pipe",
      `DESCRIPTION >
    Endpoint for filtered events
NODE base
DESCRIPTION >
    Base filter
SQL >
    %
    SELECT event_id, user_id, payload
    FROM events
    WHERE user_id = {{UInt64(user_id)}}
      AND env = {{String(env, 'prod')}}
NODE endpoint
SQL >
    SELECT event_id AS event_id, user_id AS user_id
    FROM base
TYPE endpoint
CACHE 120
TOKEN endpoint_token READ
`
    );

    writeFile(
      tempDir,
      "events_mv.pipe",
      `DESCRIPTION >
    Materialized rollup
NODE rollup
SQL >
    SELECT user_id, count() AS total
    FROM events
    GROUP BY user_id
TYPE MATERIALIZED
DATASOURCE events_rollup
DEPLOYMENT_METHOD alter
TOKEN mv_token READ
`
    );

    writeFile(
      tempDir,
      "copy_events.pipe",
      `NODE copy_node
SQL >
    SELECT event_id, user_id
    FROM events
TYPE COPY
TARGET_DATASOURCE events_rollup
COPY_SCHEDULE @on-demand
COPY_MODE replace
TOKEN copy_token READ
`
    );

    writeFile(
      tempDir,
      "stats_pipe.pipe",
      `NODE agg
SQL >
    SELECT user_id, count() AS total
    FROM events
    GROUP BY user_id
NODE final
SQL >
    SELECT user_id, total
    FROM agg
    WHERE total > {{UInt32(min_total, 10)}}
TOKEN stats_token READ
`
    );

    const result = await runMigrate({
      cwd: tempDir,
      patterns: ["."],
      strict: true,
    });

    expect(result.success).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.migrated).toHaveLength(7);
    expect(result.migrated.filter((resource) => resource.kind === "connection")).toHaveLength(1);
    expect(result.migrated.filter((resource) => resource.kind === "datasource")).toHaveLength(2);
    expect(result.migrated.filter((resource) => resource.kind === "pipe")).toHaveLength(4);
    expect(path.basename(result.outputPath)).toBe("tinybird.migration.ts");
    expect(fs.existsSync(result.outputPath)).toBe(true);

    const output = fs.readFileSync(result.outputPath, "utf-8");
    const expected = readExpectedFixture("complex.expected.ts");
    expect(output).toBe(expected);
  });

  it("continues processing and reports all errors while writing complex migratable resources", async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-migrate-"));
    tempDirs.push(tempDir);

    writeFile(
      tempDir,
      "stream.connection",
      `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
`
    );

    writeFile(
      tempDir,
      "events.datasource",
      `SCHEMA >
    event_id String,
    user_id UInt64,
    created_at DateTime

ENGINE "MergeTree"
ENGINE_SORTING_KEY "event_id"
KAFKA_CONNECTION_NAME stream
KAFKA_TOPIC events_topic
`
    );

    writeFile(
      tempDir,
      "events_endpoint.pipe",
      `NODE source
SQL >
    SELECT event_id, user_id
    FROM events
NODE endpoint
SQL >
    SELECT event_id AS event_id, user_id AS user_id
    FROM source
    WHERE user_id = {{UInt64(user_id)}}
TYPE endpoint
TOKEN endpoint_token READ
`
    );

    writeFile(
      tempDir,
      "events_mv.pipe",
      `NODE rollup
SQL >
    SELECT user_id, count() AS total
    FROM events
    GROUP BY user_id
TYPE MATERIALIZED
DATASOURCE missing_ds
`
    );

    writeFile(
      tempDir,
      "broken.pipe",
      `NODE broken
SQL >
    SELECT *
    FROM events
TYPE endpoint
UNSUPPORTED_DIRECTIVE true
`
    );

    const result = await runMigrate({
      cwd: tempDir,
      patterns: ["."],
      strict: true,
    });

    expect(result.success).toBe(false);
    expect(result.errors).toHaveLength(2);
    expect(result.errors.map((error) => error.message)).toEqual(
      expect.arrayContaining([
        'Unsupported pipe directive in strict mode: "UNSUPPORTED_DIRECTIVE true"',
        'Materialized pipe references missing/unmigrated datasource "missing_ds".',
      ])
    );
    expect(result.migrated.filter((resource) => resource.kind === "connection")).toHaveLength(1);
    expect(result.migrated.filter((resource) => resource.kind === "datasource")).toHaveLength(1);
    expect(result.migrated.filter((resource) => resource.kind === "pipe")).toHaveLength(1);
    expect(fs.existsSync(result.outputPath)).toBe(true);

    const output = fs.readFileSync(result.outputPath, "utf-8");
    const expected = readExpectedFixture("partial.expected.ts");
    expect(output).toBe(expected);
  });

  it("returns exact output content in dry-run mode for complex migratable resources", async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-migrate-"));
    tempDirs.push(tempDir);

    writeFile(
      tempDir,
      "stream.connection",
      `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
`
    );

    writeFile(
      tempDir,
      "events.datasource",
      `SCHEMA >
    event_id String,
    user_id UInt64,
    created_at DateTime

ENGINE "MergeTree"
ENGINE_SORTING_KEY "event_id"
KAFKA_CONNECTION_NAME stream
KAFKA_TOPIC events_topic
`
    );

    writeFile(
      tempDir,
      "events_endpoint.pipe",
      `NODE source
SQL >
    SELECT event_id, user_id
    FROM events
NODE endpoint
SQL >
    SELECT event_id AS event_id, user_id AS user_id
    FROM source
    WHERE user_id = {{UInt64(user_id)}}
TYPE endpoint
TOKEN endpoint_token READ
`
    );

    const result = await runMigrate({
      cwd: tempDir,
      patterns: ["."],
      strict: true,
      dryRun: true,
    });

    expect(result.success).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.migrated).toHaveLength(3);
    const expected = readExpectedFixture("partial.expected.ts");
    expect(result.outputContent).toBe(expected);
    expect(fs.existsSync(result.outputPath)).toBe(false);
  });
});
