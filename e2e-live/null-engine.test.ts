import {
  describe,
  it,
  expect,
  beforeAll,
  beforeEach,
  afterEach,
  afterAll,
} from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { runInit } from "../src/cli/commands/init.js";
import { runBuild } from "../src/cli/commands/build.js";
import { getBranch, deleteBranch, BranchApiError } from "../src/api/branches.js";
import { createClient } from "../src/client/base.js";
import {
  getLiveE2EConfigFromEnv,
  assertWorkspaceAdminToken,
  createWorkspaceWithToken,
  deleteWorkspace,
  type LiveE2EConfig,
} from "./cloud-workspace.js";
import {
  ensureDistBuild,
  createTempProjectDir,
  cleanupTempProjectDir,
  setConfigBaseUrl,
} from "./test-project.js";

const liveConfig = getLiveE2EConfigFromEnv();
const describeLive = liveConfig ? describe : describe.skip;

const SOURCE_DATASOURCE_NAME = "null_engine_source_events";
const TARGET_DATASOURCE_NAME = "null_engine_processed_events";
const MATERIALIZED_PIPE_NAME = "null_engine_processed_mv";
const ENDPOINT_NAME = "null_engine_processed_rows";

function toTinybirdDateTime(value: Date): string {
  return value.toISOString().slice(0, 19).replace("T", " ");
}

function writeNullEngineTestEntities(projectDir: string): void {
  const tinybirdFile = path.join(projectDir, "lib", "tinybird.ts");
  const content = `import {
  defineDatasource,
  defineEndpoint,
  defineMaterializedView,
  Tinybird,
  node,
  t,
  engine,
} from "@tinybirdco/sdk";

export const nullEngineSourceEvents = defineDatasource("${SOURCE_DATASOURCE_NAME}", {
  schema: {
    timestamp: t.dateTime(),
    run_id: t.string(),
    metric_value: t.int32(),
  },
  engine: engine.null(),
});

export const nullEngineProcessedEvents = defineDatasource("${TARGET_DATASOURCE_NAME}", {
  schema: {
    timestamp: t.dateTime(),
    run_id: t.string(),
    metric_value: t.int32(),
    doubled_metric_value: t.int32(),
  },
  engine: engine.mergeTree({
    sortingKey: ["run_id", "timestamp"],
  }),
});

export const nullEngineProcessedMv = defineMaterializedView("${MATERIALIZED_PIPE_NAME}", {
  datasource: nullEngineProcessedEvents,
  nodes: [
    node({
      name: "processed",
      sql: \`
        SELECT
          timestamp,
          run_id,
          metric_value,
          metric_value * 2 AS doubled_metric_value
        FROM ${SOURCE_DATASOURCE_NAME}
      \`,
    }),
  ],
});

export const nullEngineProcessedRows = defineEndpoint("${ENDPOINT_NAME}", {
  nodes: [
    node({
      name: "rows",
      sql: \`
        SELECT
          timestamp,
          run_id,
          metric_value,
          doubled_metric_value
        FROM ${TARGET_DATASOURCE_NAME}
        ORDER BY timestamp DESC
        LIMIT 100
      \`,
    }),
  ],
  output: {
    timestamp: t.dateTime(),
    run_id: t.string(),
    metric_value: t.int32(),
    doubled_metric_value: t.int32(),
  },
});

export const tinybird = new Tinybird({
  datasources: {
    nullEngineSourceEvents,
    nullEngineProcessedEvents,
  },
  pipes: {
    nullEngineProcessedMv,
    nullEngineProcessedRows,
  },
});
`;

  fs.writeFileSync(tinybirdFile, content);
}

async function waitForProcessedRows(
  branchToken: string,
  baseUrl: string,
  runId: string
): Promise<Array<Record<string, unknown>>> {
  const client = createClient({ baseUrl, token: branchToken });

  for (let attempt = 0; attempt < 15; attempt++) {
    const result = await client.query<Record<string, unknown>>(ENDPOINT_NAME);
    const rows = result.data.filter((row) => row.run_id === runId);

    if (rows.length > 0) {
      return rows;
    }

    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }

  return [];
}

describeLive("E2E Live: Null engine", () => {
  const config = liveConfig as LiveE2EConfig;

  let tempDir = "";
  let originalEnv: NodeJS.ProcessEnv;

  let workspaceId = "";
  let workspaceName = "";
  let workspaceToken = "";
  let branchName = "";

  beforeAll(async () => {
    ensureDistBuild();
    await assertWorkspaceAdminToken(config);

    const workspace = await createWorkspaceWithToken(config, "sdk_null_engine");
    workspaceId = workspace.id;
    workspaceName = workspace.name;
    workspaceToken = workspace.token;
  });

  beforeEach(() => {
    tempDir = createTempProjectDir();
    originalEnv = { ...process.env };
    process.env.GITHUB_REF_NAME = `live-null-engine/${workspaceName}`;
    process.env.TINYBIRD_TOKEN = workspaceToken;
  });

  afterEach(async () => {
    if (branchName) {
      try {
        await deleteBranch(
          { baseUrl: config.baseUrl, token: workspaceToken },
          branchName
        );
      } catch (error) {
        if (!(error instanceof BranchApiError && error.status === 404)) {
          throw error;
        }
      }
    }

    branchName = "";
    process.env = originalEnv;
    cleanupTempProjectDir(tempDir);
  });

  afterAll(async () => {
    if (workspaceId && workspaceName) {
      await deleteWorkspace(config, workspaceId, workspaceName);
    }
  });

  it("builds a Null engine source and ingests through its materialized view target", async () => {
    const initResult = await runInit({
      cwd: tempDir,
      skipLogin: true,
      devMode: "branch",
      clientPath: "lib/tinybird.ts",
    });
    expect(initResult.success).toBe(true);

    setConfigBaseUrl(tempDir, config.baseUrl);
    writeNullEngineTestEntities(tempDir);

    const buildResult = await runBuild({ cwd: tempDir });
    expect(buildResult.success).toBe(true);
    expect(buildResult.deploy?.success).toBe(true);

    const sourceDatasource = buildResult.build?.resources.datasources.find(
      (datasource) => datasource.name === SOURCE_DATASOURCE_NAME
    );
    expect(sourceDatasource?.content).toContain("ENGINE Null");
    expect(sourceDatasource?.content).not.toContain("ENGINE_SORTING_KEY");

    branchName = buildResult.branchInfo?.tinybirdBranch ?? "";
    expect(branchName).toBeTruthy();

    const branch = await getBranch(
      { baseUrl: config.baseUrl, token: workspaceToken },
      branchName
    );
    const branchToken = branch.token ?? "";
    expect(branchToken).toBeTruthy();

    const client = createClient({
      baseUrl: config.baseUrl,
      token: branchToken,
    });

    const runId = `null_engine_${Date.now()}`;
    const metricValue = 21;

    const ingestResult = await client.datasources.ingest(SOURCE_DATASOURCE_NAME, {
      timestamp: toTinybirdDateTime(new Date()),
      run_id: runId,
      metric_value: metricValue,
    });
    expect(ingestResult.successful_rows).toBe(1);

    const rows = await waitForProcessedRows(branchToken, config.baseUrl, runId);
    expect(rows.length).toBeGreaterThan(0);
    expect(
      rows.some(
        (row) =>
          row.run_id === runId &&
          Number(row.metric_value) === metricValue &&
          Number(row.doubled_metric_value) === metricValue * 2
      )
    ).toBe(true);
  });
});
