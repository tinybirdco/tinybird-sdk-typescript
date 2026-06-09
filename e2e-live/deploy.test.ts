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
import { pathToFileURL } from "node:url";
import { runInit } from "../src/cli/commands/init.js";
import { runDeploy } from "../src/cli/commands/deploy.js";
import { listDatasources, listPipesV1 } from "../src/api/resources.js";
import { tinybirdFetch } from "../src/api/fetcher.js";
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

const DATASOURCE_NAME = "prod_deploy_events";
const ENDPOINT_NAME = "prod_deploy_rows";

interface ProductionTinybirdClient {
  prodDeployEvents: {
    ingest: (event: {
      timestamp: string;
      run_id: string;
      metric_value: number;
    }) => Promise<unknown>;
  };
  prodDeployRows: {
    query: (
      params: Record<string, never>
    ) => Promise<{ data: Array<Record<string, unknown>> }>;
  };
}

interface DeploymentListItem {
  id: string;
  status: string;
  live?: boolean;
}

function toTinybirdDateTime(value: Date): string {
  return value.toISOString().slice(0, 19).replace("T", " ");
}

function writeProductionTestEntities(
  projectDir: string,
  options?: { requireNonNegativeMetric?: boolean }
): void {
  const tinybirdFile = path.join(projectDir, "lib", "tinybird.ts");
  const whereClause = options?.requireNonNegativeMetric
    ? "        WHERE metric_value >= 0\n"
    : "";
  const content = `import {
  defineDatasource,
  defineEndpoint,
  Tinybird,
  node,
  t,
  engine,
} from "@tinybirdco/sdk";

export const prodDeployEvents = defineDatasource("${DATASOURCE_NAME}", {
  schema: {
    timestamp: t.dateTime(),
    run_id: t.string(),
    metric_value: t.int32(),
  },
  engine: engine.mergeTree({
    sortingKey: ["run_id", "timestamp"],
  }),
});

export const prodDeployRows = defineEndpoint("${ENDPOINT_NAME}", {
  nodes: [
    node({
      name: "rows",
      sql: \`
        SELECT
          timestamp,
          run_id,
          metric_value
        FROM ${DATASOURCE_NAME}
${whereClause}        ORDER BY timestamp DESC
        LIMIT 100
      \`,
    }),
  ],
  output: {
    timestamp: t.dateTime(),
    run_id: t.string(),
    metric_value: t.int32(),
  },
});

export const tinybird = new Tinybird({
  datasources: { prodDeployEvents },
  pipes: { prodDeployRows },
});
`;

  fs.writeFileSync(tinybirdFile, content);
}

async function importTinybirdClient(projectDir: string): Promise<ProductionTinybirdClient> {
  const tinybirdFileUrl = pathToFileURL(path.join(projectDir, "lib", "tinybird.ts")).href;
  const module = (await import(
    /* @vite-ignore */ `${tinybirdFileUrl}?ts=${Date.now()}`
  )) as { tinybird: ProductionTinybirdClient };
  return module.tinybird;
}

async function waitForEndpointRows(
  tinybird: ProductionTinybirdClient,
  runId: string
): Promise<Array<Record<string, unknown>>> {
  for (let attempt = 0; attempt < 15; attempt++) {
    const result = await tinybird.prodDeployRows.query({});
    const matchingRows = result.data.filter((row) => row.run_id === runId);

    if (matchingRows.length > 0) {
      return matchingRows;
    }

    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }

  return [];
}

async function listDeployments(
  config: LiveE2EConfig,
  workspaceToken: string,
  options?: { includeDeleted?: boolean }
): Promise<DeploymentListItem[]> {
  const endpoint = new URL("/v1/deployments", config.baseUrl);
  if (options?.includeDeleted) {
    endpoint.searchParams.set("include_deleted", "true");
  }

  const response = await tinybirdFetch(endpoint.toString(), {
    headers: {
      Authorization: `Bearer ${workspaceToken}`,
    },
  });
  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(
      `Failed to list deployments: ${response.status} ${response.statusText} - ${responseText}`
    );
  }

  const payload = JSON.parse(responseText) as { deployments?: DeploymentListItem[] };
  return payload.deployments ?? [];
}

async function waitForDeploymentStatus(
  config: LiveE2EConfig,
  workspaceToken: string,
  deploymentId: string,
  expectedStatus: string
): Promise<DeploymentListItem> {
  for (let attempt = 0; attempt < 30; attempt++) {
    const deployments = await listDeployments(config, workspaceToken, { includeDeleted: true });
    const deployment = deployments.find((item) => item.id === deploymentId);

    if (deployment?.status === expectedStatus) {
      return deployment;
    }

    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }

  const deployments = await listDeployments(config, workspaceToken, { includeDeleted: true });
  const deployment = deployments.find((item) => item.id === deploymentId);
  throw new Error(
    `Timed out waiting for deployment ${deploymentId} to become ${expectedStatus}. ` +
      `Last status: ${deployment?.status ?? "missing"}`
  );
}

describeLive("E2E Live: deploy", () => {
  const config = liveConfig as LiveE2EConfig;

  let tempDir = "";
  let originalEnv: NodeJS.ProcessEnv;

  let workspaceId = "";
  let workspaceName = "";
  let workspaceToken = "";

  beforeAll(async () => {
    ensureDistBuild();
    await assertWorkspaceAdminToken(config);
    const workspace = await createWorkspaceWithToken(config, "sdk_deploy");
    workspaceId = workspace.id;
    workspaceName = workspace.name;
    workspaceToken = workspace.token;
  });

  beforeEach(() => {
    tempDir = createTempProjectDir();
    originalEnv = { ...process.env };
    process.env.GITHUB_REF_NAME = "main";
    process.env.TINYBIRD_TOKEN = workspaceToken;
    process.env.TINYBIRD_URL = config.baseUrl;
  });

  afterEach(() => {
    process.env = originalEnv;
    cleanupTempProjectDir(tempDir);
  });

  afterAll(async () => {
    if (workspaceId && workspaceName) {
      await deleteWorkspace(config, workspaceId, workspaceName);
    }
  });

  it("deploys custom TypeScript entities to production and serves ingested data", async () => {
    const initResult = await runInit({
      cwd: tempDir,
      skipLogin: true,
      devMode: "branch",
      clientPath: "lib/tinybird.ts",
    });
    expect(initResult.success).toBe(true);

    setConfigBaseUrl(tempDir, config.baseUrl);
    writeProductionTestEntities(tempDir);

    const deployResult = await runDeploy({ cwd: tempDir });
    expect(deployResult.success).toBe(true);
    expect(deployResult.deploy?.success).toBe(true);

    const [datasources, pipes] = await Promise.all([
      listDatasources({ baseUrl: config.baseUrl, token: workspaceToken }),
      listPipesV1({ baseUrl: config.baseUrl, token: workspaceToken }),
    ]);

    expect(datasources).toContain(DATASOURCE_NAME);
    expect(pipes).toContain(ENDPOINT_NAME);

    const tinybird = await importTinybirdClient(tempDir);
    const runId = `prod_deploy_${Date.now()}`;
    const metricValue = 1337;

    await tinybird.prodDeployEvents.ingest({
      timestamp: toTinybirdDateTime(new Date()),
      run_id: runId,
      metric_value: metricValue,
    });

    const rows = await waitForEndpointRows(tinybird, runId);
    expect(rows.length).toBeGreaterThan(0);
    expect(
      rows.some(
        (row) => row.run_id === runId && Number(row.metric_value) === metricValue
      )
    ).toBe(true);
  });

  it("creates a second deploy after ingestion and keeps deployment live with data preserved", async () => {
    const initResult = await runInit({
      cwd: tempDir,
      skipLogin: true,
      devMode: "branch",
      clientPath: "lib/tinybird.ts",
    });
    expect(initResult.success).toBe(true);

    setConfigBaseUrl(tempDir, config.baseUrl);
    writeProductionTestEntities(tempDir);

    const firstDeployResult = await runDeploy({ cwd: tempDir });
    expect(firstDeployResult.success).toBe(true);
    expect(firstDeployResult.deploy?.success).toBe(true);
    const firstDeploymentId = firstDeployResult.deploy?.buildId;
    expect(firstDeploymentId).toBeTruthy();

    const tinybird = await importTinybirdClient(tempDir);
    const runId = `prod_deploy_second_${Date.now()}`;
    const metricValue = 2048;

    await tinybird.prodDeployEvents.ingest({
      timestamp: toTinybirdDateTime(new Date()),
      run_id: runId,
      metric_value: metricValue,
    });

    const rowsAfterIngest = await waitForEndpointRows(tinybird, runId);
    expect(rowsAfterIngest.length).toBeGreaterThan(0);
    expect(
      rowsAfterIngest.some(
        (row) => row.run_id === runId && Number(row.metric_value) === metricValue
      )
    ).toBe(true);

    writeProductionTestEntities(tempDir, { requireNonNegativeMetric: true });

    const secondDeployResult = await runDeploy({ cwd: tempDir });
    expect(secondDeployResult.success).toBe(true);
    expect(secondDeployResult.deploy?.success).toBe(true);
    expect(secondDeployResult.deploy?.result).toBe("success");
    expect(secondDeployResult.deploy?.buildId).toBeTruthy();
    expect(secondDeployResult.deploy?.buildId).not.toBe(firstDeploymentId);

    const previousDeployment = await waitForDeploymentStatus(
      config,
      workspaceToken,
      firstDeploymentId!,
      "deleted"
    );
    expect(previousDeployment.live).not.toBe(true);

    const rowsAfterSecondDeploy = await waitForEndpointRows(tinybird, runId);
    expect(rowsAfterSecondDeploy.length).toBeGreaterThan(0);
    expect(
      rowsAfterSecondDeploy.some(
        (row) => row.run_id === runId && Number(row.metric_value) === metricValue
      )
    ).toBe(true);
  });
});
