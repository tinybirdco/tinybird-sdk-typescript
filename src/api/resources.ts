/**
 * Tinybird Resources API client
 * Functions to list and fetch resources from a workspace
 */

import type { WorkspaceApiConfig } from "./workspaces.js";
import { tinybirdFetch } from "./fetcher.js";

/**
 * Error thrown by resource API operations
 */
export class ResourceApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly endpoint: string,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "ResourceApiError";
  }
}

// ============ Datasource Types ============

/**
 * Column information from Tinybird API
 */
export interface DatasourceColumn {
  /** Column name */
  name: string;
  /** ClickHouse type (e.g., "String", "DateTime", "Nullable(String)") */
  type: string;
  /** JSON path for JSON extraction */
  jsonpath?: string;
  /** Default value expression */
  default?: string;
  /** Codec for compression */
  codec?: string;
}

/**
 * Engine information from Tinybird API
 */
export interface DatasourceEngine {
  /** Engine type (e.g., "MergeTree", "ReplacingMergeTree") */
  type: string;
  /** Sorting key columns */
  sorting_key?: string;
  /** Partition key expression */
  partition_key?: string;
  /** Primary key columns */
  primary_key?: string;
  /** TTL expression */
  ttl?: string;
  /** Version column (ReplacingMergeTree) */
  ver?: string;
  /** Sign column (CollapsingMergeTree) */
  sign?: string;
  /** Version column (VersionedCollapsingMergeTree) */
  version?: string;
  /** Summing columns (SummingMergeTree) */
  summing_columns?: string;
}

/**
 * Full datasource information from Tinybird API
 */
export interface DatasourceInfo {
  /** Datasource name */
  name: string;
  /** Human-readable description */
  description?: string;
  /** Column definitions */
  columns: DatasourceColumn[];
  /** Engine configuration */
  engine: DatasourceEngine;
  /** Forward query for schema evolution */
  forward_query?: string;
}

/**
 * Datasource list item from /v0/datasources
 */
interface DatasourceListItem {
  name: string;
  description?: string;
}

/**
 * Column definition from API response
 */
interface ColumnResponse {
  name: string;
  type: string;
  jsonpath?: string;
  default_value?: string;
  codec?: string;
  nullable?: boolean;
}

/**
 * Engine object from API response
 * Note: The detail endpoint uses different property names than the list endpoint
 */
interface EngineResponse {
  engine?: string;
  // Detail endpoint uses these names
  sorting_key?: string;
  partition_key?: string;
  primary_key?: string;
  // List endpoint uses these names
  engine_sorting_key?: string;
  engine_partition_key?: string;
  engine_primary_key?: string;
  // Other engine properties
  engine_ver?: string;
  engine_sign?: string;
  engine_version?: string;
  engine_summing_columns?: string;
}

/**
 * Datasource detail response from /v0/datasources/{name}
 */
interface DatasourceDetailResponse {
  name: string;
  description?: string;
  // Detail endpoint has columns under schema.columns
  schema?: {
    columns?: ColumnResponse[];
  };
  // List endpoint has columns at top level
  columns?: ColumnResponse[];
  engine?: EngineResponse;
  sorting_key?: string;
  partition_key?: string;
  primary_key?: string;
  ttl?: string;
  forward_query?: string;
}

// ============ Pipe Types ============

/**
 * Node information from a pipe
 */
export interface PipeNode {
  /** Node name */
  name: string;
  /** SQL query */
  sql: string;
  /** Node description */
  description?: string;
}

/**
 * Parameter information from a pipe
 */
export interface PipeParam {
  /** Parameter name */
  name: string;
  /** ClickHouse type */
  type: string;
  /** Default value */
  default?: string | number;
  /** Whether the parameter is required */
  required: boolean;
  /** Parameter description */
  description?: string;
}

/**
 * Pipe type classification
 */
export type PipeType = "endpoint" | "materialized" | "copy" | "pipe";

/**
 * Full pipe information from Tinybird API
 */
export interface PipeInfo {
  /** Pipe name */
  name: string;
  /** Human-readable description */
  description?: string;
  /** Nodes in the pipe */
  nodes: PipeNode[];
  /** Query parameters */
  params: PipeParam[];
  /** Pipe type */
  type: PipeType;
  /** Endpoint configuration (if type is endpoint) */
  endpoint?: {
    enabled: boolean;
    cache?: { enabled: boolean; ttl?: number };
  };
  /** Materialized view configuration (if type is materialized) */
  materialized?: {
    datasource: string;
  };
  /** Copy pipe configuration (if type is copy) */
  copy?: {
    target_datasource: string;
    copy_schedule?: string;
    copy_mode?: "append" | "replace";
  };
  /** Output column schema (for endpoints) */
  output_columns: DatasourceColumn[];
}

/**
 * Pipe list item from /v0/pipes
 */
interface PipeListItem {
  name: string;
  description?: string;
  type?: string;
  endpoint?: string;
}

/**
 * Pipe detail response from /v0/pipes/{name}
 */
interface PipeDetailResponse {
  name: string;
  description?: string;
  nodes?: Array<{
    name: string;
    sql: string;
    description?: string;
    params?: Array<{
      name: string;
      type: string;
      default?: string | number;
      required?: boolean;
      description?: string;
    }>;
    columns?: Array<{
      name: string;
      type: string;
    }>;
  }>;
  type?: string;
  endpoint?: string;
  copy_target_datasource?: string;
  copy_schedule?: string;
  copy_mode?: string;
  materialized_datasource?: string;
}

// ============ Connector/Datafile Types ============

/**
 * Resource file type returned by pull operations
 */
export type ResourceFileType = "datasource" | "pipe" | "connection";

/**
 * Raw Tinybird datafile pulled from API
 */
export interface ResourceFile {
  /** Resource name (without extension) */
  name: string;
  /** Resource kind */
  type: ResourceFileType;
  /** Filename with extension */
  filename: string;
  /** Raw datafile content */
  content: string;
}

/**
 * Grouped resource files returned by pull operations
 */
export interface PulledResourceFiles {
  datasources: ResourceFile[];
  pipes: ResourceFile[];
  connections: ResourceFile[];
}

// ============ API Helper ============

/**
 * Handle API response and throw appropriate errors
 */
async function handleApiResponse<T>(
  response: Response,
  endpoint: string
): Promise<T> {
  if (response.status === 401) {
    throw new ResourceApiError(
      "Invalid or expired token",
      401,
      endpoint
    );
  }
  if (response.status === 403) {
    throw new ResourceApiError(
      "Insufficient permissions to access resources",
      403,
      endpoint
    );
  }
  if (response.status === 404) {
    throw new ResourceApiError(
      "Resource not found",
      404,
      endpoint
    );
  }
  if (!response.ok) {
    const body = await response.text();
    throw new ResourceApiError(
      `API request failed: ${response.status} ${response.statusText}`,
      response.status,
      endpoint,
      body
    );
  }
  return response.json() as Promise<T>;
}

/**
 * Handle API text response and throw appropriate errors
 */
async function handleApiTextResponse(
  response: Response,
  endpoint: string
): Promise<string> {
  if (response.status === 401) {
    throw new ResourceApiError("Invalid or expired token", 401, endpoint);
  }
  if (response.status === 403) {
    throw new ResourceApiError(
      "Insufficient permissions to access resources",
      403,
      endpoint
    );
  }
  if (response.status === 404) {
    throw new ResourceApiError("Resource not found", 404, endpoint);
  }
  if (!response.ok) {
    const body = await response.text();
    throw new ResourceApiError(
      `API request failed: ${response.status} ${response.statusText}`,
      response.status,
      endpoint,
      body
    );
  }
  return response.text();
}

/**
 * Extract resource names from API response arrays
 */
function extractNames(
  data: Record<string, unknown>,
  keys: string[]
): string[] {
  for (const key of keys) {
    const value = data[key];
    if (!Array.isArray(value)) {
      continue;
    }

    const names = value
      .map((item) => {
        if (typeof item === "string") {
          return item;
        }
        if (
          typeof item === "object" &&
          item !== null &&
          "name" in item &&
          typeof (item as { name: unknown }).name === "string"
        ) {
          return (item as { name: string }).name;
        }
        return null;
      })
      .filter((name): name is string => name !== null);

    return names;
  }

  return [];
}

/**
 * Fetch text resource from the first successful endpoint.
 * Falls back on 404 responses.
 */
async function fetchTextFromAnyEndpoint(
  config: WorkspaceApiConfig,
  endpoints: string[]
): Promise<string> {
  let lastNotFound: ResourceApiError | null = null;

  for (const endpoint of endpoints) {
    const url = new URL(endpoint, config.baseUrl);
    const response = await tinybirdFetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${config.token}`,
      },
    });

    if (response.status === 404) {
      lastNotFound = new ResourceApiError("Resource not found", 404, endpoint);
      continue;
    }

    return handleApiTextResponse(response, endpoint);
  }

  throw (
    lastNotFound ??
    new ResourceApiError("Resource not found", 404, endpoints[0] ?? "unknown")
  );
}

// ============ Datasource API ============

/**
 * List all datasources in the workspace
 *
 * @param config - API configuration
 * @returns Array of datasource names
 */
export async function listDatasources(
  config: WorkspaceApiConfig
): Promise<string[]> {
  const url = new URL("/v0/datasources", config.baseUrl);

  const response = await tinybirdFetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  const data = await handleApiResponse<{ datasources: DatasourceListItem[] }>(
    response,
    "/v0/datasources"
  );

  return data.datasources.map((ds) => ds.name);
}

/**
 * Get detailed information about a specific datasource
 *
 * @param config - API configuration
 * @param name - Datasource name
 * @returns Datasource information including schema and engine
 */
export async function getDatasource(
  config: WorkspaceApiConfig,
  name: string
): Promise<DatasourceInfo> {
  const url = new URL(`/v0/datasources/${encodeURIComponent(name)}`, config.baseUrl);

  const response = await tinybirdFetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  const data = await handleApiResponse<DatasourceDetailResponse>(
    response,
    `/v0/datasources/${name}`
  );

  // Extract columns from either schema.columns (detail) or columns (list)
  const rawColumns = data.schema?.columns ?? data.columns ?? [];

  // Extract engine info from the engine object
  const engineObj = data.engine;
  const engineType = parseEngineType(engineObj?.engine);

  // Engine properties can be in different places depending on the endpoint
  const sortingKey =
    engineObj?.sorting_key ?? engineObj?.engine_sorting_key ?? data.sorting_key;
  const partitionKey =
    engineObj?.partition_key ?? engineObj?.engine_partition_key ?? data.partition_key;
  const primaryKey =
    engineObj?.primary_key ?? engineObj?.engine_primary_key ?? data.primary_key;

  return {
    name: data.name,
    description: data.description,
    columns: rawColumns.map((col) => ({
      name: col.name,
      type: col.type,
      jsonpath: col.jsonpath,
      default: col.default_value,
      codec: col.codec,
    })),
    engine: {
      type: engineType,
      sorting_key: sortingKey,
      partition_key: partitionKey,
      primary_key: primaryKey,
      ttl: data.ttl,
      ver: engineObj?.engine_ver,
      sign: engineObj?.engine_sign,
      version: engineObj?.engine_version,
      summing_columns: engineObj?.engine_summing_columns,
    },
    forward_query: data.forward_query,
  };
}

/**
 * Parse engine type from engine string
 */
function parseEngineType(engineString?: string): string {
  if (!engineString) {
    return "MergeTree";
  }

  // Engine string might be like "MergeTree" or "ReplacingMergeTree(version_column)"
  const match = engineString.match(/^(\w+)/);
  return match ? match[1] : "MergeTree";
}

// ============ Pipe API ============

/**
 * List all pipes in the workspace
 *
 * @param config - API configuration
 * @returns Array of pipe names
 */
export async function listPipes(
  config: WorkspaceApiConfig
): Promise<string[]> {
  const url = new URL("/v0/pipes", config.baseUrl);

  const response = await tinybirdFetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  const data = await handleApiResponse<{ pipes: PipeListItem[] }>(
    response,
    "/v0/pipes"
  );

  return data.pipes.map((p) => p.name);
}

/**
 * List all pipes from the v1 endpoint.
 * Falls back to v0 when v1 is unavailable.
 *
 * @param config - API configuration
 * @returns Array of pipe names
 */
export async function listPipesV1(config: WorkspaceApiConfig): Promise<string[]> {
  const endpoint = "/v1/pipes";
  const url = new URL(endpoint, config.baseUrl);

  const response = await tinybirdFetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  // Older/self-hosted versions may not expose /v1/pipes.
  if (response.status === 404) {
    return listPipes(config);
  }

  const data = await handleApiResponse<Record<string, unknown>>(response, endpoint);
  return extractNames(data, ["pipes", "data"]);
}

/**
 * Get a datasource as native .datasource text
 *
 * @param config - API configuration
 * @param name - Datasource name
 * @returns Raw .datasource content
 */
export async function getDatasourceFile(
  config: WorkspaceApiConfig,
  name: string
): Promise<string> {
  const encoded = encodeURIComponent(name);
  return fetchTextFromAnyEndpoint(config, [
    `/v0/datasources/${encoded}.datasource`,
    `/v0/datasources/${encoded}?format=datasource`,
  ]);
}

/**
 * Get a pipe as native .pipe text
 *
 * @param config - API configuration
 * @param name - Pipe name
 * @returns Raw .pipe content
 */
export async function getPipeFile(
  config: WorkspaceApiConfig,
  name: string
): Promise<string> {
  const encoded = encodeURIComponent(name);
  return fetchTextFromAnyEndpoint(config, [
    `/v1/pipes/${encoded}.pipe`,
    `/v0/pipes/${encoded}.pipe`,
    `/v1/pipes/${encoded}?format=pipe`,
    `/v0/pipes/${encoded}?format=pipe`,
  ]);
}

/**
 * List all connectors in the workspace
 *
 * @param config - API configuration
 * @returns Array of connector names
 */
export async function listConnectors(
  config: WorkspaceApiConfig
): Promise<string[]> {
  const endpoint = "/v0/connectors";
  const url = new URL(endpoint, config.baseUrl);

  const response = await tinybirdFetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  // Not all workspaces expose connectors. Treat missing endpoint as no connectors.
  if (response.status === 404) {
    return [];
  }

  const data = await handleApiResponse<Record<string, unknown>>(response, endpoint);
  return extractNames(data, ["connectors", "connections"]);
}

/**
 * Get a connector as native .connection text
 *
 * @param config - API configuration
 * @param name - Connector name
 * @returns Raw .connection content
 */
export async function getConnectorFile(
  config: WorkspaceApiConfig,
  name: string
): Promise<string> {
  const encoded = encodeURIComponent(name);
  return fetchTextFromAnyEndpoint(config, [
    `/v0/connectors/${encoded}.connection`,
    `/v0/connectors/${encoded}?format=connection`,
  ]);
}

/**
 * Get detailed information about a specific pipe
 *
 * @param config - API configuration
 * @param name - Pipe name
 * @returns Pipe information including nodes, params, and output schema
 */
export async function getPipe(
  config: WorkspaceApiConfig,
  name: string
): Promise<PipeInfo> {
  const url = new URL(`/v0/pipes/${encodeURIComponent(name)}`, config.baseUrl);

  const response = await tinybirdFetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  const data = await handleApiResponse<PipeDetailResponse>(
    response,
    `/v0/pipes/${name}`
  );

  // Determine pipe type
  let pipeType: PipeType = "pipe";
  if (data.endpoint) {
    pipeType = "endpoint";
  } else if (data.materialized_datasource) {
    pipeType = "materialized";
  } else if (data.copy_target_datasource) {
    pipeType = "copy";
  }

  // Extract nodes
  const nodes: PipeNode[] = (data.nodes ?? []).map((node) => ({
    name: node.name,
    sql: node.sql,
    description: node.description,
  }));

  // Extract params from all nodes (they're typically on the first node)
  const params: PipeParam[] = [];
  const seenParams = new Set<string>();
  for (const node of data.nodes ?? []) {
    for (const param of node.params ?? []) {
      if (!seenParams.has(param.name)) {
        seenParams.add(param.name);
        params.push({
          name: param.name,
          type: param.type,
          default: param.default,
          required: param.required ?? true,
          description: param.description,
        });
      }
    }
  }

  // Extract output columns from the last node
  const lastNode = data.nodes?.[data.nodes.length - 1];
  const outputColumns: DatasourceColumn[] = (lastNode?.columns ?? []).map((col) => ({
    name: col.name,
    type: col.type,
  }));

  return {
    name: data.name,
    description: data.description,
    nodes,
    params,
    type: pipeType,
    endpoint: pipeType === "endpoint" ? { enabled: true } : undefined,
    materialized: data.materialized_datasource
      ? { datasource: data.materialized_datasource }
      : undefined,
    copy: data.copy_target_datasource
      ? {
          target_datasource: data.copy_target_datasource,
          copy_schedule: data.copy_schedule,
          copy_mode: data.copy_mode as "append" | "replace" | undefined,
        }
      : undefined,
    output_columns: outputColumns,
  };
}

// ============ Convenience Functions ============

/**
 * Fetch all resources from a workspace
 *
 * @param config - API configuration
 * @returns All datasources and pipes with full details
 */
export async function fetchAllResources(
  config: WorkspaceApiConfig
): Promise<{
  datasources: DatasourceInfo[];
  pipes: PipeInfo[];
}> {
  // List all resources first
  const [datasourceNames, pipeNames] = await Promise.all([
    listDatasources(config),
    listPipes(config),
  ]);

  // Fetch details in parallel
  const [datasources, pipes] = await Promise.all([
    Promise.all(datasourceNames.map((name) => getDatasource(config, name))),
    Promise.all(pipeNames.map((name) => getPipe(config, name))),
  ]);

  return { datasources, pipes };
}

/**
 * Pull all datasource/pipe/connector datafiles from a workspace
 *
 * @param config - API configuration
 * @returns Raw resource files grouped by type
 */
export async function pullAllResourceFiles(
  config: WorkspaceApiConfig
): Promise<PulledResourceFiles> {
  const [datasourceNames, pipeNames, connectorNames] = await Promise.all([
    listDatasources(config),
    listPipesV1(config),
    listConnectors(config),
  ]);

  const [datasources, pipes, connections] = await Promise.all([
    Promise.all(
      datasourceNames.map(async (name): Promise<ResourceFile> => ({
        name,
        type: "datasource",
        filename: `${name}.datasource`,
        content: await getDatasourceFile(config, name),
      }))
    ),
    Promise.all(
      pipeNames.map(async (name): Promise<ResourceFile> => ({
        name,
        type: "pipe",
        filename: `${name}.pipe`,
        content: await getPipeFile(config, name),
      }))
    ),
    Promise.all(
      connectorNames.map(async (name): Promise<ResourceFile> => ({
        name,
        type: "connection",
        filename: `${name}.connection`,
        content: await getConnectorFile(config, name),
      }))
    ),
  ]);

  return {
    datasources,
    pipes,
    connections,
  };
}

/**
 * Check if a workspace has any resources
 *
 * @param config - API configuration
 * @returns True if the workspace has at least one datasource or pipe
 */
export async function hasResources(
  config: WorkspaceApiConfig
): Promise<boolean> {
  const [datasourceNames, pipeNames] = await Promise.all([
    listDatasources(config),
    listPipes(config),
  ]);

  return datasourceNames.length > 0 || pipeNames.length > 0;
}
