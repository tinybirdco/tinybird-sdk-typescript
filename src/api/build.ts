/**
 * Build and deploy resources to Tinybird API
 * Uses the /v1/build endpoint to deploy all resources at once
 */

import type { GeneratedResources } from "../generator/index.js";

/**
 * Configuration for building/deploying to Tinybird
 */
export interface BuildConfig {
  /** Tinybird API base URL */
  baseUrl: string;
  /** API token for authentication */
  token: string;
}

/**
 * Resource info in the build response
 */
export interface ResourceInfo {
  name: string;
  type: string;
}

/**
 * Error details from the build endpoint
 */
export interface BuildError {
  filename?: string;
  type?: string;
  error: string;
}

/**
 * Build response from the /v1/build endpoint
 */
export interface BuildResponse {
  /** Result status */
  result: "success" | "failed" | "no_changes";
  /** Error message if failed (simple error) */
  error?: string;
  /** Array of errors if multiple (validation errors) */
  errors?: BuildError[];
  /** Build details */
  build?: {
    id: string;
    datasources?: ResourceInfo[];
    pipes?: ResourceInfo[];
    /** Names of pipes that were changed in this build */
    changed_pipe_names?: string[];
    /** Names of newly created pipes in this build */
    new_pipe_names?: string[];
    /** Names of pipes that were deleted in this build */
    deleted_pipe_names?: string[];
    /** Names of datasources that were changed in this build */
    changed_datasource_names?: string[];
    /** Names of newly created datasources in this build */
    new_datasource_names?: string[];
    /** Names of datasources that were deleted in this build */
    deleted_datasource_names?: string[];
  };
}

/**
 * Resource changes in a build
 */
export interface ResourceChanges {
  /** Names of resources that were changed */
  changed: string[];
  /** Names of newly created resources */
  created: string[];
  /** Names of resources that were deleted */
  deleted: string[];
}

/**
 * Build result with additional metadata
 */
export interface BuildApiResult {
  /** Whether the build was successful */
  success: boolean;
  /** Result status from API */
  result: "success" | "failed" | "no_changes";
  /** Error message if failed */
  error?: string;
  /** Number of datasources deployed */
  datasourceCount: number;
  /** Number of pipes deployed */
  pipeCount: number;
  /** Number of connections deployed */
  connectionCount: number;
  /** Build ID if successful */
  buildId?: string;
  /** Pipe changes in this build */
  pipes?: ResourceChanges;
  /** Datasource changes in this build */
  datasources?: ResourceChanges;
  /** @deprecated Use pipes.changed instead */
  changedPipeNames?: string[];
  /** @deprecated Use pipes.created instead */
  newPipeNames?: string[];
}

/**
 * Build and deploy generated resources to Tinybird API
 *
 * Uses the /v1/build endpoint which accepts all resources in a single
 * multipart form request.
 *
 * @param config - Build configuration with API URL and token
 * @param resources - Generated resources to deploy
 * @returns Build result
 *
 * @example
 * ```ts
 * const result = await buildToTinybird(
 *   {
 *     baseUrl: 'https://api.tinybird.co',
 *     token: 'p.xxx',
 *   },
 *   {
 *     datasources: [{ name: 'events', content: '...' }],
 *     pipes: [{ name: 'top_events', content: '...' }],
 *   }
 * );
 *
 * if (result.success) {
 *   console.log('Build deployed successfully!');
 * }
 * ```
 */
export async function buildToTinybird(
  config: BuildConfig,
  resources: GeneratedResources,
  options?: { debug?: boolean }
): Promise<BuildApiResult> {
  const debug = options?.debug ?? !!process.env.TINYBIRD_DEBUG;
  const formData = new FormData();

  // Add datasources
  for (const ds of resources.datasources) {
    const fieldName = `data_project://`;
    const fileName = `${ds.name}.datasource`;
    if (debug) {
      console.log(`[debug] Adding datasource: ${fieldName} (filename: ${fileName})`);
      console.log(`[debug] Content:\n${ds.content}\n`);
    }
    formData.append(
      fieldName,
      new Blob([ds.content], { type: "text/plain" }),
      fileName
    );
  }

  // Add pipes
  for (const pipe of resources.pipes) {
    const fieldName = `data_project://`;
    const fileName = `${pipe.name}.pipe`;
    if (debug) {
      console.log(`[debug] Adding pipe: ${fieldName} (filename: ${fileName})`);
      console.log(`[debug] Content:\n${pipe.content}\n`);
    }
    formData.append(
      fieldName,
      new Blob([pipe.content], { type: "text/plain" }),
      fileName
    );
  }

  // Add connections
  for (const conn of resources.connections ?? []) {
    const fieldName = `data_project://`;
    const fileName = `${conn.name}.connection`;
    if (debug) {
      console.log(`[debug] Adding connection: ${fieldName} (filename: ${fileName})`);
      console.log(`[debug] Content:\n${conn.content}\n`);
    }
    formData.append(
      fieldName,
      new Blob([conn.content], { type: "text/plain" }),
      fileName
    );
  }

  // Make the request
  const url = `${config.baseUrl.replace(/\/$/, "")}/v1/build`;

  if (debug) {
    console.log(`[debug] POST ${url}`);
  }

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
    body: formData,
  });

  // Parse response
  let body: BuildResponse;
  const rawBody = await response.text();

  if (debug) {
    console.log(`[debug] Response status: ${response.status}`);
    console.log(`[debug] Response body: ${rawBody}`);
  }

  try {
    body = JSON.parse(rawBody) as BuildResponse;
  } catch {
    throw new Error(
      `Failed to parse response from Tinybird API: ${response.status} ${response.statusText}\nBody: ${rawBody}`
    );
  }

  // Helper to format errors
  const formatErrors = (): string => {
    if (body.errors && body.errors.length > 0) {
      return body.errors.map(e => {
        const prefix = e.filename ? `[${e.filename}] ` : '';
        return `${prefix}${e.error}`;
      }).join('\n');
    }
    return body.error || `HTTP ${response.status}: ${response.statusText}`;
  };

  // Handle non-OK responses
  if (!response.ok) {
    return {
      success: false,
      result: "failed",
      error: formatErrors(),
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections?.length ?? 0,
    };
  }

  // Handle API result
  if (body.result === "failed") {
    return {
      success: false,
      result: "failed",
      error: formatErrors(),
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections?.length ?? 0,
    };
  }

  return {
    success: true,
    result: body.result,
    datasourceCount: resources.datasources.length,
    pipeCount: resources.pipes.length,
    connectionCount: resources.connections?.length ?? 0,
    buildId: body.build?.id,
    pipes: {
      changed: body.build?.changed_pipe_names ?? [],
      created: body.build?.new_pipe_names ?? [],
      deleted: body.build?.deleted_pipe_names ?? [],
    },
    datasources: {
      changed: body.build?.changed_datasource_names ?? [],
      created: body.build?.new_datasource_names ?? [],
      deleted: body.build?.deleted_datasource_names ?? [],
    },
    // Keep deprecated fields for backwards compatibility
    changedPipeNames: body.build?.changed_pipe_names ?? [],
    newPipeNames: body.build?.new_pipe_names ?? [],
  };
}

/**
 * Validate that the configuration is complete
 */
export function validateBuildConfig(config: Partial<BuildConfig>): asserts config is BuildConfig {
  if (!config.baseUrl) {
    throw new Error("Missing baseUrl in configuration");
  }
  if (!config.token) {
    throw new Error("Missing token in configuration");
  }
}
