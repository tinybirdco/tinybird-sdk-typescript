/**
 * Push resources to Tinybird API
 * Uses the /v1/build endpoint to deploy all resources at once
 */

import type { GeneratedResources } from "../generator/index.js";

/**
 * Configuration for pushing to Tinybird
 */
export interface PushConfig {
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
  };
}

/**
 * Push result with additional metadata
 */
export interface PushResult {
  /** Whether the push was successful */
  success: boolean;
  /** Result status from API */
  result: "success" | "failed" | "no_changes";
  /** Error message if failed */
  error?: string;
  /** Number of datasources pushed */
  datasourceCount: number;
  /** Number of pipes pushed */
  pipeCount: number;
  /** Build ID if successful */
  buildId?: string;
}

/**
 * Push generated resources to Tinybird API
 *
 * Uses the /v1/build endpoint which accepts all resources in a single
 * multipart form request.
 *
 * @param config - Push configuration with API URL and token
 * @param resources - Generated resources to push
 * @returns Push result
 *
 * @example
 * ```ts
 * const result = await pushToTinybird(
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
 *   console.log('Pushed successfully!');
 * }
 * ```
 */
export async function pushToTinybird(
  config: PushConfig,
  resources: GeneratedResources,
  options?: { debug?: boolean }
): Promise<PushResult> {
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
    };
  }

  return {
    success: true,
    result: body.result,
    datasourceCount: resources.datasources.length,
    pipeCount: resources.pipes.length,
    buildId: body.build?.id,
  };
}

/**
 * Validate that the configuration is complete
 */
export function validatePushConfig(config: Partial<PushConfig>): config is PushConfig {
  if (!config.baseUrl) {
    throw new Error("Missing baseUrl in configuration");
  }
  if (!config.token) {
    throw new Error("Missing token in configuration");
  }
  return true;
}
