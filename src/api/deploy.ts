/**
 * Deploy resources to Tinybird main workspace
 * Uses the /v1/deploy endpoint (same payload format as /v1/build)
 */

import type { GeneratedResources } from "../generator/index.js";
import type { BuildConfig, BuildApiResult, BuildResponse } from "./build.js";

/**
 * Deploy generated resources to Tinybird main workspace
 *
 * Uses the /v1/deploy endpoint which accepts all resources in a single
 * multipart form request. This is used for deploying to the main workspace
 * (not branches).
 *
 * @param config - Build configuration with API URL and token
 * @param resources - Generated resources to deploy
 * @returns Build result
 *
 * @example
 * ```ts
 * const result = await deployToMain(
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
 *   console.log('Deployed to main workspace!');
 * }
 * ```
 */
export async function deployToMain(
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

  // Make the request to /v1/deploy (instead of /v1/build)
  const url = `${config.baseUrl.replace(/\/$/, "")}/v1/deploy`;

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
      return body.errors
        .map((e) => {
          const prefix = e.filename ? `[${e.filename}] ` : "";
          return `${prefix}${e.error}`;
        })
        .join("\n");
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
