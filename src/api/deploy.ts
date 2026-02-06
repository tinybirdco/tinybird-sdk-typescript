/**
 * Deploy resources to Tinybird main workspace
 * Uses the /v1/deploy endpoint to create a deployment, then sets it live
 */

import type { GeneratedResources } from "../generator/index.js";
import type { BuildConfig, BuildApiResult } from "./build.js";
import { tinybirdFetch } from "./fetcher.js";

/**
 * Feedback item from deployment response
 */
export interface DeploymentFeedback {
  resource: string;
  level: "ERROR" | "WARNING" | "INFO";
  message: string;
}

/**
 * Deployment object returned by the /v1/deploy endpoint
 */
export interface Deployment {
  id: string;
  status: string;
  live?: boolean;
  created_at?: string;
  updated_at?: string;
  feedback?: DeploymentFeedback[];
}

/**
 * Response from /v1/deployments list endpoint
 */
export interface DeploymentsListResponse {
  deployments: Deployment[];
}

/**
 * Response from /v1/deploy endpoint
 */
export interface DeployResponse {
  result: "success" | "failed" | "no_changes";
  deployment?: DeploymentDetails;
  error?: string;
  errors?: Array<{ filename?: string; error: string }>;
}

/**
 * Detailed deployment information with resource changes
 */
export interface DeploymentDetails extends Deployment {
  /** Names of newly created datasources */
  new_datasource_names?: string[];
  /** Names of changed datasources */
  changed_datasource_names?: string[];
  /** Names of deleted datasources */
  deleted_datasource_names?: string[];
  /** Names of newly created pipes */
  new_pipe_names?: string[];
  /** Names of changed pipes */
  changed_pipe_names?: string[];
  /** Names of deleted pipes */
  deleted_pipe_names?: string[];
  /** Names of newly created connections */
  new_data_connector_names?: string[];
  /** Names of changed connections */
  changed_data_connector_names?: string[];
  /** Names of deleted connections */
  deleted_data_connector_names?: string[];
  /** Deployment errors */
  errors?: Array<{ filename?: string; error: string }>;
  /** Deployment feedback/warnings */
  feedback?: Array<{ level: string; resource?: string; message: string }>;
}

/**
 * Response from /v1/deployments/{id} endpoint
 */
export interface DeploymentStatusResponse {
  result: string;
  deployment: Deployment;
}

/**
 * Deploy generated resources to Tinybird main workspace
 *
 * Uses the /v1/deploy endpoint which accepts all resources in a single
 * multipart form request. After creating the deployment, this function:
 * 1. Polls until the deployment is ready (status === 'data_ready')
 * 2. Sets the deployment as live via /v1/deployments/{id}/set-live
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
/**
 * Callbacks for deploy progress updates
 */
export interface DeployCallbacks {
  /** Called when waiting for deployment to be ready */
  onWaitingForReady?: () => void;
  /** Called when deployment is ready */
  onDeploymentReady?: () => void;
  /** Called when waiting for deployment to be promoted */
  onWaitingForPromote?: () => void;
  /** Called when deployment is promoted */
  onDeploymentPromoted?: () => void;
  /** Called when deployment is live */
  onDeploymentLive?: (deploymentId: string) => void;
  /** Called when validating deployment (check mode) */
  onValidating?: () => void;
}

export async function deployToMain(
  config: BuildConfig,
  resources: GeneratedResources,
  options?: {
    debug?: boolean;
    pollIntervalMs?: number;
    maxPollAttempts?: number;
    check?: boolean;
    allowDestructiveOperations?: boolean;
    callbacks?: DeployCallbacks;
  }
): Promise<BuildApiResult> {
  const debug = options?.debug ?? !!process.env.TINYBIRD_DEBUG;
  const pollIntervalMs = options?.pollIntervalMs ?? 1000;
  const maxPollAttempts = options?.maxPollAttempts ?? 120; // 2 minutes max
  const baseUrl = config.baseUrl.replace(/\/$/, "");

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

  // Step 0: Clean up any stale non-live deployments that might block the new deployment
  try {
    const deploymentsUrl = `${baseUrl}/v1/deployments`;
    const deploymentsResponse = await tinybirdFetch(deploymentsUrl, {
      headers: {
        Authorization: `Bearer ${config.token}`,
      },
    });

    if (deploymentsResponse.ok) {
      const deploymentsBody = (await deploymentsResponse.json()) as DeploymentsListResponse;
      const staleDeployments = deploymentsBody.deployments.filter(
        (d) => !d.live && d.status !== "live"
      );

      for (const stale of staleDeployments) {
        if (debug) {
          console.log(`[debug] Cleaning up stale deployment: ${stale.id} (status: ${stale.status})`);
        }
        await tinybirdFetch(`${baseUrl}/v1/deployments/${stale.id}`, {
          method: "DELETE",
          headers: {
            Authorization: `Bearer ${config.token}`,
          },
        });
      }
    }
  } catch (e) {
    // Ignore errors during cleanup - we'll try to deploy anyway
    if (debug) {
      console.log(`[debug] Failed to clean up stale deployments: ${e}`);
    }
  }

  // Step 1: Create deployment via /v1/deploy
  const deployUrlBase = `${baseUrl}/v1/deploy`;
  const urlParams = new URLSearchParams();
  if (options?.check) {
    urlParams.set("check", "true");
  }
  if (options?.allowDestructiveOperations) {
    urlParams.set("allow_destructive_operations", "true");
  }
  const queryString = urlParams.toString();
  const deployUrl = queryString ? `${deployUrlBase}?${queryString}` : deployUrlBase;

  if (debug) {
    console.log(`[debug] POST ${deployUrl}`);
  }

  const response = await tinybirdFetch(deployUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
    body: formData,
  });

  // Parse response
  let body: DeployResponse;
  const rawBody = await response.text();

  if (debug) {
    console.log(`[debug] Response status: ${response.status}`);
    console.log(`[debug] Response body: ${rawBody}`);
  }

  try {
    body = JSON.parse(rawBody) as DeployResponse;
  } catch {
    throw new Error(
      `Failed to parse response from Tinybird API: ${response.status} ${response.statusText}\nBody: ${rawBody}`
    );
  }

  // Helper to format feedback from deployment
  const formatFeedback = (feedback: DeploymentFeedback[]): string => {
    return feedback
      .map((f) => {
        // Extract just the filename from "Datasource events.datasource" format
        const resourceName = f.resource.split(" ").pop() ?? f.resource;
        return `${resourceName}: ${f.message}`;
      })
      .join("\n");
  };

  // Helper to format errors
  const formatErrors = (): string => {
    // Check for feedback in deployment (most detailed error info)
    if (body.deployment?.feedback && body.deployment.feedback.length > 0) {
      const errors = body.deployment.feedback.filter((f) => f.level === "ERROR");
      if (errors.length > 0) {
        return formatFeedback(errors);
      }
    }
    if (body.errors && body.errors.length > 0) {
      return body.errors
        .map((e) => {
          const prefix = e.filename ? `[${e.filename}] ` : "";
          return `${prefix}${e.error}`;
        })
        .join("\n");
    }
    if (body.error) {
      return body.error;
    }
    // Include raw response body for debugging when no structured error is available
    return `HTTP ${response.status}: ${response.statusText}\nResponse: ${rawBody}`;
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

  if (options?.check) {
    options.callbacks?.onValidating?.();

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
      result: body.result ?? "success",
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections?.length ?? 0,
    };
  }

  // Handle no changes - no deployment was created, so we can return early
  if (body.result === "no_changes") {
    return {
      success: true,
      result: "no_changes",
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections?.length ?? 0,
      pipes: {
        changed: [],
        created: [],
        deleted: [],
      },
      datasources: {
        changed: [],
        created: [],
        deleted: [],
      },
    };
  }

  // Handle API result
  if (body.result === "failed" || !body.deployment) {
    return {
      success: false,
      result: "failed",
      error: formatErrors(),
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections?.length ?? 0,
    };
  }

  const deploymentId = body.deployment.id;

  if (debug) {
    console.log(`[debug] Deployment created with ID: ${deploymentId}`);
  }

  // Step 2: Poll until deployment is ready
  let deployment = body.deployment;
  let attempts = 0;

  options?.callbacks?.onWaitingForReady?.();

  while (deployment.status !== "data_ready" && attempts < maxPollAttempts) {
    await sleep(pollIntervalMs);
    attempts++;

    if (debug) {
      console.log(`[debug] Polling deployment status (attempt ${attempts})...`);
    }

    const statusUrl = `${baseUrl}/v1/deployments/${deploymentId}`;
    const statusResponse = await tinybirdFetch(statusUrl, {
      headers: {
        Authorization: `Bearer ${config.token}`,
      },
    });

    if (!statusResponse.ok) {
      return {
        success: false,
        result: "failed",
        error: `Failed to check deployment status: ${statusResponse.status} ${statusResponse.statusText}`,
        datasourceCount: resources.datasources.length,
        pipeCount: resources.pipes.length,
        connectionCount: resources.connections?.length ?? 0,
        buildId: deploymentId,
      };
    }

    const statusBody = (await statusResponse.json()) as DeploymentStatusResponse;
    deployment = statusBody.deployment;

    if (debug) {
      console.log(`[debug] Deployment status: ${deployment.status}`);
    }

    // Check for failed status
    if (deployment.status === "failed" || deployment.status === "error") {
      return {
        success: false,
        result: "failed",
        error: `Deployment failed with status: ${deployment.status}`,
        datasourceCount: resources.datasources.length,
        pipeCount: resources.pipes.length,
        connectionCount: resources.connections?.length ?? 0,
        buildId: deploymentId,
      };
    }
  }

  if (deployment.status !== "data_ready") {
    return {
      success: false,
      result: "failed",
      error: `Deployment timed out after ${maxPollAttempts} attempts. Last status: ${deployment.status}`,
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections?.length ?? 0,
      buildId: deploymentId,
    };
  }

  options?.callbacks?.onDeploymentReady?.();

  // Step 3: Set the deployment as live
  const setLiveUrl = `${baseUrl}/v1/deployments/${deploymentId}/set-live`;

  if (debug) {
    console.log(`[debug] POST ${setLiveUrl}`);
  }

  const setLiveResponse = await tinybirdFetch(setLiveUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  if (!setLiveResponse.ok) {
    const setLiveBody = await setLiveResponse.text();
    return {
      success: false,
      result: "failed",
      error: `Failed to set deployment as live: ${setLiveResponse.status} ${setLiveResponse.statusText}\n${setLiveBody}`,
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections?.length ?? 0,
      buildId: deploymentId,
    };
  }

  if (debug) {
    console.log(`[debug] Deployment ${deploymentId} is now live`);
  }

  options?.callbacks?.onDeploymentLive?.(deploymentId);

  // Extract resource changes from deployment details
  const deploymentDetails = body.deployment as DeploymentDetails;

  return {
    success: true,
    result: "success",
    datasourceCount: resources.datasources.length,
    pipeCount: resources.pipes.length,
    connectionCount: resources.connections?.length ?? 0,
    buildId: deploymentId,
    pipes: {
      changed: deploymentDetails.changed_pipe_names ?? [],
      created: deploymentDetails.new_pipe_names ?? [],
      deleted: deploymentDetails.deleted_pipe_names ?? [],
    },
    datasources: {
      changed: deploymentDetails.changed_datasource_names ?? [],
      created: deploymentDetails.new_datasource_names ?? [],
      deleted: deploymentDetails.deleted_datasource_names ?? [],
    },
  };
}

/**
 * Helper function to sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
