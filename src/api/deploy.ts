/**
 * Deploy resources to Tinybird main workspace via the `tb` CLI binary
 * (https://gitlab.com/tinybird/analytics/-/merge_requests/23765, PROTM-471).
 *
 * This used to POST directly to /v1/deploy. It now delegates deployment
 * *creation* to the downloaded `tb` binary, which hits the exact same
 * endpoint with the exact same multipart layout (verified against
 * tinybird/tb/modules/deployment_common.py), so the heavy lifting lives in
 * one place instead of being reimplemented here. Two things stay local:
 * - Stale-deployment cleanup (Step 0 below): `create_deployment()` on the
 *   Python side has no equivalent, so this is purely an SDK-side workaround
 *   and still applies regardless of which client calls /v1/deploy.
 * - Status polling (Step 2 below): the binary's own --wait mode would block
 *   without giving us anything to drive progress callbacks from, so
 *   deployment creation is always submitted with --no-wait and polled here.
 *
 * No HTTP fallback: if the binary can't be resolved (unsupported platform,
 * no network, checksum mismatch), this returns a clear, specific failure
 * instead of silently falling back to a direct POST.
 *
 * DRAFT: there is no published `tb` binary yet (the MR above hasn't
 * merged), so this has only been exercised against test fakes, not a real
 * binary. See the two VERIFY comments below for the specific things to
 * re-check once one exists.
 */

import type { GeneratedResources } from "../generator/index.js";
import type { BuildConfig, BuildApiResult } from "./build.js";
import { tinybirdFetch } from "./fetcher.js";
import { materializeResourcesToTempProject } from "../cli-binary/resources-to-project.js";
import { CliBinaryUnavailableError, resolveTbBinaryPath } from "../cli-binary/index.js";
import { runTbBinary } from "../cli-binary/run.js";

const FORWARD_CLASSIC_GUIDANCE =
  "Use the Tinybird Classic CLI (`tb`) from a Tinybird Classic workspace for this operation.";

/**
 * Poll interval used while waiting for a deployment to reach `data_ready`
 * (and, when auto-promoting, `live`). Matches the Tinybird CLI (`tb deploy`).
 */
const POLL_INTERVAL_MS = 5_000;

/**
 * How many consecutive `failed` status polls to tolerate before giving up.
 * With a 5s poll interval this is ~5 minutes, matching the CLI safety valve
 * for deployments that fail but never auto-delete.
 */
export const MAX_CONSECUTIVE_FAILED_POLLS = 60;

/**
 * Feedback item from deployment response
 */
export interface DeploymentFeedback {
  resource: string | null;
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
 *
 * @deprecated no longer produced internally (deployment creation goes
 * through the `tb` binary now, which emits a `DeploymentDetails` object
 * directly, not this `{result, deployment}` envelope). Kept exported for
 * backwards compatibility with existing imports.
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
}

/**
 * Response from /v1/deployments/{id} endpoint
 */
export interface DeploymentStatusResponse {
  result: string;
  deployment: Deployment;
}

/**
 * Resource changes from deployment
 */
export interface DeploymentChanges {
  datasources: {
    created: string[];
    changed: string[];
    deleted: string[];
  };
  pipes: {
    created: string[];
    changed: string[];
    deleted: string[];
  };
  connections: {
    created: string[];
    changed: string[];
    deleted: string[];
  };
}

/**
 * Callbacks for deploy progress updates
 */
export interface DeployCallbacks {
  /** Called when deployment is created and changes are available */
  onChanges?: (changes: DeploymentChanges) => void;
  /** Called when deployment was submitted but wait was disabled */
  onDeploymentSubmitted?: (deploymentId: string) => void;
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

function emptyChanges(): DeploymentChanges {
  return {
    datasources: { created: [], changed: [], deleted: [] },
    pipes: { created: [], changed: [], deleted: [] },
    connections: { created: [], changed: [], deleted: [] },
  };
}

function normalizeDeployErrorMessage(message: string): string {
  const trimmedMessage = message.trim();
  const isForwardClassicError =
    trimmedMessage.includes("Tinybird Forward workspace") &&
    trimmedMessage.includes("Tinybird Classic workspaces");

  if (!isForwardClassicError || trimmedMessage.includes(FORWARD_CLASSIC_GUIDANCE)) {
    return message;
  }

  return `${trimmedMessage}\n${FORWARD_CLASSIC_GUIDANCE}`;
}

function parseTbDeploymentJson(stdout: string): DeploymentDetails | null {
  const trimmed = stdout.trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed) as DeploymentDetails;
  } catch {
    return null;
  }
}

function formatTbErrors(parsed: DeploymentDetails, fallback: string): string {
  if (parsed.errors && parsed.errors.length > 0) {
    return parsed.errors
      .map((e) => (e.filename ? `[${e.filename}] ${normalizeDeployErrorMessage(e.error)}` : normalizeDeployErrorMessage(e.error)))
      .join("\n");
  }
  if (parsed.feedback && parsed.feedback.length > 0) {
    const errors = parsed.feedback.filter((f) => f.level === "ERROR");
    if (errors.length > 0) {
      return errors
        .map((f) => {
          const resourceName = f.resource?.split(" ").pop() ?? f.resource ?? "unknown";
          return `${resourceName}: ${normalizeDeployErrorMessage(f.message)}`;
        })
        .join("\n");
    }
  }
  return fallback;
}

/**
 * Deploy generated resources to Tinybird main workspace
 *
 * 1. Cleans up any stale (non-live) deployments that might block the new one.
 * 2. Submits the deployment via `tb deployment create --no-wait --output json`.
 * 3. When `wait` is true (default), polls /v1/deployments/{id} until the
 *    deployment reaches `data_ready` (and, if `auto` is also true, `live`).
 *
 * @param config - Deploy configuration with API URL and token
 * @param resources - Generated resources to deploy
 * @returns Deploy result
 */
export async function deployToMain(
  config: BuildConfig,
  resources: GeneratedResources,
  options?: {
    debug?: boolean;
    check?: boolean;
    allowDestructiveOperations?: boolean;
    wait?: boolean;
    auto?: boolean;
    callbacks?: DeployCallbacks;
  }
): Promise<BuildApiResult> {
  const debug = options?.debug ?? !!process.env.TINYBIRD_DEBUG;
  const wait = options?.wait ?? true;
  const auto = options?.auto ?? true;
  const baseUrl = config.baseUrl.replace(/\/$/, "");

  const counts = {
    datasourceCount: resources.datasources.length,
    pipeCount: resources.pipes.length,
    connectionCount: resources.connections?.length ?? 0,
  };

  // Step 0: Clean up any stale non-live deployments that might block the new deployment.
  // Skipped in check mode so validation runs don't tear down in-flight deployments.
  // The `tb` binary doesn't do this itself (verified against
  // tinybird/tb/modules/deployment_common.py's create_deployment()) — it's an
  // SDK-side workaround that applies regardless of which client creates the
  // deployment, so it stays here rather than moving into the binary call.
  if (!options?.check) {
    try {
      const deploymentsUrl = `${baseUrl}/v1/deployments`;
      const deploymentsResponse = await tinybirdFetch(deploymentsUrl, {
        headers: { Authorization: `Bearer ${config.token}` },
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
            headers: { Authorization: `Bearer ${config.token}` },
          });
        }
      }
    } catch (e) {
      // Ignore errors during cleanup - we'll try to deploy anyway
      if (debug) {
        console.log(`[debug] Failed to clean up stale deployments: ${e}`);
      }
    }
  }

  // Step 1: Submit the deployment via the tb binary.
  const project = await materializeResourcesToTempProject(resources);

  try {
    let binaryPath: string;
    try {
      binaryPath = await resolveTbBinaryPath();
    } catch (error) {
      if (error instanceof CliBinaryUnavailableError) {
        return { success: false, result: "failed", error: error.message, ...counts };
      }
      throw error;
    }

    const args = ["deployment", "create", "--no-wait"];
    if (options?.check) {
      args.push("--check");
    } else if (auto) {
      args.push("--auto");
    } else {
      args.push("--no-auto");
    }
    if (options?.allowDestructiveOperations) {
      args.push("--allow-destructive-operations");
    }

    const run = await runTbBinary(binaryPath, args, {
      cwd: project.dir,
      token: config.token,
      host: baseUrl,
    });

    if (debug) {
      console.log(`[debug] tb exit code: ${run.exitCode}`);
      console.log(`[debug] tb stdout: ${run.stdout}`);
      console.log(`[debug] tb stderr: ${run.stderr}`);
    }

    const parsed = parseTbDeploymentJson(run.stdout);

    if (!parsed) {
      if (run.exitCode === 0) {
        // VERIFY once a real binary exists: deployment_common.py's
        // `no_changes` branch doesn't call echo_json, so empty stdout + exit
        // 0 is inferred (not confirmed) to mean "nothing to deploy".
        return { success: true, result: "no_changes", ...counts, ...emptyChanges() };
      }
      // VERIFY once a real binary exists: a failed --check run also doesn't
      // call echo_json in deployment_common.py, so this covers both that
      // case and any other no-JSON failure.
      const message = run.stderr.trim() || run.stdout.trim() || `tb exited with code ${run.exitCode}`;
      return { success: false, result: "failed", error: message, ...counts };
    }

    if (run.exitCode !== 0) {
      return {
        success: false,
        result: "failed",
        error: formatTbErrors(parsed, run.stderr.trim() || `tb exited with code ${run.exitCode}`),
        ...counts,
      };
    }

    if (options?.check) {
      options.callbacks?.onValidating?.();
      return { success: true, result: "success", ...counts };
    }

    const changes: DeploymentChanges = {
      datasources: {
        created: parsed.new_datasource_names ?? [],
        changed: parsed.changed_datasource_names ?? [],
        deleted: parsed.deleted_datasource_names ?? [],
      },
      pipes: {
        created: parsed.new_pipe_names ?? [],
        changed: parsed.changed_pipe_names ?? [],
        deleted: parsed.deleted_pipe_names ?? [],
      },
      connections: {
        created: parsed.new_data_connector_names ?? [],
        changed: parsed.changed_data_connector_names ?? [],
        deleted: parsed.deleted_data_connector_names ?? [],
      },
    };

    options?.callbacks?.onChanges?.(changes);

    const deploymentId = parsed.id;

    if (!wait) {
      options?.callbacks?.onDeploymentSubmitted?.(deploymentId);
      return { success: true, result: "success", buildId: deploymentId, ...counts, ...changes };
    }

    // Step 2: poll until the deployment reaches a terminal state.
    return await pollDeploymentUntilDone({
      config,
      deploymentId,
      auto,
      debug,
      callbacks: options?.callbacks,
      counts,
      changes,
    });
  } finally {
    await project.cleanup();
  }
}

async function pollDeploymentUntilDone(args: {
  config: BuildConfig;
  deploymentId: string;
  auto: boolean;
  debug?: boolean;
  callbacks?: DeployCallbacks;
  counts: { datasourceCount: number; pipeCount: number; connectionCount: number };
  changes: DeploymentChanges;
}): Promise<BuildApiResult> {
  const { config, deploymentId, auto, debug, callbacks, counts, changes } = args;
  const baseUrl = config.baseUrl.replace(/\/$/, "");

  let deployment: Deployment = { id: deploymentId, status: "pending" };
  let timesSeenFailed = 0;
  let notifiedReady = false;
  let notifiedWaitingForPromote = false;

  callbacks?.onWaitingForReady?.();

  const isDone = (): boolean => {
    if (deployment.status !== "data_ready") {
      return false;
    }
    if (auto) {
      // When auto-promoting we must also wait for the server to flip it live.
      return deployment.live === true;
    }
    return true;
  };

  const buildError = (message: string): BuildApiResult => ({
    success: false,
    result: "failed",
    error: message,
    buildId: deploymentId,
    ...counts,
  });

  while (!isDone()) {
    await sleep(POLL_INTERVAL_MS);

    if (debug) {
      console.log(`[debug] Polling deployment status...`);
    }

    const statusUrl = `${baseUrl}/v1/deployments/${deploymentId}`;
    const statusResponse = await tinybirdFetch(statusUrl, {
      headers: { Authorization: `Bearer ${config.token}` },
    });

    if (!statusResponse.ok) {
      return buildError(
        `Failed to check deployment status: ${statusResponse.status} ${statusResponse.statusText}`
      );
    }

    const statusBody = (await statusResponse.json()) as DeploymentStatusResponse;
    deployment = statusBody.deployment;

    if (debug) {
      console.log(
        `[debug] Deployment status: ${deployment.status} (live=${deployment.live ?? false})`
      );
    }

    if (deployment.status === "failed") {
      timesSeenFailed++;
      if (timesSeenFailed > MAX_CONSECUTIVE_FAILED_POLLS) {
        return buildError(
          "Deployment failed to create and didn't start deleting automatically after 5 minutes. " +
            "You might need to delete it manually in the UI."
        );
      }
      continue;
    }

    if (deployment.status === "deleting" || deployment.status === "deleted") {
      const errors = deployment.feedback
        ?.filter((f) => f.level === "ERROR")
        .map((f) => f.message)
        .join("\n");
      const errorSuffix = errors ? `\n${errors}` : "";
      return buildError(
        `Deployment failed and ${
          deployment.status === "deleting" ? "is being" : "was"
        } deleted automatically.${errorSuffix}`
      );
    }

    if (
      auto &&
      deployment.status === "data_ready" &&
      !deployment.live &&
      !notifiedWaitingForPromote
    ) {
      if (!notifiedReady) {
        callbacks?.onDeploymentReady?.();
        notifiedReady = true;
      }
      callbacks?.onWaitingForPromote?.();
      notifiedWaitingForPromote = true;
    }
  }

  if (!notifiedReady) {
    callbacks?.onDeploymentReady?.();
  }

  if (auto) {
    if (debug) {
      console.log(`[debug] Deployment ${deploymentId} is now live`);
    }
    callbacks?.onDeploymentPromoted?.();
    callbacks?.onDeploymentLive?.(deploymentId);
  }

  return { success: true, result: "success", buildId: deploymentId, ...counts, ...changes };
}

/**
 * Helper function to sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
