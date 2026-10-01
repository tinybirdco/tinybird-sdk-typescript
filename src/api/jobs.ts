/**
 * Tinybird Job management API client
 * Wraps the /v0/jobs endpoints used to inspect and cancel async jobs
 * (ingest/append/replace operations, connector syncs, populates, etc.)
 *
 * Note: there is no confirmed `/v0/jobs/{id}/retry` endpoint, so this module
 * intentionally does not expose a `retry` operation. See PROTM-2270.
 */

import { createTinybirdFetcher } from "./fetcher.js";

/**
 * API configuration for job operations
 */
export interface JobApiConfig {
  /** Tinybird API base URL */
  baseUrl: string;
  /** Workspace or branch token used to authenticate requests */
  token: string;
  /** Custom fetch implementation (optional, defaults to global fetch) */
  fetch?: typeof fetch;
  /** Default timeout in milliseconds applied to every request (optional) */
  timeout?: number;
}

/**
 * Per-request options shared by job operations
 */
export interface JobRequestOptions {
  /** Request timeout in milliseconds (overrides `JobApiConfig.timeout`) */
  timeout?: number;
  /** AbortController signal for cancellation */
  signal?: AbortSignal;
}

/**
 * Status values reported for a Tinybird job
 */
export type JobStatus =
  | "waiting"
  | "working"
  | "done"
  | "error"
  | "cancelling"
  | "cancelled";

/**
 * A Tinybird job as returned by the Jobs API.
 *
 * The exact set of fields present depends on the job `kind` (e.g. `import`,
 * `populateview`, `copy`, `datasource_create_from_connector`, ...), so
 * additional kind-specific fields are preserved via the index signature.
 */
export interface Job {
  /** Job identifier */
  id: string;
  /** Job kind (e.g. "import", "populateview", "copy") */
  kind: string;
  /** Current job status */
  status: JobStatus;
  /** ISO timestamp of when the job was created */
  created_at: string;
  /** ISO timestamp of the last update to the job */
  updated_at: string;
  /** URL to fetch this job's status */
  job_url?: string;
  /** Error message, present when `status` is "error" */
  error?: string | null;
  /** Additional kind-specific fields returned by the API */
  [key: string]: unknown;
}

/**
 * Options for filtering the jobs list
 */
export interface ListJobsOptions extends JobRequestOptions {
  /** Only return jobs with this status */
  status?: JobStatus;
  /** Only return jobs of this kind */
  kind?: string;
}

/**
 * Error thrown by job API operations
 */
export class JobApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "JobApiError";
  }
}

function getFetch(config: JobApiConfig) {
  return createTinybirdFetcher(config.fetch ?? globalThis.fetch);
}

function createAbortSignal(
  config: JobApiConfig,
  options?: JobRequestOptions
): AbortSignal | undefined {
  const timeoutMs = options?.timeout ?? config.timeout;
  const existingSignal = options?.signal;

  if (!timeoutMs) {
    return existingSignal;
  }

  const timeoutSignal = AbortSignal.timeout(timeoutMs);

  if (!existingSignal) {
    return timeoutSignal;
  }

  return AbortSignal.any([timeoutSignal, existingSignal]);
}

async function handleErrorResponse(
  response: Response,
  notFoundMessage?: string
): Promise<never> {
  const body = await response.text();

  let message: string;
  if (response.status === 404 && notFoundMessage) {
    message = notFoundMessage;
  } else {
    message = `Request failed: ${response.status} ${response.statusText}`;
    if (body) {
      message += `\nAPI response: ${body}`;
    }
  }

  throw new JobApiError(message, response.status, body || undefined);
}

/**
 * List jobs in the workspace, optionally filtered by status and/or kind.
 * GET /v0/jobs
 *
 * @param config - API configuration
 * @param options - Optional status/kind filters
 * @returns Array of jobs
 */
export async function listJobs(
  config: JobApiConfig,
  options: ListJobsOptions = {}
): Promise<Job[]> {
  const fetchFn = getFetch(config);
  const url = new URL("/v0/jobs", `${config.baseUrl}/`);

  if (options.status) {
    url.searchParams.set("status", options.status);
  }
  if (options.kind) {
    url.searchParams.set("kind", options.kind);
  }

  const response = await fetchFn(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
    signal: createAbortSignal(config, options),
  });

  if (!response.ok) {
    await handleErrorResponse(response);
  }

  const data = (await response.json()) as { jobs?: Job[] };
  return data.jobs ?? [];
}

/**
 * Get details for a single job.
 * GET /v0/jobs/{job_id}
 *
 * @param config - API configuration
 * @param jobId - Job ID
 * @param options - Request options
 * @returns Job details
 */
export async function getJob(
  config: JobApiConfig,
  jobId: string,
  options: JobRequestOptions = {}
): Promise<Job> {
  const fetchFn = getFetch(config);
  const url = new URL(`/v0/jobs/${encodeURIComponent(jobId)}`, `${config.baseUrl}/`);

  const response = await fetchFn(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
    signal: createAbortSignal(config, options),
  });

  if (!response.ok) {
    await handleErrorResponse(response, `Job '${jobId}' not found.`);
  }

  return (await response.json()) as Job;
}

/**
 * Cancel a running job.
 * POST /v0/jobs/{job_id}/cancel
 *
 * Note: not all job kinds/states support cancellation; the API may respond
 * with the job already transitioning to "cancelling" rather than "cancelled".
 *
 * @param config - API configuration
 * @param jobId - Job ID
 * @param options - Request options
 * @returns The updated job
 */
export async function cancelJob(
  config: JobApiConfig,
  jobId: string,
  options: JobRequestOptions = {}
): Promise<Job> {
  const fetchFn = getFetch(config);
  const url = new URL(
    `/v0/jobs/${encodeURIComponent(jobId)}/cancel`,
    `${config.baseUrl}/`
  );

  const response = await fetchFn(url.toString(), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
    signal: createAbortSignal(config, options),
  });

  if (!response.ok) {
    await handleErrorResponse(response, `Job '${jobId}' not found.`);
  }

  return (await response.json()) as Job;
}
