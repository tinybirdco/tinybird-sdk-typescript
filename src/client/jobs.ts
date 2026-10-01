/**
 * Job management namespace for TinybirdClient
 *
 * Wraps `/v0/jobs` so callers can inspect and manage the async jobs the SDK
 * (or `tb`/the UI) triggers, such as ingest/append/replace operations and
 * connector syncs.
 *
 * Note: a dedicated job "retry" endpoint could not be confirmed against the
 * Tinybird backend, so this namespace intentionally only exposes
 * `list`, `get` and `cancel`. See PROTM-2270.
 */

import type {
  Job,
  JobRequestOptions,
  ListJobsOptions,
} from "../api/jobs.js";
import {
  listJobs as apiListJobs,
  getJob as apiGetJob,
  cancelJob as apiCancelJob,
  JobApiError,
} from "../api/jobs.js";
import { TinybirdError } from "./types.js";

/**
 * Job management namespace for TinybirdClient
 */
export class JobsNamespace {
  constructor(
    private readonly getToken: () => Promise<string>,
    private readonly baseUrl: string,
    private readonly fetchFn?: typeof globalThis.fetch,
    private readonly timeout?: number
  ) {}

  /**
   * List jobs in the workspace, optionally filtered by status and/or kind.
   *
   * @param options - Optional status/kind filters
   * @returns Array of jobs
   *
   * @example
   * ```ts
   * const jobs = await client.jobs.list();
   *
   * const failedImports = await client.jobs.list({
   *   status: "error",
   *   kind: "import",
   * });
   * ```
   */
  async list(options: ListJobsOptions = {}): Promise<Job[]> {
    const token = await this.getToken();

    try {
      return await apiListJobs(
        {
          baseUrl: this.baseUrl,
          token,
          fetch: this.fetchFn,
          timeout: this.timeout,
        },
        options
      );
    } catch (error) {
      this.rethrow(error);
    }
  }

  /**
   * Get details for a single job.
   *
   * @param jobId - Job ID
   * @param options - Request options
   * @returns Job details
   *
   * @example
   * ```ts
   * const job = await client.jobs.get("job_abc123");
   * console.log(job.status); // "done"
   * ```
   */
  async get(jobId: string, options: JobRequestOptions = {}): Promise<Job> {
    const token = await this.getToken();

    try {
      return await apiGetJob(
        {
          baseUrl: this.baseUrl,
          token,
          fetch: this.fetchFn,
          timeout: this.timeout,
        },
        jobId,
        options
      );
    } catch (error) {
      this.rethrow(error);
    }
  }

  /**
   * Cancel a running job.
   *
   * Not all job kinds/states support cancellation; the API may respond with
   * the job transitioning to "cancelling" rather than immediately
   * "cancelled".
   *
   * @param jobId - Job ID
   * @param options - Request options
   * @returns The updated job
   *
   * @example
   * ```ts
   * const job = await client.jobs.cancel("job_abc123");
   * console.log(job.status); // "cancelling" or "cancelled"
   * ```
   */
  async cancel(jobId: string, options: JobRequestOptions = {}): Promise<Job> {
    const token = await this.getToken();

    try {
      return await apiCancelJob(
        {
          baseUrl: this.baseUrl,
          token,
          fetch: this.fetchFn,
          timeout: this.timeout,
        },
        jobId,
        options
      );
    } catch (error) {
      this.rethrow(error);
    }
  }

  private rethrow(error: unknown): never {
    if (error instanceof JobApiError) {
      throw new TinybirdError(error.message, error.status, {
        error: error.message,
        status: error.status,
      });
    }
    throw error;
  }
}
