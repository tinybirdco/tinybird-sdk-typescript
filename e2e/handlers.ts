/**
 * MSW handlers for E2E tests
 */

import { http, HttpResponse } from "msw";

export const BASE_URL = "https://api.tinybird.co";

/**
 * Create a successful build response
 */
export function createBuildSuccessResponse(options?: {
  buildId?: string;
  newPipes?: string[];
  newDatasources?: string[];
  changedPipes?: string[];
  changedDatasources?: string[];
}) {
  return {
    result: "success",
    build: {
      id: options?.buildId ?? "build-e2e-123",
      new_pipe_names: options?.newPipes ?? [],
      new_datasource_names: options?.newDatasources ?? [],
      changed_pipe_names: options?.changedPipes ?? [],
      changed_datasource_names: options?.changedDatasources ?? [],
      deleted_pipe_names: [],
      deleted_datasource_names: [],
    },
  };
}

/**
 * Create a branch response
 */
export function createBranchResponse(name: string) {
  return {
    id: `branch-${name}`,
    name,
    token: `p.branch-token-${name}`,
    created_at: new Date().toISOString(),
  };
}

/**
 * Create a job response (for async branch creation)
 */
export function createJobResponse(jobId: string = "job-e2e-123") {
  return {
    job: {
      id: jobId,
      status: "working",
    },
  };
}

/**
 * Create a job done response
 */
export function createJobDoneResponse(jobId: string = "job-e2e-123") {
  return {
    id: jobId,
    status: "done" as const,
  };
}

/**
 * Default handlers for E2E tests
 */
export const handlers = [
  // Build endpoint - success by default
  http.post(`${BASE_URL}/v1/build`, () => {
    return HttpResponse.json(
      createBuildSuccessResponse({
        newDatasources: ["page_views"],
        newPipes: ["top_pages"],
      })
    );
  }),

  // Create branch (POST /v1/environments) - returns job
  http.post(`${BASE_URL}/v1/environments`, () => {
    return HttpResponse.json(createJobResponse());
  }),

  // List branches (GET /v1/environments)
  http.get(`${BASE_URL}/v1/environments`, () => {
    return HttpResponse.json({
      environments: [],
    });
  }),

  // Get branch with token (GET /v0/environments/:name)
  http.get(`${BASE_URL}/v0/environments/:name`, ({ params }) => {
    const name = params.name as string;
    return HttpResponse.json(createBranchResponse(name));
  }),

  // Job polling (GET /v0/jobs/:jobId)
  http.get(`${BASE_URL}/v0/jobs/:jobId`, ({ params }) => {
    const jobId = params.jobId as string;
    return HttpResponse.json(createJobDoneResponse(jobId));
  }),
];
