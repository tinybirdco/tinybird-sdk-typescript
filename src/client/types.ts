/**
 * Client types for Tinybird API interactions
 */

/**
 * Configuration for the Tinybird client
 */
export interface ClientConfig {
  /** Tinybird API base URL (e.g., 'https://api.tinybird.co' or 'https://api.us-east.tinybird.co') */
  baseUrl: string;
  /** API token for authentication */
  token: string;
  /** Custom fetch implementation (optional, defaults to global fetch) */
  fetch?: typeof fetch;
  /** Default timeout in milliseconds (optional) */
  timeout?: number;
}

/**
 * Column metadata from query response
 */
export interface ColumnMeta {
  /** Column name */
  name: string;
  /** Column type (Tinybird/ClickHouse type) */
  type: string;
}

/**
 * Query statistics from response
 */
export interface QueryStatistics {
  /** Time elapsed in seconds */
  elapsed: number;
  /** Number of rows read */
  rows_read: number;
  /** Number of bytes read */
  bytes_read: number;
}

/**
 * Result of a query operation
 */
export interface QueryResult<T> {
  /** Query result data */
  data: T[];
  /** Column metadata */
  meta: ColumnMeta[];
  /** Number of rows returned */
  rows: number;
  /** Query statistics */
  statistics: QueryStatistics;
}

/**
 * Result of an ingest operation
 */
export interface IngestResult {
  /** Number of rows successfully ingested */
  successful_rows: number;
  /** Number of rows that failed to ingest */
  quarantined_rows: number;
}

/**
 * Error response from Tinybird API
 */
export interface TinybirdErrorResponse {
  /** Error message */
  error: string;
  /** HTTP status code */
  status?: number;
  /** Additional error details */
  documentation?: string;
}

/**
 * Custom error class for Tinybird API errors
 */
export class TinybirdError extends Error {
  /** HTTP status code */
  readonly statusCode: number;
  /** Raw error response */
  readonly response?: TinybirdErrorResponse;

  constructor(message: string, statusCode: number, response?: TinybirdErrorResponse) {
    super(message);
    this.name = "TinybirdError";
    this.statusCode = statusCode;
    this.response = response;
  }

  /**
   * Check if this is a rate limit error
   */
  isRateLimitError(): boolean {
    return this.statusCode === 429;
  }

  /**
   * Check if this is an authentication error
   */
  isAuthError(): boolean {
    return this.statusCode === 401 || this.statusCode === 403;
  }

  /**
   * Check if this is a not found error
   */
  isNotFoundError(): boolean {
    return this.statusCode === 404;
  }

  /**
   * Check if this is a server error
   */
  isServerError(): boolean {
    return this.statusCode >= 500;
  }
}

/**
 * Options for query requests
 */
export interface QueryOptions {
  /** Request timeout in milliseconds */
  timeout?: number;
  /** AbortController signal for cancellation */
  signal?: AbortSignal;
}

/**
 * Options for ingest requests
 */
export interface IngestOptions {
  /** Request timeout in milliseconds */
  timeout?: number;
  /** AbortController signal for cancellation */
  signal?: AbortSignal;
  /** Wait for the ingestion to complete before returning */
  wait?: boolean;
}

/**
 * Base interface for typed pipe endpoints
 */
export interface TypedPipeEndpoint<TParams, TOutput> {
  (params: TParams, options?: QueryOptions): Promise<QueryResult<TOutput>>;
}

/**
 * Base interface for typed datasource ingestion
 */
export interface TypedDatasourceIngest<TRow> {
  /** Send a single event */
  send(event: TRow, options?: IngestOptions): Promise<IngestResult>;
  /** Send multiple events in a batch */
  sendBatch(events: TRow[], options?: IngestOptions): Promise<IngestResult>;
}
