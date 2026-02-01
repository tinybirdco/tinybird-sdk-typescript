/**
 * Engine configurations for Tinybird datasources
 * ClickHouse table engines determine how data is stored and queried
 */

/**
 * Base configuration shared by all MergeTree engines
 */
export interface BaseMergeTreeConfig {
  /** Columns used for sorting data within parts (required for all MergeTree engines) */
  sortingKey: string | readonly string[];
  /** Expression for partitioning data (e.g., 'toYYYYMM(timestamp)') */
  partitionKey?: string;
  /** Primary key columns (defaults to sortingKey if not specified) */
  primaryKey?: string | readonly string[];
  /** TTL expression for automatic data expiration (e.g., 'timestamp + INTERVAL 90 DAY') */
  ttl?: string;
  /** Additional engine settings */
  settings?: Record<string, string | number | boolean>;
}

/**
 * MergeTree engine configuration
 * The most universal and functional table engine for high-load tasks
 */
export interface MergeTreeConfig extends BaseMergeTreeConfig {
  type: "MergeTree";
}

/**
 * ReplacingMergeTree engine configuration
 * Removes duplicate rows with the same sorting key during merges
 */
export interface ReplacingMergeTreeConfig extends BaseMergeTreeConfig {
  type: "ReplacingMergeTree";
  /** Optional version column - rows with highest version are kept */
  ver?: string;
  /** Optional flag to enable clean mode (ClickHouse 23.2+) */
  isDeleted?: string;
}

/**
 * SummingMergeTree engine configuration
 * Sums numeric columns for rows with the same sorting key during merges
 */
export interface SummingMergeTreeConfig extends BaseMergeTreeConfig {
  type: "SummingMergeTree";
  /** Columns to sum (if not specified, all numeric columns are summed) */
  columns?: readonly string[];
}

/**
 * AggregatingMergeTree engine configuration
 * For incremental data aggregation with AggregateFunction columns
 */
export interface AggregatingMergeTreeConfig extends BaseMergeTreeConfig {
  type: "AggregatingMergeTree";
}

/**
 * CollapsingMergeTree engine configuration
 * For collapsing rows that cancel each other out
 */
export interface CollapsingMergeTreeConfig extends BaseMergeTreeConfig {
  type: "CollapsingMergeTree";
  /** Column containing sign (1 for state, -1 for cancel) */
  sign: string;
}

/**
 * VersionedCollapsingMergeTree engine configuration
 * For collapsing with versioning when events may arrive out of order
 */
export interface VersionedCollapsingMergeTreeConfig extends BaseMergeTreeConfig {
  type: "VersionedCollapsingMergeTree";
  /** Column containing sign (1 for state, -1 for cancel) */
  sign: string;
  /** Column containing version number */
  version: string;
}

/**
 * Union type of all engine configurations
 */
export type EngineConfig =
  | MergeTreeConfig
  | ReplacingMergeTreeConfig
  | SummingMergeTreeConfig
  | AggregatingMergeTreeConfig
  | CollapsingMergeTreeConfig
  | VersionedCollapsingMergeTreeConfig;

/**
 * Helper to normalize sorting key to array format
 */
function normalizeSortingKey(key: string | readonly string[]): readonly string[] {
  return typeof key === "string" ? [key] : key;
}

/**
 * Engine configuration builders
 *
 * @example
 * ```ts
 * import { engine } from '@tinybirdco/sdk';
 *
 * // Basic MergeTree
 * engine.mergeTree({
 *   sortingKey: ['user_id', 'timestamp'],
 *   partitionKey: 'toYYYYMM(timestamp)',
 * });
 *
 * // ReplacingMergeTree for upserts
 * engine.replacingMergeTree({
 *   sortingKey: ['id'],
 *   ver: 'updated_at',
 * });
 *
 * // SummingMergeTree for counters
 * engine.summingMergeTree({
 *   sortingKey: ['date', 'metric_name'],
 *   columns: ['value'],
 * });
 * ```
 */
export const engine = {
  /**
   * MergeTree - The most universal engine for high-load tasks
   * Best for: General-purpose analytics, logs, events
   */
  mergeTree: (config: Omit<MergeTreeConfig, "type">): MergeTreeConfig => ({
    type: "MergeTree",
    ...config,
  }),

  /**
   * ReplacingMergeTree - Removes duplicates during background merges
   * Best for: Maintaining latest state, upserts, slowly changing dimensions
   *
   * @param config.ver - Optional version column. Rows with highest version are kept.
   */
  replacingMergeTree: (
    config: Omit<ReplacingMergeTreeConfig, "type">
  ): ReplacingMergeTreeConfig => ({
    type: "ReplacingMergeTree",
    ...config,
  }),

  /**
   * SummingMergeTree - Sums numeric columns during background merges
   * Best for: Counters, metrics aggregation, pre-aggregated data
   *
   * @param config.columns - Columns to sum. If not specified, all numeric columns are summed.
   */
  summingMergeTree: (
    config: Omit<SummingMergeTreeConfig, "type">
  ): SummingMergeTreeConfig => ({
    type: "SummingMergeTree",
    ...config,
  }),

  /**
   * AggregatingMergeTree - For incremental aggregation with AggregateFunction columns
   * Best for: Materialized views, incremental aggregation pipelines
   */
  aggregatingMergeTree: (
    config: Omit<AggregatingMergeTreeConfig, "type">
  ): AggregatingMergeTreeConfig => ({
    type: "AggregatingMergeTree",
    ...config,
  }),

  /**
   * CollapsingMergeTree - For collapsing state/cancel row pairs
   * Best for: Changelog-style updates, mutable data with deletes
   *
   * @param config.sign - Column containing 1 (state) or -1 (cancel)
   */
  collapsingMergeTree: (
    config: Omit<CollapsingMergeTreeConfig, "type">
  ): CollapsingMergeTreeConfig => ({
    type: "CollapsingMergeTree",
    ...config,
  }),

  /**
   * VersionedCollapsingMergeTree - Collapsing with versioning for out-of-order events
   * Best for: Changelog-style updates with potential out-of-order arrival
   *
   * @param config.sign - Column containing 1 (state) or -1 (cancel)
   * @param config.version - Column containing version number for ordering
   */
  versionedCollapsingMergeTree: (
    config: Omit<VersionedCollapsingMergeTreeConfig, "type">
  ): VersionedCollapsingMergeTreeConfig => ({
    type: "VersionedCollapsingMergeTree",
    ...config,
  }),
} as const;

/**
 * Get the sorting key as an array
 */
export function getSortingKey(config: EngineConfig): readonly string[] {
  return normalizeSortingKey(config.sortingKey);
}

/**
 * Get the primary key as an array (defaults to sorting key)
 */
export function getPrimaryKey(config: EngineConfig): readonly string[] {
  if (config.primaryKey) {
    return normalizeSortingKey(config.primaryKey);
  }
  return getSortingKey(config);
}

/**
 * Generate the engine clause for a datasource file
 */
export function getEngineClause(config: EngineConfig): string {
  const parts: string[] = [`ENGINE "${config.type}"`];

  if (config.partitionKey) {
    parts.push(`ENGINE_PARTITION_KEY "${config.partitionKey}"`);
  }

  const sortingKey = getSortingKey(config);
  parts.push(`ENGINE_SORTING_KEY "${sortingKey.join(", ")}"`);

  if (config.primaryKey) {
    const primaryKey = getPrimaryKey(config);
    parts.push(`ENGINE_PRIMARY_KEY "${primaryKey.join(", ")}"`);
  }

  if (config.ttl) {
    parts.push(`ENGINE_TTL "${config.ttl}"`);
  }

  // Engine-specific options
  if (config.type === "ReplacingMergeTree" && config.ver) {
    parts.push(`ENGINE_VER "${config.ver}"`);
  }

  if (config.type === "CollapsingMergeTree" || config.type === "VersionedCollapsingMergeTree") {
    parts.push(`ENGINE_SIGN "${config.sign}"`);
  }

  if (config.type === "VersionedCollapsingMergeTree") {
    parts.push(`ENGINE_VERSION "${config.version}"`);
  }

  if (config.type === "SummingMergeTree" && config.columns && config.columns.length > 0) {
    parts.push(`ENGINE_SUMMING_COLUMNS "${config.columns.join(", ")}"`);
  }

  if (config.settings && Object.keys(config.settings).length > 0) {
    const settingsStr = Object.entries(config.settings)
      .map(([k, v]) => {
        if (typeof v === "string") {
          const escaped = v.replace(/'/g, "\\'");
          return `${k}='${escaped}'`;
        }
        return `${k}=${v}`;
      })
      .join(", ");
    parts.push(`ENGINE_SETTINGS "${settingsStr}"`);
  }

  return parts.join("\n");
}
