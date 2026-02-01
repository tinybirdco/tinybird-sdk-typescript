/**
 * Schema validation for pipe output
 * Validates that query responses match the declared output schema
 */

import { TinybirdClient } from "../../client/base.js";
import type { ProjectDefinition, PipesDefinition } from "../../schema/project.js";
import type { PipeDefinition, OutputDefinition } from "../../schema/pipe.js";
import type { ColumnMeta } from "../../client/types.js";
import type { LoadedEntities } from "../../generator/loader.js";

/**
 * Options for schema validation
 */
export interface SchemaValidationOptions {
  /** The project definition containing pipe schemas (legacy) */
  project?: ProjectDefinition;
  /** The loaded entities containing pipe schemas (new) */
  entities?: LoadedEntities;
  /** Names of pipes to validate */
  pipeNames: string[];
  /** Tinybird API base URL */
  baseUrl: string;
  /** API token for authentication */
  token: string;
}

/**
 * A single validation issue
 */
export interface ValidationIssue {
  /** Name of the pipe with the issue */
  pipeName: string;
  /** Issue severity */
  type: "error" | "warning";
  /** Human-readable description of the issue */
  message: string;
}

/**
 * Result of schema validation
 */
export interface SchemaValidationResult {
  /** Whether all validations passed (no errors) */
  valid: boolean;
  /** List of validation issues found */
  issues: ValidationIssue[];
  /** Names of pipes that were successfully validated */
  pipesValidated: string[];
  /** Names of pipes that were skipped (e.g., require params) */
  pipesSkipped: string[];
}

/**
 * Internal result of validating a single pipe's output schema
 */
interface ColumnValidation {
  valid: boolean;
  missingColumns: { name: string; expectedType: string }[];
  extraColumns: { name: string; actualType: string }[];
  typeMismatches: { name: string; expectedType: string; actualType: string }[];
}

/**
 * Validate pipe schemas by querying them and comparing response to output definition
 *
 * @param options - Validation options
 * @returns Validation result with issues found
 */
export async function validatePipeSchemas(
  options: SchemaValidationOptions
): Promise<SchemaValidationResult> {
  const client = new TinybirdClient({
    baseUrl: options.baseUrl,
    token: options.token,
  });

  const result: SchemaValidationResult = {
    valid: true,
    issues: [],
    pipesValidated: [],
    pipesSkipped: [],
  };

  // Get pipes from either project or entities
  const pipes: PipesDefinition = options.entities
    ? Object.fromEntries(
        Object.entries(options.entities.pipes).map(([name, { definition }]) => [name, definition])
      )
    : options.project?.pipes ?? {};

  // Only validate the specified pipes
  for (const pipeName of options.pipeNames) {
    // Find pipe by name
    const pipe = Object.values(pipes).find(
      (p) => p._name === pipeName
    );

    if (!pipe) {
      // Pipe exists in Tinybird but not in local schema (could be deleted or renamed)
      continue;
    }

    // Skip if pipe has required params without defaults
    if (hasRequiredParams(pipe)) {
      result.pipesSkipped.push(pipeName);
      continue;
    }

    // Skip if pipe has no output schema (reusable pipes)
    if (!pipe._output) {
      result.pipesSkipped.push(pipeName);
      continue;
    }

    // Build params using defaults
    const params = buildDefaultParams(pipe);

    try {
      const response = await client.query(pipeName, params);
      const validation = validateOutputSchema(response.meta, pipe._output);

      if (!validation.valid) {
        result.valid = false;
      }

      // Add missing column errors
      for (const missing of validation.missingColumns) {
        result.issues.push({
          pipeName,
          type: "error",
          message: `Missing column '${missing.name}' (expected: ${missing.expectedType})`,
        });
      }

      // Add type mismatch errors
      for (const mismatch of validation.typeMismatches) {
        result.issues.push({
          pipeName,
          type: "error",
          message: `Type mismatch '${mismatch.name}': expected ${mismatch.expectedType}, got ${mismatch.actualType}`,
        });
      }

      // Add extra column warnings
      for (const extra of validation.extraColumns) {
        result.issues.push({
          pipeName,
          type: "warning",
          message: `Extra column '${extra.name}' (${extra.actualType}) not in output schema`,
        });
      }

      result.pipesValidated.push(pipeName);
    } catch {
      // Query failed - skip validation for this pipe
      // This could happen if the pipe doesn't exist yet, network issues, etc.
      result.pipesSkipped.push(pipeName);
    }
  }

  return result;
}

/**
 * Check if a pipe has any required parameters without defaults
 */
function hasRequiredParams(pipe: PipeDefinition): boolean {
  if (!pipe._params) return false;

  for (const param of Object.values(pipe._params)) {
    if (param._required && param._default === undefined) {
      return true;
    }
  }
  return false;
}

/**
 * Build a params object using default values from the pipe definition
 */
function buildDefaultParams(pipe: PipeDefinition): Record<string, unknown> {
  const params: Record<string, unknown> = {};

  if (!pipe._params) return params;

  for (const [name, param] of Object.entries(pipe._params)) {
    if (param._default !== undefined) {
      params[name] = param._default;
    }
  }

  return params;
}

/**
 * Validate response metadata against the expected output schema
 */
function validateOutputSchema(
  responseMeta: ColumnMeta[],
  outputSchema: OutputDefinition
): ColumnValidation {
  const result: ColumnValidation = {
    valid: true,
    missingColumns: [],
    extraColumns: [],
    typeMismatches: [],
  };

  // Build a map of response columns for lookup
  const responseColumns = new Map(
    responseMeta.map((col) => [col.name, col.type])
  );

  // Check each expected column from the schema
  for (const [name, validator] of Object.entries(outputSchema)) {
    const expectedType = validator._tinybirdType;
    const actualType = responseColumns.get(name);

    if (!actualType) {
      // Column missing from response
      result.missingColumns.push({ name, expectedType });
      result.valid = false;
    } else if (!typesAreCompatible(actualType, expectedType)) {
      // Column exists but type doesn't match
      result.typeMismatches.push({ name, expectedType, actualType });
      result.valid = false;
    }

    // Remove from map so we can find extra columns
    responseColumns.delete(name);
  }

  // Remaining columns are extras (warnings, not errors)
  for (const [name, actualType] of responseColumns) {
    result.extraColumns.push({ name, actualType });
  }

  return result;
}

/**
 * Check if two ClickHouse types are compatible
 * Handles Nullable, LowCardinality, and timezone variations
 */
function typesAreCompatible(actual: string, expected: string): boolean {
  const normalize = (t: string): string => {
    let normalized = t;
    // Remove LowCardinality(Nullable(...)) to just the inner type (must be before individual removals)
    normalized = normalized.replace(
      /^LowCardinality\(Nullable\((.+)\)\)$/,
      "$1"
    );
    // Remove Nullable wrapper
    normalized = normalized.replace(/^Nullable\((.+)\)$/, "$1");
    // Remove LowCardinality wrapper
    normalized = normalized.replace(/^LowCardinality\((.+)\)$/, "$1");
    // Remove timezone from DateTime
    normalized = normalized.replace(/^DateTime\('.+'\)$/, "DateTime");
    // Remove precision and timezone from DateTime64
    normalized = normalized.replace(/^DateTime64\(\d+(, '.+')?\)$/, "DateTime64");
    return normalized;
  };

  return normalize(actual) === normalize(expected);
}

// Export internal functions for testing
export { typesAreCompatible as _typesAreCompatible };
export { validateOutputSchema as _validateOutputSchema };
export { hasRequiredParams as _hasRequiredParams };
export { buildDefaultParams as _buildDefaultParams };
