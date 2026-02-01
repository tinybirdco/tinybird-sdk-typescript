/**
 * Type inference utilities for extracting TypeScript types from Tinybird definitions
 */

import type { TypeValidator, TypeModifiers } from "../schema/types.js";
import type { ParamValidator } from "../schema/params.js";
import type { DatasourceDefinition, SchemaDefinition, ColumnDefinition } from "../schema/datasource.js";
import type { PipeDefinition, ParamsDefinition, OutputDefinition } from "../schema/pipe.js";

/**
 * Extract the TypeScript type from a type validator
 *
 * @example
 * ```ts
 * import { t, Infer } from '@tinybirdco/sdk';
 *
 * const myType = t.string();
 * type MyType = Infer<typeof myType>; // string
 *
 * const myArray = t.array(t.int32());
 * type MyArray = Infer<typeof myArray>; // number[]
 * ```
 */
export type Infer<T> = T extends TypeValidator<infer U, string, TypeModifiers>
  ? U
  : T extends ParamValidator<infer U, string, boolean>
    ? U
    : never;

/**
 * Infer a single column type, handling both raw validators and column definitions
 */
type InferColumn<T> = T extends TypeValidator<infer U, string, TypeModifiers>
  ? U
  : T extends { type: TypeValidator<infer U, string, TypeModifiers> }
    ? U
    : never;

/**
 * Extract a row type from a datasource definition
 *
 * @example
 * ```ts
 * import { defineDatasource, t, InferRow } from '@tinybirdco/sdk';
 *
 * const events = defineDatasource('events', {
 *   schema: {
 *     id: t.string(),
 *     count: t.int32(),
 *     timestamp: t.dateTime(),
 *   },
 * });
 *
 * type EventRow = InferRow<typeof events>;
 * // { id: string; count: number; timestamp: Date }
 * ```
 */
export type InferRow<T> = T extends DatasourceDefinition<infer S>
  ? { [K in keyof S]: InferColumn<S[K]> }
  : never;

/**
 * Infer a single parameter type, respecting required/optional
 */
type InferSingleParam<T> = T extends ParamValidator<infer U, string, infer R>
  ? R extends true
    ? U
    : U | undefined
  : never;

/**
 * Extract the required parameter keys from a params definition
 */
type RequiredParamKeys<T extends ParamsDefinition> = {
  [K in keyof T]: T[K] extends ParamValidator<unknown, string, true> ? K : never;
}[keyof T];

/**
 * Extract the optional parameter keys from a params definition
 */
type OptionalParamKeys<T extends ParamsDefinition> = {
  [K in keyof T]: T[K] extends ParamValidator<unknown, string, false> ? K : never;
}[keyof T];

/**
 * Extract the params type from a pipe definition
 *
 * @example
 * ```ts
 * import { definePipe, p, InferParams } from '@tinybirdco/sdk';
 *
 * const myPipe = definePipe('my_pipe', {
 *   params: {
 *     userId: p.string(),
 *     limit: p.int32().optional(10),
 *   },
 *   nodes: [...],
 *   output: {...},
 * });
 *
 * type MyParams = InferParams<typeof myPipe>;
 * // { userId: string; limit?: number }
 * ```
 */
export type InferParams<T> = T extends PipeDefinition<infer P, OutputDefinition>
  ? {
      [K in RequiredParamKeys<P>]: InferSingleParam<P[K]>;
    } & {
      [K in OptionalParamKeys<P>]?: InferSingleParam<P[K]>;
    }
  : never;

/**
 * Extract the output type (single row) from a pipe definition
 *
 * @example
 * ```ts
 * import { definePipe, t, InferOutput } from '@tinybirdco/sdk';
 *
 * const myPipe = definePipe('my_pipe', {
 *   params: {},
 *   nodes: [...],
 *   output: {
 *     name: t.string(),
 *     count: t.uint64(),
 *   },
 * });
 *
 * type MyOutput = InferOutput<typeof myPipe>;
 * // { name: string; count: number }[]
 * ```
 */
export type InferOutput<T> = T extends PipeDefinition<ParamsDefinition, infer O>
  ? { [K in keyof O]: InferColumn<O[K]> }[]
  : never;

/**
 * Extract a single output row type (without array wrapper)
 */
export type InferOutputRow<T> = T extends PipeDefinition<ParamsDefinition, infer O>
  ? { [K in keyof O]: InferColumn<O[K]> }
  : never;

/**
 * Infer the event type for ingestion (same as row type)
 *
 * @example
 * ```ts
 * import { defineDatasource, t, InferEvent } from '@tinybirdco/sdk';
 *
 * const events = defineDatasource('events', {
 *   schema: {
 *     id: t.string(),
 *     timestamp: t.dateTime(),
 *   },
 * });
 *
 * type Event = InferEvent<typeof events>;
 * // { id: string; timestamp: Date }
 *
 * // Use for type-safe event ingestion
 * const event: Event = { id: '123', timestamp: new Date() };
 * ```
 */
export type InferEvent<T> = InferRow<T>;

/**
 * Make all properties of InferRow optional (for partial updates)
 */
export type PartialRow<T> = T extends DatasourceDefinition<infer S>
  ? Partial<{ [K in keyof S]: InferColumn<S[K]> }>
  : never;

/**
 * Extract the schema definition type from a datasource
 */
export type InferSchema<T> = T extends DatasourceDefinition<infer S> ? S : never;

/**
 * Helper type to get the Tinybird type string for a schema
 * Handles both raw TypeValidators and ColumnDefinition wrappers
 */
export type InferTinybirdTypes<T extends SchemaDefinition> = {
  [K in keyof T]: T[K] extends ColumnDefinition<infer V>
    ? V extends TypeValidator<unknown, infer TB, TypeModifiers>
      ? TB
      : never
    : T[K] extends TypeValidator<unknown, infer TB, TypeModifiers>
      ? TB
      : never;
};

/**
 * Extract the target datasource from a materialized view pipe
 *
 * @example
 * ```ts
 * import { definePipe, defineDatasource, t, engine, InferMaterializedTarget } from '@tinybirdco/sdk';
 *
 * const salesByHour = defineDatasource('sales_by_hour', {
 *   schema: { day: t.date(), total: t.uint64() },
 *   engine: engine.aggregatingMergeTree({ sortingKey: ['day'] }),
 * });
 *
 * const salesMv = definePipe('sales_mv', {
 *   nodes: [...],
 *   output: { day: t.date(), total: t.uint64() },
 *   materialized: { datasource: salesByHour },
 * });
 *
 * type Target = InferMaterializedTarget<typeof salesMv>;
 * // typeof salesByHour
 * ```
 */
export type InferMaterializedTarget<T> = T extends PipeDefinition<
  ParamsDefinition,
  OutputDefinition
>
  ? T["options"]["materialized"] extends { datasource: infer D }
    ? D extends DatasourceDefinition<SchemaDefinition>
      ? D
      : never
    : never
  : never;

/**
 * Extract the target datasource row type from a materialized view pipe
 *
 * @example
 * ```ts
 * type TargetRow = InferMaterializedTargetRow<typeof salesMv>;
 * // { day: Date; total: number }
 * ```
 */
export type InferMaterializedTargetRow<T> = InferRow<InferMaterializedTarget<T>>;

/**
 * Check if a pipe definition is a materialized view (type-level)
 */
export type IsMaterializedPipe<T> = T extends PipeDefinition<
  ParamsDefinition,
  OutputDefinition
>
  ? T["options"]["materialized"] extends { datasource: DatasourceDefinition<SchemaDefinition> }
    ? true
    : false
  : false;
