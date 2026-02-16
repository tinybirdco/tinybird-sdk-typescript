/**
 * Parameter validators for Tinybird pipe queries
 * Similar to the column type validators but for query parameters
 */

// Symbol for brand typing - use Symbol.for() for global registry
// This ensures the same symbol is used across module instances
const PARAM_BRAND = Symbol.for("tinybird.param");

/**
 * Base interface for parameter validators
 */
export interface ParamValidator<
  TType,
  TTinybirdType extends string = string,
  TRequired extends boolean = true
> {
  readonly [PARAM_BRAND]: true;
  /** The inferred TypeScript type */
  readonly _type: TType;
  /** The Tinybird type string for the parameter */
  readonly _tinybirdType: TTinybirdType;
  /** Whether this parameter is required */
  readonly _required: TRequired;
  /** Default value if optional */
  readonly _default?: TType;
  /** Description for documentation */
  readonly _description?: string;

  /** Make this parameter optional with an optional default value */
  optional<TDefault extends TType | undefined = undefined>(
    defaultValue?: TDefault
  ): ParamValidator<
    TDefault extends undefined ? TType | undefined : TType,
    TTinybirdType,
    false
  >;

  /** Make this parameter required (default) */
  required(): ParamValidator<TType, TTinybirdType, true>;

  /** Add a description for this parameter */
  describe(description: string): ParamValidator<TType, TTinybirdType, TRequired>;
}

// Internal implementation
interface ParamValidatorImpl<TType, TTinybirdType extends string, TRequired extends boolean>
  extends ParamValidator<TType, TTinybirdType, TRequired> {
  readonly tinybirdType: TTinybirdType;
  readonly isRequired: TRequired;
  readonly defaultValue?: TType;
  readonly description?: string;
}

function createParamValidator<
  TType,
  TTinybirdType extends string,
  TRequired extends boolean = true
>(
  tinybirdType: TTinybirdType,
  options: {
    required?: TRequired;
    defaultValue?: TType;
    description?: string;
  } = {}
): ParamValidator<TType, TTinybirdType, TRequired> {
  const isRequired = (options.required ?? true) as TRequired;

  const validator: ParamValidatorImpl<TType, TTinybirdType, TRequired> = {
    [PARAM_BRAND]: true,
    _type: undefined as unknown as TType,
    _tinybirdType: tinybirdType,
    _required: isRequired,
    _default: options.defaultValue,
    _description: options.description,
    tinybirdType,
    isRequired,
    defaultValue: options.defaultValue,
    description: options.description,

    optional<TDefault extends TType | undefined = undefined>(defaultValue?: TDefault) {
      return createParamValidator<
        TDefault extends undefined ? TType | undefined : TType,
        TTinybirdType,
        false
      >(tinybirdType, {
        required: false,
        defaultValue: defaultValue as TDefault extends undefined ? TType | undefined : TType,
        description: options.description,
      });
    },

    required() {
      return createParamValidator<TType, TTinybirdType, true>(tinybirdType, {
        required: true,
        description: options.description,
      });
    },

    describe(description: string) {
      return createParamValidator<TType, TTinybirdType, TRequired>(tinybirdType, {
        required: isRequired,
        defaultValue: options.defaultValue,
        description,
      });
    },
  };

  return validator;
}

/**
 * Parameter validators for Tinybird pipe queries
 *
 * @example
 * ```ts
 * import { p } from '@tinybirdco/sdk';
 *
 * const params = {
 *   user_id: p.string(),
 *   limit: p.int32().optional(10),
 *   start_date: p.dateTime().describe('Start of date range'),
 * };
 * ```
 */
export const p = {
  // ============ String Types ============

  /** String parameter */
  string: () => createParamValidator<string, "String">("String"),

  /** UUID parameter */
  uuid: () => createParamValidator<string, "UUID">("UUID"),

  // ============ Integer Types ============

  /** Int8 parameter */
  int8: () => createParamValidator<number, "Int8">("Int8"),

  /** Int16 parameter */
  int16: () => createParamValidator<number, "Int16">("Int16"),

  /** Int32 parameter */
  int32: () => createParamValidator<number, "Int32">("Int32"),

  /** Int64 parameter */
  int64: () => createParamValidator<number, "Int64">("Int64"),

  /** UInt8 parameter */
  uint8: () => createParamValidator<number, "UInt8">("UInt8"),

  /** UInt16 parameter */
  uint16: () => createParamValidator<number, "UInt16">("UInt16"),

  /** UInt32 parameter */
  uint32: () => createParamValidator<number, "UInt32">("UInt32"),

  /** UInt64 parameter */
  uint64: () => createParamValidator<number, "UInt64">("UInt64"),

  // ============ Float Types ============

  /** Float32 parameter */
  float32: () => createParamValidator<number, "Float32">("Float32"),

  /** Float64 parameter */
  float64: () => createParamValidator<number, "Float64">("Float64"),

  // ============ Boolean ============

  /** Boolean parameter */
  boolean: () => createParamValidator<boolean, "Boolean">("Boolean"),

  // ============ Date/Time Types ============

  /** Date parameter (YYYY-MM-DD format, e.g. 2024-01-15) */
  date: () => createParamValidator<string, "Date">("Date"),

  /** DateTime parameter (YYYY-MM-DD HH:MM:SS format, e.g. 2024-01-15 10:30:00) */
  dateTime: () => createParamValidator<string, "DateTime">("DateTime"),

  /** DateTime64 parameter (YYYY-MM-DD HH:MM:SS[.fraction] format, e.g. 2024-01-15 10:30:00.123) */
  dateTime64: () => createParamValidator<string, "DateTime64">("DateTime64"),

  // ============ Array Types ============

  /**
   * Array parameter - values can be passed as comma-separated or repeated params
   * @param _element - The type of array elements (used for type inference)
   * @param _separator - Optional custom separator (default: comma)
   */
  array: <TElement extends ParamValidator<unknown, string, boolean>>(
    _element: TElement,
    _separator?: string
  ): ParamValidator<TElement["_type"][], "Array", true> =>
    createParamValidator<TElement["_type"][], "Array">("Array", {
      required: true,
    }),

  // ============ Special Types ============

  /**
   * Column reference parameter - allows dynamic column selection
   * Use with caution as it can affect query safety
   */
  column: () => createParamValidator<string, "column">("column"),

  /**
   * JSON parameter - for passing complex structured data
   */
  json: <TShape = unknown>() => createParamValidator<TShape, "JSON">("JSON"),
} as const;

/** Type alias for any parameter validator */
export type AnyParamValidator = ParamValidator<unknown, string, boolean>;

/** Extract the TypeScript type from a parameter validator */
export type InferParamType<T extends AnyParamValidator> = T["_required"] extends true
  ? T["_type"]
  : T["_type"] | undefined;

/** Check if a value is a parameter validator */
export function isParamValidator(value: unknown): value is AnyParamValidator {
  return (
    typeof value === "object" &&
    value !== null &&
    PARAM_BRAND in value &&
    (value as Record<symbol, unknown>)[PARAM_BRAND] === true
  );
}

/** Get the Tinybird type string from a parameter validator */
export function getParamTinybirdType(validator: AnyParamValidator): string {
  return validator._tinybirdType;
}

/** Check if a parameter is required */
export function isParamRequired(validator: AnyParamValidator): boolean {
  return validator._required;
}

/** Get the default value of a parameter */
export function getParamDefault<T>(validator: ParamValidator<T, string, boolean>): T | undefined {
  return validator._default;
}

/** Get the description of a parameter */
export function getParamDescription(validator: AnyParamValidator): string | undefined {
  return validator._description;
}
