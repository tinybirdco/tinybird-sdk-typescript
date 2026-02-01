/**
 * Column type validators for Tinybird datasources
 * Similar to Convex's `v.*` pattern, but for ClickHouse types
 */

// Symbol for brand typing
const VALIDATOR_BRAND = Symbol("tinybird.validator");

/**
 * Base interface for all type validators
 * The phantom types enable TypeScript to infer the correct types
 */
export interface TypeValidator<
  TType,
  TTinybirdType extends string = string,
  TModifiers extends TypeModifiers = TypeModifiers
> {
  readonly [VALIDATOR_BRAND]: true;
  /** The inferred TypeScript type */
  readonly _type: TType;
  /** The Tinybird/ClickHouse type string */
  readonly _tinybirdType: TTinybirdType;
  /** Metadata about modifiers applied */
  readonly _modifiers: TModifiers;

  /** Make this column nullable */
  nullable(): TypeValidator<TType | null, `Nullable(${TTinybirdType})`, TModifiers & { nullable: true }>;
  /** Apply LowCardinality optimization (for strings with few unique values) */
  lowCardinality(): TypeValidator<TType, `LowCardinality(${TTinybirdType})`, TModifiers & { lowCardinality: true }>;
  /** Set a default value for the column */
  default(value: TType): TypeValidator<TType, TTinybirdType, TModifiers & { hasDefault: true; defaultValue: TType }>;
  /** Set a codec for compression */
  codec(codec: string): TypeValidator<TType, TTinybirdType, TModifiers & { codec: string }>;
}

export interface TypeModifiers {
  nullable?: boolean;
  lowCardinality?: boolean;
  hasDefault?: boolean;
  defaultValue?: unknown;
  codec?: string;
}

// Internal implementation
interface ValidatorImpl<TType, TTinybirdType extends string, TModifiers extends TypeModifiers>
  extends TypeValidator<TType, TTinybirdType, TModifiers> {
  readonly tinybirdType: TTinybirdType;
  readonly modifiers: TModifiers;
}

function createValidator<TType, TTinybirdType extends string>(
  tinybirdType: TTinybirdType,
  modifiers: TypeModifiers = {}
): TypeValidator<TType, TTinybirdType, TypeModifiers> {
  const validator: ValidatorImpl<TType, TTinybirdType, TypeModifiers> = {
    [VALIDATOR_BRAND]: true,
    _type: undefined as unknown as TType,
    _tinybirdType: tinybirdType,
    _modifiers: modifiers,
    tinybirdType,
    modifiers,

    nullable() {
      // If already has LowCardinality, we need to move Nullable inside
      // ClickHouse requires: LowCardinality(Nullable(X)), not Nullable(LowCardinality(X))
      if (modifiers.lowCardinality) {
        // Extract base type from LowCardinality(X) and wrap as LowCardinality(Nullable(X))
        const baseType = tinybirdType.replace(/^LowCardinality\((.+)\)$/, '$1');
        const newType = `LowCardinality(Nullable(${baseType}))`;
        return createValidator<TType | null, `LowCardinality(Nullable(${string}))`>(
          newType as `LowCardinality(Nullable(${string}))`,
          { ...modifiers, nullable: true }
        ) as unknown as TypeValidator<TType | null, `Nullable(${TTinybirdType})`, TypeModifiers & { nullable: true }>;
      }
      return createValidator<TType | null, `Nullable(${TTinybirdType})`>(
        `Nullable(${tinybirdType})` as `Nullable(${TTinybirdType})`,
        { ...modifiers, nullable: true }
      ) as TypeValidator<TType | null, `Nullable(${TTinybirdType})`, TypeModifiers & { nullable: true }>;
    },

    lowCardinality() {
      // If already nullable, wrap as LowCardinality(Nullable(X))
      if (modifiers.nullable) {
        // Extract base type from Nullable(X) and wrap as LowCardinality(Nullable(X))
        const baseType = tinybirdType.replace(/^Nullable\((.+)\)$/, '$1');
        const newType = `LowCardinality(Nullable(${baseType}))`;
        return createValidator<TType, `LowCardinality(Nullable(${string}))`>(
          newType as `LowCardinality(Nullable(${string}))`,
          { ...modifiers, lowCardinality: true }
        ) as unknown as TypeValidator<TType, `LowCardinality(${TTinybirdType})`, TypeModifiers & { lowCardinality: true }>;
      }
      return createValidator<TType, `LowCardinality(${TTinybirdType})`>(
        `LowCardinality(${tinybirdType})` as `LowCardinality(${TTinybirdType})`,
        { ...modifiers, lowCardinality: true }
      ) as TypeValidator<TType, `LowCardinality(${TTinybirdType})`, TypeModifiers & { lowCardinality: true }>;
    },

    default(value: TType) {
      return createValidator<TType, TTinybirdType>(tinybirdType, {
        ...modifiers,
        hasDefault: true,
        defaultValue: value,
      }) as TypeValidator<TType, TTinybirdType, TypeModifiers & { hasDefault: true; defaultValue: TType }>;
    },

    codec(codec: string) {
      return createValidator<TType, TTinybirdType>(tinybirdType, {
        ...modifiers,
        codec,
      }) as TypeValidator<TType, TTinybirdType, TypeModifiers & { codec: string }>;
    },
  };

  return validator;
}

/**
 * Type validators for Tinybird columns
 *
 * @example
 * ```ts
 * import { t } from '@tinybirdco/sdk';
 *
 * const schema = {
 *   id: t.string(),
 *   count: t.int32(),
 *   timestamp: t.dateTime(),
 *   tags: t.array(t.string()),
 *   metadata: t.json(),
 * };
 * ```
 */
export const t = {
  // ============ String Types ============

  /** String type - variable length UTF-8 string */
  string: () => createValidator<string, "String">("String"),

  /** FixedString(N) - fixed length string, padded with null bytes */
  fixedString: (length: number) =>
    createValidator<string, `FixedString(${number})`>(`FixedString(${length})`),

  /** UUID - 16-byte universally unique identifier */
  uuid: () => createValidator<string, "UUID">("UUID"),

  // ============ Integer Types ============

  /** Int8 - signed 8-bit integer (-128 to 127) */
  int8: () => createValidator<number, "Int8">("Int8"),

  /** Int16 - signed 16-bit integer */
  int16: () => createValidator<number, "Int16">("Int16"),

  /** Int32 - signed 32-bit integer */
  int32: () => createValidator<number, "Int32">("Int32"),

  /** Int64 - signed 64-bit integer (represented as number, may lose precision) */
  int64: () => createValidator<number, "Int64">("Int64"),

  /** Int128 - signed 128-bit integer (represented as bigint) */
  int128: () => createValidator<bigint, "Int128">("Int128"),

  /** Int256 - signed 256-bit integer (represented as bigint) */
  int256: () => createValidator<bigint, "Int256">("Int256"),

  /** UInt8 - unsigned 8-bit integer (0 to 255) */
  uint8: () => createValidator<number, "UInt8">("UInt8"),

  /** UInt16 - unsigned 16-bit integer */
  uint16: () => createValidator<number, "UInt16">("UInt16"),

  /** UInt32 - unsigned 32-bit integer */
  uint32: () => createValidator<number, "UInt32">("UInt32"),

  /** UInt64 - unsigned 64-bit integer (represented as number, may lose precision) */
  uint64: () => createValidator<number, "UInt64">("UInt64"),

  /** UInt128 - unsigned 128-bit integer (represented as bigint) */
  uint128: () => createValidator<bigint, "UInt128">("UInt128"),

  /** UInt256 - unsigned 256-bit integer (represented as bigint) */
  uint256: () => createValidator<bigint, "UInt256">("UInt256"),

  // ============ Float Types ============

  /** Float32 - 32-bit floating point */
  float32: () => createValidator<number, "Float32">("Float32"),

  /** Float64 - 64-bit floating point (double precision) */
  float64: () => createValidator<number, "Float64">("Float64"),

  /** Decimal(precision, scale) - fixed-point decimal number */
  decimal: (precision: number, scale: number) =>
    createValidator<number, `Decimal(${number}, ${number})`>(
      `Decimal(${precision}, ${scale})`
    ),

  // ============ Boolean ============

  /** Bool - boolean value (true/false) */
  bool: () => createValidator<boolean, "Bool">("Bool"),

  // ============ Date/Time Types ============

  /** Date - date without time (YYYY-MM-DD) */
  date: () => createValidator<Date, "Date">("Date"),

  /** Date32 - extended date range */
  date32: () => createValidator<Date, "Date32">("Date32"),

  /** DateTime - date and time with second precision */
  dateTime: (timezone?: string) =>
    timezone
      ? createValidator<Date, `DateTime('${string}')`>(`DateTime('${timezone}')`)
      : createValidator<Date, "DateTime">("DateTime"),

  /** DateTime64 - date and time with sub-second precision */
  dateTime64: (precision: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 = 3, timezone?: string) =>
    timezone
      ? createValidator<Date, `DateTime64(${number}, '${string}')`>(
          `DateTime64(${precision}, '${timezone}')`
        )
      : createValidator<Date, `DateTime64(${number})`>(`DateTime64(${precision})`),

  // ============ Complex Types ============

  /** Array(T) - array of elements of type T */
  array: <TElement extends TypeValidator<unknown, string, TypeModifiers>>(
    element: TElement
  ): TypeValidator<
    TElement["_type"][],
    `Array(${TElement["_tinybirdType"]})`,
    TypeModifiers
  > =>
    createValidator<TElement["_type"][], `Array(${TElement["_tinybirdType"]})`>(
      `Array(${element._tinybirdType})` as `Array(${TElement["_tinybirdType"]})`
    ),

  /** Tuple(T1, T2, ...) - tuple of heterogeneous types */
  tuple: <TElements extends readonly TypeValidator<unknown, string, TypeModifiers>[]>(
    ...elements: TElements
  ): TypeValidator<
    { [K in keyof TElements]: TElements[K]["_type"] },
    `Tuple(${string})`,
    TypeModifiers
  > =>
    createValidator<
      { [K in keyof TElements]: TElements[K]["_type"] },
      `Tuple(${string})`
    >(`Tuple(${elements.map((e) => e._tinybirdType).join(", ")})`),

  /** Map(K, V) - dictionary/map type */
  map: <
    TKey extends TypeValidator<string | number, string, TypeModifiers>,
    TValue extends TypeValidator<unknown, string, TypeModifiers>
  >(
    keyType: TKey,
    valueType: TValue
  ): TypeValidator<
    Map<TKey["_type"], TValue["_type"]>,
    `Map(${TKey["_tinybirdType"]}, ${TValue["_tinybirdType"]})`,
    TypeModifiers
  > =>
    createValidator<
      Map<TKey["_type"], TValue["_type"]>,
      `Map(${TKey["_tinybirdType"]}, ${TValue["_tinybirdType"]})`
    >(`Map(${keyType._tinybirdType}, ${valueType._tinybirdType})`),

  /** JSON - semi-structured JSON data */
  json: <TShape = unknown>() => createValidator<TShape, "JSON">("JSON"),

  // ============ Enum Types ============

  /** Enum8 - enumeration stored as Int8 */
  enum8: <TValues extends readonly string[]>(...values: TValues) => {
    const enumMapping = values
      .map((v, i) => `'${v.replace(/'/g, "\\'")}' = ${i + 1}`)
      .join(", ");
    return createValidator<TValues[number], `Enum8(${string})`>(
      `Enum8(${enumMapping})` as `Enum8(${string})`
    );
  },

  /** Enum16 - enumeration stored as Int16 */
  enum16: <TValues extends readonly string[]>(...values: TValues) => {
    const enumMapping = values
      .map((v, i) => `'${v.replace(/'/g, "\\'")}' = ${i + 1}`)
      .join(", ");
    return createValidator<TValues[number], `Enum16(${string})`>(
      `Enum16(${enumMapping})` as `Enum16(${string})`
    );
  },

  // ============ Special Types ============

  /** IPv4 - IPv4 address */
  ipv4: () => createValidator<string, "IPv4">("IPv4"),

  /** IPv6 - IPv6 address */
  ipv6: () => createValidator<string, "IPv6">("IPv6"),

  // ============ Aggregate Function States ============

  /** SimpleAggregateFunction - for materialized views with simple aggregates */
  simpleAggregateFunction: <
    TFunc extends string,
    TType extends TypeValidator<unknown, string, TypeModifiers>
  >(
    func: TFunc,
    type: TType
  ): TypeValidator<
    TType["_type"],
    `SimpleAggregateFunction(${TFunc}, ${TType["_tinybirdType"]})`,
    TypeModifiers
  > =>
    createValidator<
      TType["_type"],
      `SimpleAggregateFunction(${TFunc}, ${TType["_tinybirdType"]})`
    >(`SimpleAggregateFunction(${func}, ${type._tinybirdType})`),

  /** AggregateFunction - for materialized views with complex aggregates */
  aggregateFunction: <
    TFunc extends string,
    TType extends TypeValidator<unknown, string, TypeModifiers>
  >(
    func: TFunc,
    type: TType
  ): TypeValidator<
    TType["_type"],
    `AggregateFunction(${TFunc}, ${TType["_tinybirdType"]})`,
    TypeModifiers
  > =>
    createValidator<
      TType["_type"],
      `AggregateFunction(${TFunc}, ${TType["_tinybirdType"]})`
    >(`AggregateFunction(${func}, ${type._tinybirdType})`),
} as const;

/** Type alias for any type validator */
export type AnyTypeValidator = TypeValidator<unknown, string, TypeModifiers>;

/** Extract the TypeScript type from a type validator */
export type InferType<T extends AnyTypeValidator> = T["_type"];

/** Extract the Tinybird type string from a type validator */
export type TinybirdType<T extends AnyTypeValidator> = T["_tinybirdType"];

/** Helper to check if a value is a type validator */
export function isTypeValidator(value: unknown): value is AnyTypeValidator {
  return (
    typeof value === "object" &&
    value !== null &&
    VALIDATOR_BRAND in value &&
    (value as Record<symbol, unknown>)[VALIDATOR_BRAND] === true
  );
}

/** Get the Tinybird type string from a validator */
export function getTinybirdType(validator: AnyTypeValidator): string {
  return validator._tinybirdType;
}

/** Get the modifiers from a validator */
export function getModifiers(validator: AnyTypeValidator): TypeModifiers {
  return validator._modifiers;
}
