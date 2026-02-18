/**
 * Type mapping from ClickHouse types to TypeScript SDK validators
 */

/**
 * Parse enum values from an enum type string
 * e.g., "'a' = 1, 'b' = 2" -> ["a", "b"]
 */
function parseEnumValues(enumContent: string): string[] {
  const values: string[] = [];
  const regex = /'([^']+)'\s*=\s*\d+/g;
  let match;
  while ((match = regex.exec(enumContent)) !== null) {
    values.push(match[1]);
  }
  return values;
}

function splitTopLevelComma(input: string): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;

  for (let i = 0; i < input.length; i += 1) {
    const char = input[i];
    const prev = i > 0 ? input[i - 1] : "";

    if (char === "'" && !inDoubleQuote && prev !== "\\") {
      inSingleQuote = !inSingleQuote;
      current += char;
      continue;
    }

    if (char === '"' && !inSingleQuote && prev !== "\\") {
      inDoubleQuote = !inDoubleQuote;
      current += char;
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote) {
      if (char === "(") {
        depth += 1;
        current += char;
        continue;
      }
      if (char === ")") {
        depth -= 1;
        current += char;
        continue;
      }
      if (char === "," && depth === 0) {
        const trimmed = current.trim();
        if (trimmed.length > 0) {
          parts.push(trimmed);
        }
        current = "";
        continue;
      }
    }

    current += char;
  }

  const trimmed = current.trim();
  if (trimmed.length > 0) {
    parts.push(trimmed);
  }

  return parts;
}

/**
 * Map a ClickHouse type to a t.* validator call
 *
 * Handles:
 * - Basic types: String, Int32, Float64, DateTime, etc.
 * - Nullable wrapper: Nullable(String) -> t.string().nullable()
 * - LowCardinality wrapper: LowCardinality(String) -> t.string().lowCardinality()
 * - Parameterized types: DateTime('UTC'), FixedString(10), Decimal(10, 2)
 * - Complex types: Array(String), Map(String, Int32)
 * - Aggregate functions: SimpleAggregateFunction(sum, UInt64)
 */
export function clickhouseTypeToValidator(chType: string): string {
  // Trim whitespace
  chType = chType.trim();

  // Handle Nullable wrapper
  const nullableMatch = chType.match(/^Nullable\((.+)\)$/);
  if (nullableMatch) {
    const innerType = clickhouseTypeToValidator(nullableMatch[1]);
    return `${innerType}.nullable()`;
  }

  // Handle LowCardinality wrapper
  const lowCardMatch = chType.match(/^LowCardinality\((.+)\)$/);
  if (lowCardMatch) {
    const innerType = clickhouseTypeToValidator(lowCardMatch[1]);
    // If inner type already has .nullable(), we need to handle this specially
    // LowCardinality(Nullable(X)) should become t.X().nullable().lowCardinality()
    // But the recursive call already returns t.X().nullable()
    return `${innerType}.lowCardinality()`;
  }

  // Simple type mappings
  const simpleTypeMap: Record<string, string> = {
    String: "t.string()",
    UUID: "t.uuid()",
    Int8: "t.int8()",
    Int16: "t.int16()",
    Int32: "t.int32()",
    Int64: "t.int64()",
    Int128: "t.int128()",
    Int256: "t.int256()",
    UInt8: "t.uint8()",
    UInt16: "t.uint16()",
    UInt32: "t.uint32()",
    UInt64: "t.uint64()",
    UInt128: "t.uint128()",
    UInt256: "t.uint256()",
    Float32: "t.float32()",
    Float64: "t.float64()",
    Bool: "t.bool()",
    Boolean: "t.bool()",
    Date: "t.date()",
    Date32: "t.date32()",
    DateTime: "t.dateTime()",
    JSON: "t.json()",
    Object: "t.json()",
    IPv4: "t.ipv4()",
    IPv6: "t.ipv6()",
  };

  if (simpleTypeMap[chType]) {
    return simpleTypeMap[chType];
  }

  // DateTime with timezone: DateTime('UTC')
  const dtTzMatch = chType.match(/^DateTime\('([^']+)'\)$/);
  if (dtTzMatch) {
    return `t.dateTime("${dtTzMatch[1]}")`;
  }

  // DateTime64 with precision and optional timezone
  const dt64Match = chType.match(/^DateTime64\((\d+)(?:,\s*'([^']+)')?\)$/);
  if (dt64Match) {
    const precision = dt64Match[1];
    const tz = dt64Match[2];
    if (tz) {
      return `t.dateTime64(${precision}, "${tz}")`;
    }
    return `t.dateTime64(${precision})`;
  }

  // DateTime64 without precision (defaults to 3)
  if (chType === "DateTime64") {
    return "t.dateTime64(3)";
  }

  // FixedString(N)
  const fixedMatch = chType.match(/^FixedString\((\d+)\)$/);
  if (fixedMatch) {
    return `t.fixedString(${fixedMatch[1]})`;
  }

  // Decimal(P, S) or Decimal(P)
  const decMatch = chType.match(/^Decimal\((\d+)(?:,\s*(\d+))?\)$/);
  if (decMatch) {
    const precision = decMatch[1];
    const scale = decMatch[2] ?? "0";
    return `t.decimal(${precision}, ${scale})`;
  }

  // Decimal32, Decimal64, Decimal128, Decimal256 with scale
  const decNMatch = chType.match(/^Decimal(32|64|128|256)\((\d+)\)$/);
  if (decNMatch) {
    const bits = decNMatch[1];
    const scale = decNMatch[2];
    const precisionMap: Record<string, number> = {
      "32": 9,
      "64": 18,
      "128": 38,
      "256": 76,
    };
    return `t.decimal(${precisionMap[bits]}, ${scale})`;
  }

  // Array(T)
  const arrMatch = chType.match(/^Array\((.+)\)$/);
  if (arrMatch) {
    const innerType = clickhouseTypeToValidator(arrMatch[1]);
    return `t.array(${innerType})`;
  }

  // Tuple(T1, T2, ...)
  const tupleMatch = chType.match(/^Tuple\((.+)\)$/);
  if (tupleMatch) {
    const tupleArgs = splitTopLevelComma(tupleMatch[1]);
    if (tupleArgs.length === 0) {
      return `t.string() /* TODO: Unknown type: ${chType} */`;
    }
    const tupleTypes = tupleArgs.map((arg) => clickhouseTypeToValidator(arg));
    return `t.tuple(${tupleTypes.join(", ")})`;
  }

  // Map(K, V)
  const mapMatch = chType.match(/^Map\((.+)\)$/);
  if (mapMatch) {
    const mapArgs = splitTopLevelComma(mapMatch[1]);
    if (mapArgs.length !== 2) {
      return `t.string() /* TODO: Unknown type: ${chType} */`;
    }
    const keyType = clickhouseTypeToValidator(mapArgs[0]);
    const valueType = clickhouseTypeToValidator(mapArgs[1]);
    return `t.map(${keyType}, ${valueType})`;
  }

  // Enum8('a' = 1, 'b' = 2)
  const enum8Match = chType.match(/^Enum8\((.+)\)$/);
  if (enum8Match) {
    const values = parseEnumValues(enum8Match[1]);
    if (values.length > 0) {
      return `t.enum8(${values.map((v) => `"${v}"`).join(", ")})`;
    }
    return `t.string() /* Enum8 */`;
  }

  // Enum16('a' = 1, 'b' = 2)
  const enum16Match = chType.match(/^Enum16\((.+)\)$/);
  if (enum16Match) {
    const values = parseEnumValues(enum16Match[1]);
    if (values.length > 0) {
      return `t.enum16(${values.map((v) => `"${v}"`).join(", ")})`;
    }
    return `t.string() /* Enum16 */`;
  }

  // SimpleAggregateFunction(func, T)
  const simpleAggMatch = chType.match(/^SimpleAggregateFunction\((\w+),\s*(.+)\)$/);
  if (simpleAggMatch) {
    const func = simpleAggMatch[1];
    const innerType = clickhouseTypeToValidator(simpleAggMatch[2]);
    return `t.simpleAggregateFunction("${func}", ${innerType})`;
  }

  // AggregateFunction(func, T)
  const aggMatch = chType.match(/^AggregateFunction\((\w+),\s*(.+)\)$/);
  if (aggMatch) {
    const func = aggMatch[1];
    const innerType = clickhouseTypeToValidator(aggMatch[2]);
    return `t.aggregateFunction("${func}", ${innerType})`;
  }

  // AggregateFunction(count)
  const aggNoArgMatch = chType.match(/^AggregateFunction\((\w+)\)$/);
  if (aggNoArgMatch) {
    const func = aggNoArgMatch[1];
    if (func === "count") {
      return 't.aggregateFunction("count", t.uint64())';
    }
    return `t.string() /* TODO: Unknown type: ${chType} */`;
  }

  // Nested - treat as JSON
  if (chType.startsWith("Nested(")) {
    return `t.json() /* ${chType} */`;
  }

  // Fallback for unknown types
  return `t.string() /* TODO: Unknown type: ${chType} */`;
}

/**
 * Map a pipe parameter type to a p.* validator call
 */
export function paramTypeToValidator(
  paramType: string,
  defaultValue?: string | number,
  required: boolean = true
): string {
  // Normalize type
  paramType = paramType.trim();

  // Simple type mappings
  const typeMap: Record<string, string> = {
    String: "p.string()",
    UUID: "p.uuid()",
    Int8: "p.int8()",
    Int16: "p.int16()",
    Int32: "p.int32()",
    Int64: "p.int64()",
    UInt8: "p.uint8()",
    UInt16: "p.uint16()",
    UInt32: "p.uint32()",
    UInt64: "p.uint64()",
    Float32: "p.float32()",
    Float64: "p.float64()",
    Boolean: "p.boolean()",
    Bool: "p.boolean()",
    Date: "p.date()",
    DateTime: "p.dateTime()",
    DateTime64: "p.dateTime64()",
  };

  let validator = typeMap[paramType];

  // Handle parameterized DateTime types
  if (!validator) {
    if (paramType.startsWith("DateTime64")) {
      validator = "p.dateTime64()";
    } else if (paramType.startsWith("DateTime")) {
      validator = "p.dateTime()";
    } else if (paramType.startsWith("Array")) {
      // Array parameters - default to string array
      validator = "p.array(p.string())";
    } else {
      // Default to string for unknown types
      validator = "p.string()";
    }
  }

  // Add optional with default if not required or has a default value
  if (!required || defaultValue !== undefined) {
    if (defaultValue !== undefined) {
      const formattedDefault =
        typeof defaultValue === "string" ? `"${defaultValue}"` : defaultValue;
      // Replace () with .optional(value)
      validator = validator.replace(/\(\)$/, `().optional(${formattedDefault})`);
    } else {
      // Just make it optional without a default
      validator = validator.replace(/\(\)$/, "().optional()");
    }
  }

  return validator;
}
