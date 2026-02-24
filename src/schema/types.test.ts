import { describe, it, expect } from 'vitest';
import { t, isTypeValidator, getTinybirdType, getModifiers } from './types.js';

describe('Type Validators (t.*)', () => {
  describe('Basic types', () => {
    it('generates String type', () => {
      const type = t.string();
      expect(type._tinybirdType).toBe('String');
    });

    it('generates Int32 type', () => {
      const type = t.int32();
      expect(type._tinybirdType).toBe('Int32');
    });

    it('generates DateTime type', () => {
      const type = t.dateTime();
      expect(type._tinybirdType).toBe('DateTime');
    });

    it('generates DateTime with timezone', () => {
      const type = t.dateTime('UTC');
      expect(type._tinybirdType).toBe("DateTime('UTC')");
    });

    it('generates Bool type', () => {
      const type = t.bool();
      expect(type._tinybirdType).toBe('Bool');
    });

    it('generates UUID type', () => {
      const type = t.uuid();
      expect(type._tinybirdType).toBe('UUID');
    });

    it('generates Float64 type', () => {
      const type = t.float64();
      expect(type._tinybirdType).toBe('Float64');
    });

    it('generates UInt64 type', () => {
      const type = t.uint64();
      expect(type._tinybirdType).toBe('UInt64');
    });
  });

  describe('Nullable modifier', () => {
    it('wraps type in Nullable', () => {
      const type = t.string().nullable();
      expect(type._tinybirdType).toBe('Nullable(String)');
    });

    it('wraps Int32 in Nullable', () => {
      const type = t.int32().nullable();
      expect(type._tinybirdType).toBe('Nullable(Int32)');
    });

    it('sets nullable modifier', () => {
      const type = t.string().nullable();
      expect(type._modifiers.nullable).toBe(true);
    });
  });

  describe('LowCardinality modifier', () => {
    it('wraps type in LowCardinality', () => {
      const type = t.string().lowCardinality();
      expect(type._tinybirdType).toBe('LowCardinality(String)');
    });

    it('sets lowCardinality modifier', () => {
      const type = t.string().lowCardinality();
      expect(type._modifiers.lowCardinality).toBe(true);
    });
  });

  describe('LowCardinality + Nullable ordering', () => {
    it('generates LowCardinality(Nullable(X)) when chaining .lowCardinality().nullable()', () => {
      const type = t.string().lowCardinality().nullable();
      expect(type._tinybirdType).toBe('LowCardinality(Nullable(String))');
    });

    it('generates LowCardinality(Nullable(X)) when chaining .nullable().lowCardinality()', () => {
      const type = t.string().nullable().lowCardinality();
      expect(type._tinybirdType).toBe('LowCardinality(Nullable(String))');
    });

    it('preserves both modifiers when chained', () => {
      const type = t.string().lowCardinality().nullable();
      expect(type._modifiers.lowCardinality).toBe(true);
      expect(type._modifiers.nullable).toBe(true);
    });
  });

  describe('Default values', () => {
    it('sets hasDefault modifier', () => {
      const type = t.string().default('test');
      expect(type._modifiers.hasDefault).toBe(true);
    });

    it('stores defaultValue in modifiers', () => {
      const type = t.string().default('test');
      expect(type._modifiers.defaultValue).toBe('test');
    });

    it('works with numeric defaults', () => {
      const type = t.int32().default(42);
      expect(type._modifiers.defaultValue).toBe(42);
    });
  });

  describe('Codec modifier', () => {
    it('sets codec in modifiers', () => {
      const type = t.string().codec('LZ4');
      expect(type._modifiers.codec).toBe('LZ4');
    });
  });

  describe('jsonPath modifier', () => {
    it('sets jsonPath in modifiers', () => {
      const type = t.string().jsonPath('$.payload.id');
      expect(type._modifiers.jsonPath).toBe('$.payload.id');
    });

    it('supports chaining with other modifiers', () => {
      const type = t.string().nullable().jsonPath('$.user.name');
      expect(type._tinybirdType).toBe('Nullable(String)');
      expect(type._modifiers.nullable).toBe(true);
      expect(type._modifiers.jsonPath).toBe('$.user.name');
    });
  });

  describe('Complex types', () => {
    it('generates Array type', () => {
      const type = t.array(t.string());
      expect(type._tinybirdType).toBe('Array(String)');
    });

    it('generates nested Array type', () => {
      const type = t.array(t.int32());
      expect(type._tinybirdType).toBe('Array(Int32)');
    });

    it('generates Map type', () => {
      const type = t.map(t.string(), t.int32());
      expect(type._tinybirdType).toBe('Map(String, Int32)');
    });

    it('generates Decimal type', () => {
      const type = t.decimal(10, 2);
      expect(type._tinybirdType).toBe('Decimal(10, 2)');
    });

    it('generates FixedString type', () => {
      const type = t.fixedString(3);
      expect(type._tinybirdType).toBe('FixedString(3)');
    });

    it('generates Tuple type', () => {
      const type = t.tuple(t.string(), t.int32());
      expect(type._tinybirdType).toBe('Tuple(String, Int32)');
    });

    it('generates DateTime64 type', () => {
      const type = t.dateTime64(3);
      expect(type._tinybirdType).toBe('DateTime64(3)');
    });

    it('generates DateTime64 with timezone', () => {
      const type = t.dateTime64(3, 'UTC');
      expect(type._tinybirdType).toBe("DateTime64(3, 'UTC')");
    });
  });

  describe('Helper functions', () => {
    it('isTypeValidator returns true for validators', () => {
      expect(isTypeValidator(t.string())).toBe(true);
    });

    it('isTypeValidator returns false for non-validators', () => {
      expect(isTypeValidator('string')).toBe(false);
      expect(isTypeValidator({})).toBe(false);
      expect(isTypeValidator(null)).toBe(false);
    });

    it('getTinybirdType returns type string', () => {
      expect(getTinybirdType(t.string())).toBe('String');
    });

    it('getModifiers returns modifiers object', () => {
      const modifiers = getModifiers(t.string().nullable());
      expect(modifiers.nullable).toBe(true);
    });
  });

  describe('Chained modifiers', () => {
    it('supports multiple modifiers', () => {
      const type = t.string().lowCardinality().default('test');
      expect(type._tinybirdType).toBe('LowCardinality(String)');
      expect(type._modifiers.lowCardinality).toBe(true);
      expect(type._modifiers.hasDefault).toBe(true);
      expect(type._modifiers.defaultValue).toBe('test');
    });
  });

  describe('Enum types', () => {
    it('generates Enum8 with value mapping', () => {
      const type = t.enum8('active', 'inactive', 'pending');
      expect(type._tinybirdType).toBe("Enum8('active' = 1, 'inactive' = 2, 'pending' = 3)");
    });

    it('generates Enum16 with value mapping', () => {
      const type = t.enum16('draft', 'published', 'archived');
      expect(type._tinybirdType).toBe("Enum16('draft' = 1, 'published' = 2, 'archived' = 3)");
    });

    it('escapes single quotes in enum values', () => {
      const type = t.enum8("it's ok", 'normal');
      expect(type._tinybirdType).toBe("Enum8('it\\'s ok' = 1, 'normal' = 2)");
    });

    it('handles single enum value', () => {
      const type = t.enum8('only');
      expect(type._tinybirdType).toBe("Enum8('only' = 1)");
    });
  });

  describe('Generic type parameters (branded types)', () => {
    it('string with generic produces same runtime type', () => {
      type UserId = string & { readonly __brand: 'UserId' };
      const plain = t.string();
      const branded = t.string<UserId>();
      expect(branded._tinybirdType).toBe(plain._tinybirdType);
      expect(branded._tinybirdType).toBe('String');
    });

    it('int32 with generic produces same runtime type', () => {
      type Count = number & { readonly __brand: 'Count' };
      const plain = t.int32();
      const branded = t.int32<Count>();
      expect(branded._tinybirdType).toBe(plain._tinybirdType);
      expect(branded._tinybirdType).toBe('Int32');
    });

    it('uuid with generic produces same runtime type', () => {
      type TraceId = string & { readonly __brand: 'TraceId' };
      const branded = t.uuid<TraceId>();
      expect(branded._tinybirdType).toBe('UUID');
    });

    it('dateTime with generic produces same runtime type', () => {
      type Timestamp = string & { readonly __brand: 'Timestamp' };
      const branded = t.dateTime<Timestamp>();
      expect(branded._tinybirdType).toBe('DateTime');
    });

    it('dateTime with timezone and generic produces same runtime type', () => {
      type Timestamp = string & { readonly __brand: 'Timestamp' };
      const branded = t.dateTime<Timestamp>('UTC');
      expect(branded._tinybirdType).toBe("DateTime('UTC')");
    });

    it('bool with generic produces same runtime type', () => {
      type IsActive = boolean & { readonly __brand: 'IsActive' };
      const branded = t.bool<IsActive>();
      expect(branded._tinybirdType).toBe('Bool');
    });

    it('int128 with generic produces same runtime type', () => {
      type BigId = bigint & { readonly __brand: 'BigId' };
      const branded = t.int128<BigId>();
      expect(branded._tinybirdType).toBe('Int128');
    });

    it('decimal with generic produces same runtime type', () => {
      type Price = number & { readonly __brand: 'Price' };
      const branded = t.decimal<Price>(10, 2);
      expect(branded._tinybirdType).toBe('Decimal(10, 2)');
    });

    it('fixedString with generic produces same runtime type', () => {
      type CountryCode = string & { readonly __brand: 'CountryCode' };
      const branded = t.fixedString<CountryCode>(2);
      expect(branded._tinybirdType).toBe('FixedString(2)');
    });

    it('branded validators support nullable modifier', () => {
      type UserId = string & { readonly __brand: 'UserId' };
      const branded = t.string<UserId>().nullable();
      expect(branded._tinybirdType).toBe('Nullable(String)');
      expect(branded._modifiers.nullable).toBe(true);
    });

    it('branded validators support lowCardinality modifier', () => {
      type StatusCode = string & { readonly __brand: 'StatusCode' };
      const branded = t.string<StatusCode>().lowCardinality();
      expect(branded._tinybirdType).toBe('LowCardinality(String)');
    });

    it('branded validators support default values', () => {
      type StatusCode = string & { readonly __brand: 'StatusCode' };
      const branded = t.string<StatusCode>().default('active' as StatusCode);
      expect(branded._modifiers.hasDefault).toBe(true);
      expect(branded._modifiers.defaultValue).toBe('active');
    });
  });
});
