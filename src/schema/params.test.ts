import { describe, it, expect } from 'vitest';
import { p, isParamValidator, getParamTinybirdType, isParamRequired, getParamDefault, getParamDescription } from './params.js';

describe('Parameter Validators (p.*)', () => {
  describe('String types', () => {
    it('generates String parameter', () => {
      const param = p.string();
      expect(param._tinybirdType).toBe('String');
    });

    it('generates UUID parameter', () => {
      const param = p.uuid();
      expect(param._tinybirdType).toBe('UUID');
    });
  });

  describe('Integer types', () => {
    it('generates Int8 parameter', () => {
      const param = p.int8();
      expect(param._tinybirdType).toBe('Int8');
    });

    it('generates Int16 parameter', () => {
      const param = p.int16();
      expect(param._tinybirdType).toBe('Int16');
    });

    it('generates Int32 parameter', () => {
      const param = p.int32();
      expect(param._tinybirdType).toBe('Int32');
    });

    it('generates Int64 parameter', () => {
      const param = p.int64();
      expect(param._tinybirdType).toBe('Int64');
    });

    it('generates UInt8 parameter', () => {
      const param = p.uint8();
      expect(param._tinybirdType).toBe('UInt8');
    });

    it('generates UInt16 parameter', () => {
      const param = p.uint16();
      expect(param._tinybirdType).toBe('UInt16');
    });

    it('generates UInt32 parameter', () => {
      const param = p.uint32();
      expect(param._tinybirdType).toBe('UInt32');
    });

    it('generates UInt64 parameter', () => {
      const param = p.uint64();
      expect(param._tinybirdType).toBe('UInt64');
    });
  });

  describe('Float types', () => {
    it('generates Float32 parameter', () => {
      const param = p.float32();
      expect(param._tinybirdType).toBe('Float32');
    });

    it('generates Float64 parameter', () => {
      const param = p.float64();
      expect(param._tinybirdType).toBe('Float64');
    });
  });

  describe('Boolean type', () => {
    it('generates Boolean parameter', () => {
      const param = p.boolean();
      expect(param._tinybirdType).toBe('Boolean');
    });
  });

  describe('Date/Time types', () => {
    it('generates Date parameter', () => {
      const param = p.date();
      expect(param._tinybirdType).toBe('Date');
    });

    it('generates DateTime parameter', () => {
      const param = p.dateTime();
      expect(param._tinybirdType).toBe('DateTime');
    });

    it('generates DateTime64 parameter', () => {
      const param = p.dateTime64();
      expect(param._tinybirdType).toBe('DateTime64');
    });
  });

  describe('Special types', () => {
    it('generates column parameter', () => {
      const param = p.column();
      expect(param._tinybirdType).toBe('column');
    });
  });

  describe('Required by default', () => {
    it('parameters are required by default', () => {
      const param = p.string();
      expect(param._required).toBe(true);
    });
  });

  describe('Optional modifier', () => {
    it('makes parameter optional without default', () => {
      const param = p.int32().optional();
      expect(param._required).toBe(false);
      expect(param._default).toBeUndefined();
    });

    it('makes parameter optional with default value', () => {
      const param = p.int32().optional(10);
      expect(param._required).toBe(false);
      expect(param._default).toBe(10);
    });

    it('preserves type when optional', () => {
      const param = p.string().optional('default');
      expect(param._tinybirdType).toBe('String');
      expect(param._default).toBe('default');
    });
  });

  describe('Required modifier', () => {
    it('makes optional parameter required again', () => {
      const param = p.int32().optional(10).required();
      expect(param._required).toBe(true);
    });
  });

  describe('Description modifier', () => {
    it('sets description', () => {
      const param = p.string().describe('User ID');
      expect(param._description).toBe('User ID');
    });

    it('preserves description when chaining', () => {
      const param = p.int32().describe('Limit value').optional(10);
      expect(param._description).toBe('Limit value');
    });
  });

  describe('Combined modifiers', () => {
    it('supports optional with description', () => {
      const param = p.int32().optional(10).describe('Limit');
      expect(param._required).toBe(false);
      expect(param._default).toBe(10);
      expect(param._description).toBe('Limit');
    });

    it('supports description then optional', () => {
      const param = p.int32().describe('Limit').optional(10);
      expect(param._required).toBe(false);
      expect(param._default).toBe(10);
      expect(param._description).toBe('Limit');
    });
  });

  describe('Helper functions', () => {
    it('isParamValidator returns true for validators', () => {
      expect(isParamValidator(p.string())).toBe(true);
    });

    it('isParamValidator returns false for non-validators', () => {
      expect(isParamValidator('string')).toBe(false);
      expect(isParamValidator({})).toBe(false);
      expect(isParamValidator(null)).toBe(false);
    });

    it('getParamTinybirdType returns type string', () => {
      expect(getParamTinybirdType(p.string())).toBe('String');
    });

    it('isParamRequired returns required status', () => {
      expect(isParamRequired(p.string())).toBe(true);
      expect(isParamRequired(p.string().optional())).toBe(false);
    });

    it('getParamDefault returns default value', () => {
      expect(getParamDefault(p.int32().optional(10))).toBe(10);
      expect(getParamDefault(p.int32())).toBeUndefined();
    });

    it('getParamDescription returns description', () => {
      expect(getParamDescription(p.string().describe('Test'))).toBe('Test');
      expect(getParamDescription(p.string())).toBeUndefined();
    });
  });

  describe('Array parameter', () => {
    it('creates array parameter', () => {
      const param = p.array(p.string());
      expect(param._tinybirdType).toBe('Array');
    });
  });

  describe('JSON parameter', () => {
    it('creates JSON parameter', () => {
      const param = p.json();
      expect(param._tinybirdType).toBe('JSON');
    });
  });
});
