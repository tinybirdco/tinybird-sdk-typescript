import { describe, it, expect } from 'vitest';
import { p, isParamValidator, getParamTinybirdType, isParamRequired, getParamDefault, getParamDescription } from './params.js';

describe('Parameter Validators (p.*)', () => {
  describe('Basic types', () => {
    it('generates String parameter', () => {
      const param = p.string();
      expect(param._tinybirdType).toBe('String');
    });

    it('generates Int32 parameter', () => {
      const param = p.int32();
      expect(param._tinybirdType).toBe('Int32');
    });

    it('generates DateTime parameter', () => {
      const param = p.dateTime();
      expect(param._tinybirdType).toBe('DateTime');
    });

    it('generates Float64 parameter', () => {
      const param = p.float64();
      expect(param._tinybirdType).toBe('Float64');
    });

    it('generates Boolean parameter', () => {
      const param = p.boolean();
      expect(param._tinybirdType).toBe('Boolean');
    });

    it('generates UUID parameter', () => {
      const param = p.uuid();
      expect(param._tinybirdType).toBe('UUID');
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
