import { describe, it, expect } from 'vitest';
import { engine, getEngineClause, getSortingKey, getPrimaryKey } from './engines.js';

describe('Engine Configurations', () => {
  describe('MergeTree', () => {
    it('creates MergeTree config', () => {
      const config = engine.mergeTree({ sortingKey: ['id'] });
      expect(config.type).toBe('MergeTree');
      expect(config.sortingKey).toEqual(['id']);
    });

    it('supports string sortingKey', () => {
      const config = engine.mergeTree({ sortingKey: 'id' });
      expect(config.sortingKey).toBe('id');
    });

    it('supports partitionKey', () => {
      const config = engine.mergeTree({
        sortingKey: ['id'],
        partitionKey: 'toYYYYMM(timestamp)',
      });
      expect(config.partitionKey).toBe('toYYYYMM(timestamp)');
    });

    it('supports TTL', () => {
      const config = engine.mergeTree({
        sortingKey: ['id'],
        ttl: 'timestamp + INTERVAL 90 DAY',
      });
      expect(config.ttl).toBe('timestamp + INTERVAL 90 DAY');
    });
  });

  describe('ReplacingMergeTree', () => {
    it('creates ReplacingMergeTree config', () => {
      const config = engine.replacingMergeTree({ sortingKey: ['id'] });
      expect(config.type).toBe('ReplacingMergeTree');
    });

    it('supports version column', () => {
      const config = engine.replacingMergeTree({
        sortingKey: ['id'],
        ver: 'updated_at',
      });
      expect(config.ver).toBe('updated_at');
    });
  });

  describe('SummingMergeTree', () => {
    it('creates SummingMergeTree config', () => {
      const config = engine.summingMergeTree({ sortingKey: ['id'] });
      expect(config.type).toBe('SummingMergeTree');
    });

    it('supports columns to sum', () => {
      const config = engine.summingMergeTree({
        sortingKey: ['date'],
        columns: ['count', 'total'],
      });
      expect(config.columns).toEqual(['count', 'total']);
    });
  });

  describe('AggregatingMergeTree', () => {
    it('creates AggregatingMergeTree config', () => {
      const config = engine.aggregatingMergeTree({ sortingKey: ['id'] });
      expect(config.type).toBe('AggregatingMergeTree');
    });
  });

  describe('CollapsingMergeTree', () => {
    it('creates CollapsingMergeTree config', () => {
      const config = engine.collapsingMergeTree({
        sortingKey: ['id'],
        sign: 'sign_col',
      });
      expect(config.type).toBe('CollapsingMergeTree');
      expect(config.sign).toBe('sign_col');
    });
  });

  describe('VersionedCollapsingMergeTree', () => {
    it('creates VersionedCollapsingMergeTree config', () => {
      const config = engine.versionedCollapsingMergeTree({
        sortingKey: ['id'],
        sign: 'sign_col',
        version: 'version_col',
      });
      expect(config.type).toBe('VersionedCollapsingMergeTree');
      expect(config.sign).toBe('sign_col');
      expect(config.version).toBe('version_col');
    });
  });

  describe('getEngineClause', () => {
    it('generates basic MergeTree clause', () => {
      const config = engine.mergeTree({ sortingKey: ['id'] });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE "MergeTree"');
      expect(clause).toContain('ENGINE_SORTING_KEY "id"');
    });

    it('includes partition key', () => {
      const config = engine.mergeTree({
        sortingKey: ['id'],
        partitionKey: 'toYYYYMM(timestamp)',
      });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE_PARTITION_KEY "toYYYYMM(timestamp)"');
    });

    it('includes TTL', () => {
      const config = engine.mergeTree({
        sortingKey: ['id'],
        ttl: 'timestamp + INTERVAL 90 DAY',
      });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE_TTL "timestamp + INTERVAL 90 DAY"');
    });

    it('includes primary key when different from sorting key', () => {
      const config = engine.mergeTree({
        sortingKey: ['id', 'timestamp'],
        primaryKey: ['id'],
      });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE_SORTING_KEY "id, timestamp"');
      expect(clause).toContain('ENGINE_PRIMARY_KEY "id"');
    });

    it('includes ReplacingMergeTree version column', () => {
      const config = engine.replacingMergeTree({
        sortingKey: ['id'],
        ver: 'updated_at',
      });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE "ReplacingMergeTree"');
      expect(clause).toContain('ENGINE_VER "updated_at"');
    });

    it('includes SummingMergeTree columns', () => {
      const config = engine.summingMergeTree({
        sortingKey: ['date'],
        columns: ['count', 'total'],
      });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE "SummingMergeTree"');
      expect(clause).toContain('ENGINE_SUMMING_COLUMNS "count, total"');
    });

    it('includes CollapsingMergeTree sign column', () => {
      const config = engine.collapsingMergeTree({
        sortingKey: ['id'],
        sign: 'sign_col',
      });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE "CollapsingMergeTree"');
      expect(clause).toContain('ENGINE_SIGN "sign_col"');
    });

    it('includes VersionedCollapsingMergeTree sign and version', () => {
      const config = engine.versionedCollapsingMergeTree({
        sortingKey: ['id'],
        sign: 'sign_col',
        version: 'version_col',
      });
      const clause = getEngineClause(config);
      expect(clause).toContain('ENGINE "VersionedCollapsingMergeTree"');
      expect(clause).toContain('ENGINE_SIGN "sign_col"');
      expect(clause).toContain('ENGINE_VERSION "version_col"');
    });
  });

  describe('Helper functions', () => {
    it('getSortingKey returns array from string', () => {
      const config = engine.mergeTree({ sortingKey: 'id' });
      expect(getSortingKey(config)).toEqual(['id']);
    });

    it('getSortingKey returns array from array', () => {
      const config = engine.mergeTree({ sortingKey: ['id', 'timestamp'] });
      expect(getSortingKey(config)).toEqual(['id', 'timestamp']);
    });

    it('getPrimaryKey defaults to sorting key', () => {
      const config = engine.mergeTree({ sortingKey: ['id'] });
      expect(getPrimaryKey(config)).toEqual(['id']);
    });

    it('getPrimaryKey returns explicit primary key', () => {
      const config = engine.mergeTree({
        sortingKey: ['id', 'timestamp'],
        primaryKey: ['id'],
      });
      expect(getPrimaryKey(config)).toEqual(['id']);
    });
  });
});
