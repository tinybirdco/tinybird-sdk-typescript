import { describe, it, expect } from 'vitest';
import { generateDatasource, generateAllDatasources } from './datasource.js';
import { defineDatasource } from '../schema/datasource.js';
import { t } from '../schema/types.js';
import { engine } from '../schema/engines.js';

describe('Datasource Generator', () => {
  describe('generateDatasource', () => {
    it('generates basic datasource with schema', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          count: t.int32(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.name).toBe('test_ds');
      expect(result.content).toContain('SCHEMA >');
      expect(result.content).toContain('id String');
      expect(result.content).toContain('count Int32');
    });

    it('includes description when provided', () => {
      const ds = defineDatasource('test_ds', {
        description: 'Test datasource description',
        schema: {
          id: t.string(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Test datasource description');
    });

    it('includes engine configuration', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
        },
        engine: engine.mergeTree({
          sortingKey: ['id'],
        }),
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('ENGINE "MergeTree"');
      expect(result.content).toContain('ENGINE_SORTING_KEY "id"');
    });

    it('includes partition key in engine config', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          timestamp: t.dateTime(),
        },
        engine: engine.mergeTree({
          sortingKey: ['id'],
          partitionKey: 'toYYYYMM(timestamp)',
        }),
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('ENGINE_PARTITION_KEY "toYYYYMM(timestamp)"');
    });

    it('includes TTL in engine config', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          timestamp: t.dateTime(),
        },
        engine: engine.mergeTree({
          sortingKey: ['id'],
          ttl: 'timestamp + INTERVAL 90 DAY',
        }),
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('ENGINE_TTL "timestamp + INTERVAL 90 DAY"');
    });
  });

  describe('Column formatting', () => {
    it('formats Nullable columns correctly', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          name: t.string().nullable(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('name Nullable(String)');
    });

    it('formats LowCardinality columns correctly', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          country: t.string().lowCardinality(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('country LowCardinality(String)');
    });

    it('formats LowCardinality(Nullable) correctly', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          country: t.string().lowCardinality().nullable(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('country LowCardinality(Nullable(String))');
    });

    it('includes default values', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          status: t.string().default('pending'),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain("status String DEFAULT 'pending'");
    });

    it('includes codec', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          data: t.string().codec('LZ4'),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('data String CODEC(LZ4)');
    });

    it('adds commas between columns except last', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          name: t.string(),
          count: t.int32(),
        },
      });

      const result = generateDatasource(ds);
      const lines = result.content.split('\n');
      const schemaLines = lines.filter(l => l.trim().startsWith('id') || l.trim().startsWith('name') || l.trim().startsWith('count'));

      expect(schemaLines[0]).toContain(',');
      expect(schemaLines[1]).toContain(',');
      expect(schemaLines[2]).not.toContain(',');
    });
  });

  describe('generateAllDatasources', () => {
    it('generates all datasources', () => {
      const ds1 = defineDatasource('ds1', { schema: { id: t.string() } });
      const ds2 = defineDatasource('ds2', { schema: { id: t.int32() } });

      const results = generateAllDatasources({ ds1, ds2 });
      expect(results).toHaveLength(2);
      expect(results.map(r => r.name).sort()).toEqual(['ds1', 'ds2']);
    });
  });

  describe('Full integration', () => {
    it('generates complete datasource file', () => {
      const ds = defineDatasource('page_views', {
        description: 'Page view tracking data',
        schema: {
          timestamp: t.dateTime(),
          pathname: t.string(),
          session_id: t.string(),
          country: t.string().lowCardinality().nullable(),
        },
        engine: engine.mergeTree({
          sortingKey: ['pathname', 'timestamp'],
          partitionKey: 'toYYYYMM(timestamp)',
          ttl: 'timestamp + INTERVAL 90 DAY',
        }),
      });

      const result = generateDatasource(ds);

      expect(result.name).toBe('page_views');
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Page view tracking data');
      expect(result.content).toContain('SCHEMA >');
      expect(result.content).toContain('timestamp DateTime');
      expect(result.content).toContain('pathname String');
      expect(result.content).toContain('session_id String');
      expect(result.content).toContain('country LowCardinality(Nullable(String))');
      expect(result.content).toContain('ENGINE "MergeTree"');
      expect(result.content).toContain('ENGINE_PARTITION_KEY "toYYYYMM(timestamp)"');
      expect(result.content).toContain('ENGINE_SORTING_KEY "pathname, timestamp"');
      expect(result.content).toContain('ENGINE_TTL "timestamp + INTERVAL 90 DAY"');
    });
  });
});
