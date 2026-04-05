import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { DataLayer, BundleScopedData, BundleIsolationError, ValidationError } from '../index.js';

describe('DataLayer', () => {
  let dl;

  beforeEach(() => {
    dl = new DataLayer(':memory:');
    dl.registerSchema('testbundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          name: { type: 'string', null: false },
          value: { type: 'integer', default: 0 },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
    });
  });

  describe('insert', () => {
    it('inserts a record and returns it with generated id', () => {
      const record = dl.insert('testbundle', 'items', { name: 'test' });
      assert.ok(record.id);
      assert.equal(record.name, 'test');
      assert.ok(record.created_at);
      assert.ok(record.updated_at);
    });

    it('uses provided id if given', () => {
      const record = dl.insert('testbundle', 'items', { id: 'custom-id', name: 'test' });
      assert.equal(record.id, 'custom-id');
    });
  });

  describe('find', () => {
    it('finds a record by id', () => {
      const inserted = dl.insert('testbundle', 'items', { name: 'findme' });
      const found = dl.find('testbundle', 'items', inserted.id);
      assert.equal(found.name, 'findme');
    });

    it('returns null for missing id', () => {
      const found = dl.find('testbundle', 'items', 'nonexistent');
      assert.equal(found, null);
    });
  });

  describe('query', () => {
    it('queries with filters', () => {
      dl.insert('testbundle', 'items', { name: 'a', value: 1 });
      dl.insert('testbundle', 'items', { name: 'b', value: 2 });
      dl.insert('testbundle', 'items', { name: 'c', value: 1 });

      const results = dl.query('testbundle', 'items', { value: 1 });
      assert.equal(results.length, 2);
    });

    it('queries with order and limit', () => {
      dl.insert('testbundle', 'items', { name: 'a', value: 3 });
      dl.insert('testbundle', 'items', { name: 'b', value: 1 });
      dl.insert('testbundle', 'items', { name: 'c', value: 2 });

      const results = dl.query('testbundle', 'items', {}, { order: 'value ASC', limit: 2 });
      assert.equal(results.length, 2);
      assert.equal(results[0].name, 'b');
    });

    it('returns empty array when no matches', () => {
      const results = dl.query('testbundle', 'items', { name: 'nonexistent' });
      assert.deepEqual(results, []);
    });

    it('supports offset in query()', () => {
      dl.registerSchema('paginate', {
        items: {
          columns: {
            id: { type: 'uuid', primary: true },
            position: { type: 'integer' },
            created_at: { type: 'timestamp' },
            updated_at: { type: 'timestamp' },
          },
        },
      });

      for (let i = 1; i <= 5; i++) {
        dl.insert('paginate', 'items', { position: i });
      }

      const page1 = dl.query('paginate', 'items', {}, { order: 'position ASC', limit: 2 });
      const page2 = dl.query('paginate', 'items', {}, { order: 'position ASC', limit: 2, offset: 2 });
      assert.equal(page1[0].position, 1);
      assert.equal(page1[1].position, 2);
      assert.equal(page2[0].position, 3, 'offset should skip first 2 records');
      assert.equal(page2[1].position, 4);
    });
  });

  describe('update', () => {
    it('updates a record and returns it', () => {
      const inserted = dl.insert('testbundle', 'items', { name: 'original', value: 1 });
      const updated = dl.update('testbundle', 'items', inserted.id, { name: 'changed', value: 2 });
      assert.equal(updated.name, 'changed');
      assert.equal(updated.value, 2);
    });

    it('auto-updates updated_at', () => {
      const inserted = dl.insert('testbundle', 'items', { name: 'test' });
      const original_updated_at = inserted.updated_at;
      // Small delay to ensure timestamp differs
      const updated = dl.update('testbundle', 'items', inserted.id, { name: 'changed' });
      assert.ok(updated.updated_at >= original_updated_at);
    });
  });

  describe('delete', () => {
    it('deletes a record', () => {
      const inserted = dl.insert('testbundle', 'items', { name: 'deleteme' });
      dl.delete('testbundle', 'items', inserted.id);
      assert.equal(dl.find('testbundle', 'items', inserted.id), null);
    });
  });

  describe('count', () => {
    it('counts records with filters', () => {
      dl.insert('testbundle', 'items', { name: 'a', value: 1 });
      dl.insert('testbundle', 'items', { name: 'b', value: 2 });
      dl.insert('testbundle', 'items', { name: 'c', value: 1 });

      assert.equal(dl.count('testbundle', 'items'), 3);
      assert.equal(dl.count('testbundle', 'items', { value: 1 }), 2);
    });

    it('counts records with null filter using IS NULL', () => {
      dl.registerSchema('test', {
        items: {
          columns: {
            id: { type: 'uuid', primary: true },
            owner_id: { type: 'uuid' },
            created_at: { type: 'timestamp' },
            updated_at: { type: 'timestamp' },
          },
        },
      });

      dl.insert('test', 'items', { owner_id: 'user-1' });
      dl.insert('test', 'items', { owner_id: null });
      dl.insert('test', 'items', {});

      const nullCount = dl.count('test', 'items', { owner_id: null });
      assert.equal(nullCount, 2, 'should count records where owner_id IS NULL');
    });
  });

  describe('timestamps only when declared', () => {
    it('does not add timestamps for tables without those columns', () => {
      dl.registerSchema('notimestamps', {
        simple: {
          columns: {
            id: { type: 'uuid', primary: true },
            label: { type: 'string' },
          },
        },
      });
      const record = dl.insert('notimestamps', 'simple', { label: 'test' });
      assert.ok(record.id);
      assert.equal(record.label, 'test');
      assert.equal(record.created_at, undefined);
      assert.equal(record.updated_at, undefined);
    });
  });
});

describe('BundleIsolationError properties', () => {
  it('has .code = BUNDLE_ISOLATION', () => {
    const err = new BundleIsolationError('mybundle', 'mytable');
    assert.equal(err.code, 'BUNDLE_ISOLATION');
  });

  it('has .bundle set to the bundle argument', () => {
    const err = new BundleIsolationError('mybundle', 'mytable');
    assert.equal(err.bundle, 'mybundle');
  });

  it('has .table set to the table argument', () => {
    const err = new BundleIsolationError('mybundle', 'mytable');
    assert.equal(err.table, 'mytable');
  });
});

describe('Bundle isolation', () => {
  let dl;

  beforeEach(() => {
    dl = new DataLayer(':memory:');
    dl.registerSchema('bundle_a', {
      data: { columns: { id: { type: 'uuid', primary: true }, val: { type: 'string' } } },
    });
    dl.registerSchema('bundle_b', {
      data: { columns: { id: { type: 'uuid', primary: true }, val: { type: 'string' } } },
    });
  });

  it('allows a bundle to access its own tables', () => {
    const record = dl.insert('bundle_a', 'data', { val: 'hello' });
    assert.equal(record.val, 'hello');
  });

  it('throws BundleIsolationError when accessing another bundle\'s table', () => {
    assert.throws(
      () => dl.query('bundle_a', 'data_from_b'),
      (err) => err instanceof BundleIsolationError
    );
  });

  it('prevents bundle_b from accessing bundle_a table names', () => {
    dl.insert('bundle_a', 'data', { val: 'secret' });
    // bundle_b has its own 'data' table — this is fine (same name, different namespace)
    dl.insert('bundle_b', 'data', { val: 'public' });
    // But bundle_b cannot access a table it didn't declare
    assert.throws(
      () => dl.find('bundle_b', 'nonexistent', 'id'),
      (err) => err instanceof BundleIsolationError
    );
    // Verify data is actually isolated — bundle_b's 'data' doesn't see bundle_a's rows
    const bRows = dl.query('bundle_b', 'data', {});
    assert.equal(bRows.length, 1);
    assert.equal(bRows[0].val, 'public');
  });
});

describe('BundleScopedData', () => {
  it('scopes all operations to the bundle', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('scoped', {
      items: { columns: { id: { type: 'uuid', primary: true }, name: { type: 'string' } } },
    });

    const scoped = new BundleScopedData(dl, 'scoped');
    const record = scoped.insert('items', { name: 'test' });
    assert.equal(record.name, 'test');

    const found = scoped.find('items', record.id);
    assert.equal(found.name, 'test');

    assert.equal(scoped.count('items'), 1);
  });
});

describe('insert with validate option', () => {
  let dl;

  beforeEach(() => {
    dl = new DataLayer(':memory:');
    dl.registerSchema('validatebundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          name: { type: 'string', null: false },
          email: { type: 'string' },
          count: { type: 'integer' },
          active: { type: 'boolean', default: false },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
    });
  });

  it('passes validation when required fields are present', () => {
    const record = dl.insert('validatebundle', 'items', { name: 'test item' }, { validate: true });
    assert.ok(record.id);
    assert.equal(record.name, 'test item');
  });

  it('throws when a required field (null: false) is missing', () => {
    assert.throws(
      () => dl.insert('validatebundle', 'items', {}, { validate: true }),
      (err) => {
        assert.ok(err instanceof ValidationError, 'should be a ValidationError');
        const nameError = err.errors.find(e => e.field === 'name' && e.rule === 'required');
        assert.ok(nameError, 'should have a required error for name field');
        return true;
      }
    );
  });

  it('throws when a field has the wrong type', () => {
    assert.throws(
      () => dl.insert('validatebundle', 'items', { name: 'test', count: 'not-a-number' }, { validate: true }),
      (err) => {
        assert.ok(err instanceof ValidationError, 'should be a ValidationError');
        const countError = err.errors.find(e => e.field === 'count' && e.rule === 'type');
        assert.ok(countError, 'should have a type error for count field');
        return true;
      }
    );
  });

  it('does not validate when option is not set', () => {
    // Legacy behavior: no ValidationError should be thrown even with invalid data.
    // Note: SQLite itself may throw a NOT NULL constraint error here.
    // That's acceptable — we only prohibit ValidationError from this path.
    let thrownError = null;
    try {
      dl.insert('validatebundle', 'items', {});
    } catch (err) {
      thrownError = err;
    }
    if (thrownError !== null) {
      assert.ok(!(thrownError instanceof ValidationError), 'should not throw ValidationError (only DB-level errors allowed)');
    }
  });

  it('skips auto-generated columns (id, created_at, updated_at)', () => {
    const record = dl.insert('validatebundle', 'items', { name: 'auto cols test' }, { validate: true });
    assert.ok(record.id, 'id should be auto-generated');
    assert.ok(record.created_at, 'created_at should be auto-generated');
    assert.ok(record.updated_at, 'updated_at should be auto-generated');
  });
});

describe('query with operators', () => {
  let dl;

  beforeEach(() => {
    dl = new DataLayer(':memory:');
    dl.registerSchema('testbundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          name: { type: 'string' },
          value: { type: 'integer' },
          active: { type: 'boolean' },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
    });
    // Insert 4 test rows: alpha/10, beta/20, gamma/30, delta/20
    dl.insert('testbundle', 'items', { name: 'alpha', value: 10 });
    dl.insert('testbundle', 'items', { name: 'beta',  value: 20 });
    dl.insert('testbundle', 'items', { name: 'gamma', value: 30 });
    dl.insert('testbundle', 'items', { name: 'delta', value: 20 });
  });

  it('$eq works like plain equality', () => {
    const results = dl.query('testbundle', 'items', { value: { $eq: 20 } });
    assert.equal(results.length, 2);
    assert.ok(results.every(r => r.value === 20));
  });

  it('$ne excludes matching records', () => {
    const results = dl.query('testbundle', 'items', { value: { $ne: 20 } });
    assert.equal(results.length, 2);
    assert.ok(results.every(r => r.value !== 20));
  });

  it('$gt filters greater than', () => {
    const results = dl.query('testbundle', 'items', { value: { $gt: 20 } });
    assert.equal(results.length, 1);
    assert.equal(results[0].name, 'gamma');
  });

  it('$gte filters greater than or equal', () => {
    const results = dl.query('testbundle', 'items', { value: { $gte: 20 } });
    assert.equal(results.length, 3);
    assert.ok(results.every(r => r.value >= 20));
  });

  it('$lt filters less than', () => {
    const results = dl.query('testbundle', 'items', { value: { $lt: 20 } });
    assert.equal(results.length, 1);
    assert.equal(results[0].name, 'alpha');
  });

  it('$lte filters less than or equal', () => {
    const results = dl.query('testbundle', 'items', { value: { $lte: 20 } });
    assert.equal(results.length, 3);
    assert.ok(results.every(r => r.value <= 20));
  });

  it('$in filters to set of values', () => {
    const results = dl.query('testbundle', 'items', { value: { $in: [10, 30] } });
    assert.equal(results.length, 2);
    const names = results.map(r => r.name).sort();
    assert.deepEqual(names, ['alpha', 'gamma']);
  });

  it('$like filters with SQL LIKE pattern', () => {
    // '%mm%' matches only 'gamma' (contains double-m)
    const results = dl.query('testbundle', 'items', { name: { $like: '%mm%' } });
    assert.equal(results.length, 1);
    assert.equal(results[0].name, 'gamma');
  });

  it('$isNull filters for NULL values', () => {
    // Insert a row with null value
    dl.insert('testbundle', 'items', { name: 'nullvalue', value: null });
    const results = dl.query('testbundle', 'items', { value: { $isNull: true } });
    assert.equal(results.length, 1);
    assert.equal(results[0].name, 'nullvalue');
  });

  it('$notNull filters for non-NULL values', () => {
    dl.insert('testbundle', 'items', { name: 'nullvalue', value: null });
    const results = dl.query('testbundle', 'items', { value: { $notNull: true } });
    assert.equal(results.length, 4); // original 4 rows have non-null values
    assert.ok(results.every(r => r.value !== null));
  });

  it('plain values still work as equality (backward compatible)', () => {
    const results = dl.query('testbundle', 'items', { name: 'beta' });
    assert.equal(results.length, 1);
    assert.equal(results[0].value, 20);
  });

  it('multiple operators combine with AND', () => {
    const results = dl.query('testbundle', 'items', {
      value: { $gte: 20, $lt: 30 },
    });
    assert.equal(results.length, 2);
    assert.ok(results.every(r => r.value >= 20 && r.value < 30));
  });

  it('throws on unknown operator', () => {
    assert.throws(
      () => dl.query('testbundle', 'items', { value: { $unknown: 5 } }),
      (err) => {
        assert.ok(err.message.includes('$unknown'));
        return true;
      }
    );
  });
});

describe('BundleScopedData insert with validate option', () => {
  let dl;
  let scoped;

  beforeEach(() => {
    dl = new DataLayer(':memory:');
    dl.registerSchema('scopedbundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          title: { type: 'string', null: false },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
    });
    scoped = new BundleScopedData(dl, 'scopedbundle');
  });

  it('passes validate option through to DataLayer', () => {
    const record = scoped.insert('items', { title: 'hello' }, { validate: true });
    assert.ok(record.id);
    assert.equal(record.title, 'hello');
  });

  it('throws ValidationError for missing required fields', () => {
    assert.throws(
      () => scoped.insert('items', {}, { validate: true }),
      (err) => err instanceof ValidationError
    );
  });
});
