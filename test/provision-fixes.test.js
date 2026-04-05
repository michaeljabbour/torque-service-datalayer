/**
 * Tests for _provisionTable() Phase 3 fixes:
 *   Task 4 — Index provisioning from manifest tableSpec.indexes
 *   Task 5 — required: true treated as NOT NULL
 */
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { DataLayer } from '../index.js';

// ── Task 5: required: true -> NOT NULL ────────────────────────────────────────

describe('DataLayer _provisionTable() – required: true maps to NOT NULL', () => {
  it('enforces NOT NULL via required: true when inserting null', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('bundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          title: { type: 'string', required: true },
        },
      },
    });

    // Inserting with a null title should fail at the SQLite level
    assert.throws(
      () => dl.insert('bundle', 'items', { title: null }),
      (err) => {
        // SQLite NOT NULL constraint violation
        assert.ok(
          err.message.toLowerCase().includes('not null') || err.message.toLowerCase().includes('null'),
          `Expected NOT NULL error, got: ${err.message}`
        );
        return true;
      }
    );
  });

  it('allows insert when required field is provided', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('bundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          title: { type: 'string', required: true },
        },
      },
    });

    const record = dl.insert('bundle', 'items', { title: 'Valid Title' });
    assert.equal(record.title, 'Valid Title');
  });

  it('applies NOT NULL to existing required: true columns on CREATE TABLE', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('bundle', {
      notes: {
        columns: {
          id: { type: 'uuid', primary: true },
          body: { type: 'text', required: true },
          created_at: { type: 'timestamp' },
        },
      },
    });

    // Verify the schema in SQLite has NOT NULL for 'body'
    const info = dl.db.prepare('PRAGMA table_info("bundle_notes")').all();
    const bodyCol = info.find(c => c.name === 'body');
    assert.ok(bodyCol, 'body column should exist');
    assert.equal(bodyCol.notnull, 1, 'body should be NOT NULL (notnull=1)');
  });

  it('null: false still works unchanged', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('bundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          code: { type: 'string', null: false },
        },
      },
    });

    const info = dl.db.prepare('PRAGMA table_info("bundle_items")').all();
    const codeCol = info.find(c => c.name === 'code');
    assert.ok(codeCol, 'code column should exist');
    assert.equal(codeCol.notnull, 1, 'code should be NOT NULL (notnull=1)');
  });

  it('both required: true and null: false produce NOT NULL', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('bundle', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          a: { type: 'string', required: true },
          b: { type: 'string', null: false },
          c: { type: 'string' }, // nullable
        },
      },
    });

    const info = dl.db.prepare('PRAGMA table_info("bundle_items")').all();
    const a = info.find(c => c.name === 'a');
    const b = info.find(c => c.name === 'b');
    const c = info.find(c => c.name === 'c');
    assert.equal(a.notnull, 1, 'a should be NOT NULL');
    assert.equal(b.notnull, 1, 'b should be NOT NULL');
    assert.equal(c.notnull, 0, 'c should be nullable');
  });
});

// ── Task 4: Index provisioning ────────────────────────────────────────────────

describe('DataLayer _provisionTable() – index provisioning from manifest', () => {
  it('creates a non-unique index from manifest indexes', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('tasks', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          entity_type: { type: 'string' },
          entity_id: { type: 'uuid' },
          created_at: { type: 'timestamp' },
        },
        indexes: [
          { columns: ['entity_type', 'entity_id'] },
        ],
      },
    });

    const indexes = dl.db.prepare(
      "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='tasks_items'"
    ).all();

    const idxName = 'idx_tasks_items_entity_type_entity_id';
    const found = indexes.find(i => i.name === idxName);
    assert.ok(found, `Expected index '${idxName}' to exist, got: ${indexes.map(i => i.name).join(', ')}`);
    assert.ok(!found.sql.toUpperCase().includes('UNIQUE'), 'should be non-unique');
  });

  it('creates a UNIQUE index when idx.unique is true', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('boards', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          board_id: { type: 'uuid' },
          slug: { type: 'string' },
        },
        indexes: [
          { columns: ['board_id'], unique: true },
        ],
      },
    });

    const indexes = dl.db.prepare(
      "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='boards_items'"
    ).all();

    const idxName = 'idx_boards_items_board_id';
    const found = indexes.find(i => i.name === idxName);
    assert.ok(found, `Expected unique index '${idxName}' to exist`);
    assert.ok(found.sql.toUpperCase().includes('UNIQUE'), 'index should be UNIQUE');
  });

  it('creates multiple indexes when several are declared', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('myapp', {
      records: {
        columns: {
          id: { type: 'uuid', primary: true },
          user_id: { type: 'uuid' },
          status: { type: 'string' },
          created_at: { type: 'timestamp' },
        },
        indexes: [
          { columns: ['user_id'] },
          { columns: ['status'] },
          { columns: ['user_id', 'status'] },
        ],
      },
    });

    const indexes = dl.db.prepare(
      "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='myapp_records'"
    ).all().map(r => r.name);

    assert.ok(indexes.includes('idx_myapp_records_user_id'), 'should have user_id index');
    assert.ok(indexes.includes('idx_myapp_records_status'), 'should have status index');
    assert.ok(indexes.includes('idx_myapp_records_user_id_status'), 'should have composite index');
  });

  it('is idempotent – CREATE INDEX IF NOT EXISTS does not throw on second call', () => {
    const dl = new DataLayer(':memory:');
    const schema = {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          tag: { type: 'string' },
        },
        indexes: [{ columns: ['tag'] }],
      },
    };

    // Register once, then again — should not throw
    dl.registerSchema('myapp', schema);
    assert.doesNotThrow(() => dl.registerSchema('myapp', schema));
  });

  it('skips index provisioning when no indexes declared', () => {
    const dl = new DataLayer(':memory:');
    dl.registerSchema('simple', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          name: { type: 'string' },
        },
        // no indexes
      },
    });

    const indexes = dl.db.prepare(
      "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='simple_items' AND name NOT LIKE 'sqlite_%'"
    ).all();

    // No user-defined indexes
    assert.equal(indexes.length, 0, 'should have no user-defined indexes');
  });

  it('provisions indexes for existing tables too (migration path)', () => {
    const dl = new DataLayer(':memory:');

    // First call: create table without indexes
    dl.registerSchema('myapp', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          owner_id: { type: 'uuid' },
        },
      },
    });

    // Manually clear schema registry so we can re-register (simulates migration)
    delete dl.schemas['myapp'];

    // Second call: same table but now with an index
    dl.registerSchema('myapp', {
      items: {
        columns: {
          id: { type: 'uuid', primary: true },
          owner_id: { type: 'uuid' },
        },
        indexes: [{ columns: ['owner_id'] }],
      },
    });

    const indexes = dl.db.prepare(
      "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='myapp_items'"
    ).all().map(r => r.name);

    assert.ok(indexes.includes('idx_myapp_items_owner_id'), 'should have owner_id index after migration');
  });
});
