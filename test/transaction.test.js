import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { DataLayer, BundleScopedData } from '../index.js';

describe('DataLayer.transaction()', () => {
  let dl;

  beforeEach(() => {
    dl = new DataLayer(':memory:');
    dl.registerSchema('orders', {
      orders: {
        columns: {
          id: { type: 'uuid', primary: true },
          total_cents: { type: 'integer' },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
      line_items: {
        columns: {
          id: { type: 'uuid', primary: true },
          order_id: { type: 'uuid' },
          amount_cents: { type: 'integer' },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
    });
  });

  it('commits when fn succeeds', () => {
    dl.transaction('orders', (bundle) => {
      const order = dl.insert(bundle, 'orders', { total_cents: 5000 });
      dl.insert(bundle, 'line_items', { order_id: order.id, amount_cents: 5000 });
    });

    assert.equal(dl.count('orders', 'orders'), 1);
    assert.equal(dl.count('orders', 'line_items'), 1);
  });

  it('rolls back when fn throws', () => {
    assert.throws(() => {
      dl.transaction('orders', (bundle) => {
        dl.insert(bundle, 'orders', { total_cents: 5000 });
        throw new Error('oops');
      });
    }, { message: 'oops' });

    assert.equal(dl.count('orders', 'orders'), 0);
  });
});

describe('BundleScopedData.transaction()', () => {
  let dl, scoped;

  beforeEach(() => {
    dl = new DataLayer(':memory:');
    dl.registerSchema('orders', {
      orders: {
        columns: {
          id: { type: 'uuid', primary: true },
          total_cents: { type: 'integer' },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
      line_items: {
        columns: {
          id: { type: 'uuid', primary: true },
          order_id: { type: 'uuid' },
          amount_cents: { type: 'integer' },
          created_at: { type: 'timestamp' },
          updated_at: { type: 'timestamp' },
        },
      },
    });
    scoped = new BundleScopedData(dl, 'orders');
  });

  it('commits when fn succeeds', () => {
    scoped.transaction(() => {
      const order = scoped.insert('orders', { total_cents: 5000 });
      scoped.insert('line_items', { order_id: order.id, amount_cents: 5000 });
    });

    assert.equal(scoped.count('orders'), 1);
    assert.equal(scoped.count('line_items'), 1);
  });

  it('rolls back when fn throws', () => {
    assert.throws(() => {
      scoped.transaction(() => {
        scoped.insert('orders', { total_cents: 5000 });
        throw new Error('rollback me');
      });
    }, { message: 'rollback me' });

    assert.equal(scoped.count('orders'), 0);
  });
});
