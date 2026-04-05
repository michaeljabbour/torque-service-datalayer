import Database from 'better-sqlite3';
import { v4 as uuid } from 'uuid';
import { mkdirSync } from 'fs';
import { dirname } from 'path';

export class BundleIsolationError extends Error {
  constructor(bundle, table) {
    super(`Bundle '${bundle}' cannot access table '${table}'. A bundle can only read/write its own declared tables.`);
    this.name = 'BundleIsolationError';
    this.code = 'BUNDLE_ISOLATION';
    this.bundle = bundle;
    this.table = table;
  }
}

export class ValidationError extends Error {
  constructor(errors) {
    const fields = errors.map(e => e.field).join(', ');
    super(`Validation failed: ${fields}`);
    this.name = 'ValidationError';
    this.code = 'VALIDATION_FAILED';
    this.errors = errors;
  }
}

const AUTO_COLS = new Set(['id', 'created_at', 'updated_at']);

export class DataLayer {
  constructor(dbPath = 'data/demo.sqlite3', { readPoolSize = 3 } = {}) {
    const isMemory = dbPath === ':memory:' || dbPath === '';
    if (!isMemory) mkdirSync(dirname(dbPath), { recursive: true });
    // B3: Write connection (exclusive for mutations)
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');
    this.db.pragma('busy_timeout = 5000');
    // B3: Read pool — separate connections for concurrent reads in WAL mode
    // In-memory databases cannot have readonly connections, so skip the pool.
    this._readPool = [];
    if (!isMemory) {
      for (let i = 0; i < readPoolSize; i++) {
        const reader = new Database(dbPath, { readonly: true });
        reader.pragma('journal_mode = WAL');
        reader.pragma('busy_timeout = 5000');
        this._readPool.push(reader);
      }
    }
    this._readIdx = 0;
    this.schemas = {};
  }

  /** Get next read connection (round-robin) */
  _reader() {
    if (this._readPool.length === 0) return this.db;
    const r = this._readPool[this._readIdx % this._readPool.length];
    this._readIdx++;
    return r;
  }

  close() {
    for (const r of this._readPool) try { r.close(); } catch {}
    try { this.db.close(); } catch {}
  }

  registerSchema(bundleName, tables) {
    this.schemas[bundleName] = {};
    for (const [tableName, tableDef] of Object.entries(tables)) {
      const fullName = `${bundleName}_${tableName}`;
      const columns = tableDef.columns || {};
      this.schemas[bundleName][tableName] = { fullName, columns };
      this._provisionTable(fullName, columns, tableDef);
    }
  }

  insert(bundle, table, attrs, opts = {}) {
    this._enforceAccess(bundle, table);
    if (opts.validate) {
      this._validateInsert(bundle, table, attrs);
    }
    const full = this._fullName(bundle, table);
    const columns = this._columns(bundle, table);
    const record = { ...attrs };
    if (columns.id && !record.id) record.id = uuid();
    if (columns.created_at && !record.created_at) record.created_at = new Date().toISOString();
    if (columns.updated_at && !record.updated_at) record.updated_at = new Date().toISOString();
    this._coerceBooleans(record, columns);
    const cols = Object.keys(record);
    const placeholders = cols.map(() => '?');
    const vals = Object.values(record);
    const quotedCols = cols.map(c => `"${c}"`).join(', ');
    this.db.prepare(`INSERT INTO "${full}" (${quotedCols}) VALUES (${placeholders.join(', ')})`).run(...vals);
    return this.find(bundle, table, record.id);
  }

  find(bundle, table, id) {
    this._enforceAccess(bundle, table);
    const full = this._fullName(bundle, table);
    const row = this._reader().prepare(`SELECT * FROM "${full}" WHERE id = ?`).get(id) || null;
    return row ? this._restoreBooleans(row, this._columns(bundle, table)) : null;
  }

  count(bundle, table, filters = {}) {
    this._enforceAccess(bundle, table);
    const full = this._fullName(bundle, table);
    const where = Object.keys(filters);
    const clause = where.length > 0 ? ' WHERE ' + where.map(k => `"${k}" = ?`).join(' AND ') : '';
    const row = this._reader().prepare(`SELECT COUNT(*) as cnt FROM "${full}"${clause}`).get(...Object.values(filters));
    return row?.cnt || 0;
  }

  query(bundle, table, filters = {}, { order, limit, offset } = {}) {
    this._enforceAccess(bundle, table);
    const full = this._fullName(bundle, table);
    const columns = this._columns(bundle, table);
    const { clauses, vals } = this._buildWhere(filters, columns);
    let sql = `SELECT * FROM "${full}"`;
    if (clauses.length) sql += ` WHERE ${clauses.join(' AND ')}`;
    if (order) {
      const sanitized = this._sanitizeOrder(order, columns);
      if (sanitized) sql += ` ORDER BY ${sanitized}`;
    }
    if (limit) sql += ` LIMIT ${parseInt(limit)}`;
    if (offset) sql += ` OFFSET ${parseInt(offset)}`;
    const rows = this._reader().prepare(sql).all(...vals);
    return rows.map(row => this._restoreBooleans(row, columns));
  }

  update(bundle, table, id, attrs) {
    this._enforceAccess(bundle, table);
    const full = this._fullName(bundle, table);
    const columns = this._columns(bundle, table);
    const allowedCols = new Set(Object.keys(columns));
    const updates = { ...attrs };
    this._coerceBooleans(updates, columns);
    if (columns.updated_at && updates.updated_at === undefined) {
      updates.updated_at = new Date().toISOString();
    }
    const keys = Object.keys(updates);
    if (keys.length === 0) return this.find(bundle, table, id);
    for (const k of keys) {
      if (!allowedCols.has(k)) {
        throw new Error(`Invalid update column: '${k}' is not a declared column for ${bundle}.${table}`);
      }
    }
    const sets = keys.map(k => `"${k}" = ?`);
    const vals = [...keys.map((key) => updates[key]), id];
    this.db.prepare(`UPDATE "${full}" SET ${sets.join(', ')} WHERE id = ?`).run(...vals);
    return this.find(bundle, table, id);
  }

  delete(bundle, table, id) {
    this._enforceAccess(bundle, table);
    const full = this._fullName(bundle, table);
    this.db.prepare(`DELETE FROM "${full}" WHERE id = ?`).run(id);
    return true;
  }

  count(bundle, table, filters = {}) {
    this._enforceAccess(bundle, table);
    const full = this._fullName(bundle, table);
    const columns = this._columns(bundle, table);
    const allowedCols = new Set(Object.keys(columns));
    const clauses = [];
    const vals = [];
    for (const [k, v] of Object.entries(filters)) {
      if (!allowedCols.has(k)) {
        throw new Error(`Invalid filter column: '${k}' is not a declared column for ${bundle}.${table}`);
      }
      if (v === null || v === undefined) { clauses.push(`"${k}" IS NULL`); }
      else { clauses.push(`"${k}" = ?`); vals.push(v); }
    }
    let sql = `SELECT COUNT(*) as c FROM "${full}"`;
    if (clauses.length) sql += ` WHERE ${clauses.join(' AND ')}`;
    return this.db.prepare(sql).get(...vals).c;
  }

  transaction(bundle, fn) {
    const txn = this.db.transaction(() => {
      fn(bundle);
    });
    txn();
  }

  tablesFor(bundle) {
    return Object.keys(this.schemas[bundle] || {});
  }

  /**
   * Feature 14: Register relations declared in manifest schema.
   * Called during bundle boot after registerSchema.
   *
   * relations format (from manifest):
   *   list: { type: belongs_to, table: lists, key: list_id }
   *   members: { type: has_many, table: card_members, key: card_id }
   *   labels: { type: has_many, through: card_labels, table: labels }
   *   creator: { type: foreign_ref, bundle: iam, interface: getUser, key: created_by, map: { userId: created_by } }
   */
  registerRelations(bundle, table, relations) {
    if (!this._relations) this._relations = {};
    if (!this._relations[bundle]) this._relations[bundle] = {};
    this._relations[bundle][table] = relations || {};
  }

  /**
   * Feature 14: Find a record with its declared relations resolved.
   * @param {string} bundle - Bundle name
   * @param {string} table - Table name
   * @param {string} id - Record ID
   * @param {string[]} include - Relation names to include (default: all)
   * @param {object} coordinator - ScopedCoordinator for cross-bundle refs
   * @returns {object} Record with relations populated
   */
  async findWithRelations(bundle, table, id, include, coordinator) {
    const record = this.find(bundle, table, id);
    if (!record) return null;

    const relations = this._relations?.[bundle]?.[table] || {};
    const toInclude = include || Object.keys(relations);

    for (const relName of toInclude) {
      const rel = relations[relName];
      if (!rel) continue;

      if (rel.type === 'belongs_to') {
        const fk = record[rel.key];
        record[relName] = fk ? this.find(bundle, rel.table, fk) : null;
      } else if (rel.type === 'has_many' && !rel.through) {
        record[relName] = this.query(bundle, rel.table, { [rel.key]: id });
      } else if (rel.type === 'has_many' && rel.through) {
        // Many-to-many via join table
        const joins = this.query(bundle, rel.through, { [rel.key || `${table.replace(/s$/, '')}_id`]: id });
        const foreignKey = Object.keys(joins[0] || {}).find(k => k.endsWith('_id') && k !== (rel.key || `${table.replace(/s$/, '')}_id`)) || 'id';
        record[relName] = joins.map(j => this.find(bundle, rel.table, j[foreignKey])).filter(Boolean);
      } else if (rel.type === 'foreign_ref' && coordinator) {
        // Cross-bundle reference via coordinator
        try {
          const input = {};
          for (const [param, field] of Object.entries(rel.map || {})) {
            input[param] = record[field];
          }
          record[relName] = await coordinator.call(rel.bundle, rel.interface, input);
        } catch {
          record[relName] = null;
        }
      }
    }

    return record;
  }

  /**
   * Feature 14: Query with relations (batch).
   */
  async queryWithRelations(bundle, table, filters, opts, include, coordinator) {
    const records = this.query(bundle, table, filters, opts);
    if (!include?.length) return records;

    return Promise.all(
      records.map(async (record) => {
        // Re-use findWithRelations logic but with existing record
        const enriched = { ...record };
        const relations = this._relations?.[bundle]?.[table] || {};
        for (const relName of include) {
          const rel = relations[relName];
          if (!rel) continue;
          if (rel.type === 'belongs_to') {
            enriched[relName] = record[rel.key] ? this.find(bundle, rel.table, record[rel.key]) : null;
          } else if (rel.type === 'has_many' && !rel.through) {
            enriched[relName] = this.query(bundle, rel.table, { [rel.key]: record.id });
          } else if (rel.type === 'has_many' && rel.through) {
            const joins = this.query(bundle, rel.through, { [rel.key || `${table.replace(/s$/, '')}_id`]: record.id });
            const fk = Object.keys(joins[0] || {}).find(k => k.endsWith('_id') && k !== (rel.key || `${table.replace(/s$/, '')}_id`));
            enriched[relName] = joins.map(j => this.find(bundle, rel.table, j[fk])).filter(Boolean);
          } else if (rel.type === 'foreign_ref' && coordinator) {
            try {
              const input = {};
              for (const [param, field] of Object.entries(rel.map || {})) input[param] = record[field];
              enriched[relName] = await coordinator.call(rel.bundle, rel.interface, input);
            } catch { enriched[relName] = null; }
          }
        }
        return enriched;
      })
    );
  }

  _coerceBooleans(record, columns) {
    for (const [col, spec] of Object.entries(columns)) {
      if (col in record && (spec.type === 'boolean' || spec === 'boolean')) {
        record[col] = record[col] ? 1 : 0;
      }
    }
  }

  _restoreBooleans(row, columns) {
    for (const [col, spec] of Object.entries(columns)) {
      if (col in row && (spec.type === 'boolean' || spec === 'boolean')) {
        row[col] = row[col] === 1;
      }
    }
    return row;
  }

  _enforceAccess(bundle, table) {
    if (!this.schemas[bundle]?.[table]) {
      throw new BundleIsolationError(bundle, table);
    }
  }

  _fullName(bundle, table) {
    return this.schemas[bundle]?.[table]?.fullName;
  }

  _columns(bundle, table) {
    return this.schemas[bundle]?.[table]?.columns || {};
  }

  _provisionTable(fullName, columns, tableSpec = {}) {
    const colDefs = Object.entries(columns).map(([name, spec]) => {
      if (typeof spec === 'string') spec = { type: spec };
      const sqlType = this._sqlType(spec.type);
      let def = `"${name}" ${sqlType}`;
      if (spec.primary) def += ' PRIMARY KEY';
      if (spec.null === false || spec.required === true) def += ' NOT NULL';
      if (spec.unique) def += ' UNIQUE';
      if (spec.default !== undefined) {
        if (spec.type === 'boolean') {
          def += ` DEFAULT ${spec.default ? 1 : 0}`;
        } else {
          def += ` DEFAULT '${spec.default.toString().replace(/'/g, "''")}'`;
        }
      }
      return def;
    });

    // Check if table already exists
    const tableExists = this.db.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' AND name=?`
    ).get(fullName);

    if (!tableExists) {
      this.db.exec(`CREATE TABLE IF NOT EXISTS "${fullName}" (${colDefs.join(', ')})`);
      // Provision indexes for new table
      if (tableSpec.indexes) {
        for (const idx of tableSpec.indexes) {
          const cols = idx.columns.map(c => `"${c}"`).join(', ');
          const unique = idx.unique ? 'UNIQUE ' : '';
          const idxName = `idx_${fullName}_${idx.columns.join('_')}`;
          this.db.exec(`CREATE ${unique}INDEX IF NOT EXISTS "${idxName}" ON "${fullName}" (${cols})`);
        }
      }
      return;
    }

    // Table exists — diff columns and ADD any new ones
    const existing = new Set(
      this.db.prepare(`PRAGMA table_info("${fullName}")`).all().map(c => c.name)
    );

    for (const [name, spec] of Object.entries(columns)) {
      if (existing.has(name)) continue;

      const normalizedSpec = typeof spec === 'string' ? { type: spec } : spec;
      const sqlType = this._sqlType(normalizedSpec.type);
      let colDef = `"${name}" ${sqlType}`;
      if (normalizedSpec.default !== undefined) {
        colDef += ` DEFAULT '${normalizedSpec.default.toString().replace(/'/g, "''")}'`;
      }

      try {
        this.db.exec(`ALTER TABLE "${fullName}" ADD COLUMN ${colDef}`);
        console.log(`[datalayer] Added column ${fullName}.${name} (${sqlType})`);
      } catch (e) {
        console.warn(`[datalayer] Failed to add column ${fullName}.${name}: ${e.message}`);
      }
    }

    // Warn about columns in DB that aren't in manifest (informational only)
    const declared = new Set(Object.keys(columns));
    for (const col of existing) {
      if (!declared.has(col)) {
        console.warn(`[datalayer] Column ${fullName}.${col} exists in DB but not in manifest (orphaned)`);
      }
    }

    // Provision indexes declared in the table spec
    if (tableSpec.indexes) {
      for (const idx of tableSpec.indexes) {
        const cols = idx.columns.map(c => `"${c}"`).join(', ');
        const unique = idx.unique ? 'UNIQUE ' : '';
        const idxName = `idx_${fullName}_${idx.columns.join('_')}`;
        this.db.exec(`CREATE ${unique}INDEX IF NOT EXISTS "${idxName}" ON "${fullName}" (${cols})`);
      }
    }
  }

  /**
   * Build WHERE clauses from a filters object supporting operator objects.
   * @param {object} filters - Filters map: { colName: value | operatorObject }
   * @param {object} columns - Declared columns from the manifest schema
   * @returns {{ clauses: string[], vals: any[] }}
   */
  _buildWhere(filters, columns) {
    const allowedCols = new Set(Object.keys(columns));
    const clauses = [];
    const vals = [];

    const OP_MAP = {
      $eq: '=', $ne: '!=', $gt: '>', $gte: '>=', $lt: '<', $lte: '<=', $like: 'LIKE',
    };

    for (const [k, v] of Object.entries(filters)) {
      if (!allowedCols.has(k)) {
        throw new Error(`Invalid filter column: '${k}' is not a declared column`);
      }
      const colSpec = columns[k];
      const isBool = colSpec && (colSpec.type === 'boolean' || colSpec === 'boolean');

      const coerce = (val) => isBool && typeof val === 'boolean' ? (val ? 1 : 0) : val;

      // Operator object: { $gt: 5, $lt: 10, ... }
      if (v !== null && v !== undefined && typeof v === 'object' && !Array.isArray(v)) {
        for (const [op, opVal] of Object.entries(v)) {
          if (op in OP_MAP) {
            clauses.push(`"${k}" ${OP_MAP[op]} ?`);
            vals.push(coerce(opVal));
          } else if (op === '$in') {
            if (!Array.isArray(opVal) || opVal.length === 0) {
              throw new Error(`$in requires a non-empty array for column '${k}'`);
            }
            const placeholders = opVal.map(() => '?').join(', ');
            clauses.push(`"${k}" IN (${placeholders})`);
            for (const item of opVal) vals.push(coerce(item));
          } else if (op === '$isNull') {
            clauses.push(`"${k}" IS NULL`);
          } else if (op === '$notNull') {
            clauses.push(`"${k}" IS NOT NULL`);
          } else {
            throw new Error(`Unknown operator '${op}' on column '${k}'`);
          }
        }
      } else if (v === null || v === undefined) {
        // Plain null/undefined -> IS NULL
        clauses.push(`"${k}" IS NULL`);
      } else {
        // Plain value -> equality
        clauses.push(`"${k}" = ?`);
        vals.push(coerce(v));
      }
    }

    return { clauses, vals };
  }

  /**
   * Sanitize ORDER BY clause to prevent SQL injection.
   * Only allows column names declared in the manifest, with optional ASC/DESC.
   * @param {string} order - Raw order string (e.g., 'created_at DESC')
   * @param {object} columns - Declared columns from the manifest schema
   * @returns {string|null} Sanitized order clause or null if invalid
   */
  _sanitizeOrder(order, columns) {
    const allowedCols = new Set(Object.keys(columns));
    const parts = order.split(',').map(p => p.trim());
    const sanitized = [];
    for (const part of parts) {
      const tokens = part.split(/\s+/);
      const colName = tokens[0];
      const direction = (tokens[1] || '').toUpperCase();
      if (!allowedCols.has(colName)) {
        console.warn(`[datalayer] ORDER BY rejected: column '${colName}' not in manifest schema`);
        return null;
      }
      if (direction && direction !== 'ASC' && direction !== 'DESC') {
        console.warn(`[datalayer] ORDER BY rejected: invalid direction '${direction}'`);
        return null;
      }
      sanitized.push(direction ? `"${colName}" ${direction}` : `"${colName}"`);
    }
    return sanitized.join(', ');
  }

  _sqlType(type) {
    return { uuid: 'TEXT', string: 'TEXT', integer: 'INTEGER', boolean: 'INTEGER', float: 'REAL', decimal: 'REAL', timestamp: 'TEXT', datetime: 'TEXT', text: 'TEXT' }[type] || 'TEXT';
  }

  _validateInsert(bundle, table, attrs) {
    const columns = this._columns(bundle, table);
    const errors = [];

    for (const [col, spec] of Object.entries(columns)) {
      if (AUTO_COLS.has(col)) continue;
      const colSpec = typeof spec === 'string' ? { type: spec } : spec;
      const value = attrs[col];

      // Check required (null: false or required: true)
      if (colSpec.null === false || colSpec.required === true) {
        if (value === undefined || value === null || value === '') {
          errors.push({ field: col, rule: 'required' });
          continue;
        }
      }

      // Type check (only if value is provided and not null/undefined)
      if (value !== undefined && value !== null) {
        const type = colSpec.type;
        let typeOk = true;
        if (type === 'string' || type === 'text') {
          typeOk = typeof value === 'string';
        } else if (type === 'integer') {
          typeOk = typeof value === 'number' && Number.isInteger(value);
        } else if (type === 'float' || type === 'decimal') {
          typeOk = typeof value === 'number';
        } else if (type === 'boolean') {
          typeOk = typeof value === 'boolean';
        } else if (type === 'uuid') {
          typeOk = typeof value === 'string';
        }
        if (!typeOk) {
          errors.push({ field: col, rule: 'type' });
        }
      }
    }

    if (errors.length > 0) {
      throw new ValidationError(errors);
    }
  }
}

export class BundleScopedData {
  constructor(dataLayer, bundleName, coordinator) {
    this.data = dataLayer;
    this.bundle = bundleName;
    this.coordinator = coordinator;
  }
  insert(table, attrs, opts = {}) { return this.data.insert(this.bundle, table, attrs, opts); }
  find(table, id) { return this.data.find(this.bundle, table, id); }
  query(table, filters = {}, opts = {}) { return this.data.query(this.bundle, table, filters, opts); }
  update(table, id, attrs) { return this.data.update(this.bundle, table, id, attrs); }
  delete(table, id) { return this.data.delete(this.bundle, table, id); }
  count(table, filters = {}) { return this.data.count(this.bundle, table, filters); }
  transaction(fn) { this.data.transaction(this.bundle, () => fn()); }
  // Feature 14: Relations
  findWithRelations(table, id, include) { return this.data.findWithRelations(this.bundle, table, id, include, this.coordinator); }
  queryWithRelations(table, filters, opts, include) { return this.data.queryWithRelations(this.bundle, table, filters, opts, include, this.coordinator); }
}
