# @torquedev/datalayer

Bundle-scoped SQLite storage for the Torque framework. Each bundle registers its own tables; the data layer enforces strict cross-bundle access isolation.

## Install

```bash
npm install @torquedev/datalayer
```

Or via git dependency:

```bash
npm install git+https://github.com/torque-framework/torque-service-datalayer.git
```

Peer dependencies: `better-sqlite3`, `uuid`

## Usage

```js
import { DataLayer } from '@torquedev/datalayer';

const db = new DataLayer('/path/to/data.db');

// Each bundle declares the tables it owns
db.registerSchema('tasks', [
  { name: 'tasks', columns: [
    { name: 'id', type: 'uuid' },
    { name: 'title', type: 'string' },
    { name: 'done', type: 'boolean' },
    { name: 'created_at', type: 'timestamp' },
  ]},
]);

// Insert, find, query, update, delete — always scoped to declared tables
db.insert('tasks', 'tasks', { id: '...', title: 'Ship it', done: false });
const row = db.find('tasks', 'tasks', { id: '...' });
```

## API

### `DataLayer`

| Method | Description |
|---|---|
| `constructor(dbPath)` | Open (or create) a SQLite database at the given path. |
| `registerSchema(bundleName, tables)` | Declare the tables a bundle owns. Tables are auto-provisioned on boot; new columns are added via `ALTER TABLE` on subsequent boots. Columns are never dropped. |
| `insert(bundleName, table, row)` | Insert a row. |
| `find(bundleName, table, where)` | Return a single matching row. |
| `query(bundleName, table, { filters, order, limit, offset })` | Query with optional filtering, ordering, and pagination. |
| `update(bundleName, table, where, values)` | Update matching rows. |
| `delete(bundleName, table, where)` | Delete matching rows. |
| `count(bundleName, table, where)` | Return the count of matching rows. |
| `transaction(fn)` | Execute `fn` inside a SQLite transaction. |
| `tablesFor(bundleName)` | List the tables registered by a bundle. |

### `BundleScopedData`

Convenience wrapper that pre-binds a bundle name so callers don't have to pass it on every call:

```js
const scoped = new BundleScopedData(db, 'tasks');
scoped.insert('tasks', { title: 'Demo' });
```

### `BundleIsolationError`

Thrown when a bundle attempts to access a table it did not declare in `registerSchema`. This is the core safety mechanism — bundles cannot read or write each other's data.

## Type Mapping

| Schema type | SQLite type |
|---|---|
| `uuid`, `string`, `text` | `TEXT` |
| `integer` | `INTEGER` |
| `boolean` | `INTEGER` (0/1) |
| `float`, `decimal` | `REAL` |
| `timestamp`, `datetime` | `TEXT` |

## Schema Enforcement

The data layer enforces schema integrity at multiple levels to prevent both data corruption and SQL injection.

### Foreign Keys

SQLite foreign key enforcement is disabled by default. The data layer enables it at connection time:

```sql
PRAGMA foreign_keys = ON;
```

This ensures referential integrity across tables (e.g., `refresh_tokens.user_id` → `users.id`) is enforced by the database engine, not just application logic.

### Column Validation

Before any `INSERT` or `UPDATE`, the data layer filters the supplied key/value pairs against the columns declared in the bundle's registered schema:

- **Insert keys** -- Only columns present in the schema are passed to the SQL statement. Unknown keys are silently dropped.
- **Update keys** -- Same filtering applies to `UPDATE SET` values.
- **Identifier quoting** -- All column and table identifiers are double-quoted (`"column_name"`) to guard against reserved-word collisions and injection via crafted column names.

### DDL Safety

When provisioning or migrating tables, the data layer escapes identifiers in all generated SQL:

- **Default escaping** -- `CREATE TABLE` statements quote table and column names.
- **`ALTER TABLE` escaping** -- When adding new columns on subsequent boots, the column name is quoted in the `ALTER TABLE ... ADD COLUMN` statement.

This ensures safe operation even when bundle authors use column names that are SQLite reserved words.

### Index Provisioning

Indexes declared in the bundle schema YAML are automatically created (if they do not already exist) at boot time:

```yaml
schema:
  tables:
    refresh_tokens:
      id:        { type: text, primary: true }
      user_id:   { type: text, required: true }
      jti:       { type: text, required: true }
      expires_at: { type: text }
      indexes:
        - columns: [user_id]
        - columns: [jti], unique: true
```

Generated SQL:

```sql
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON "refresh_tokens" ("user_id");
CREATE UNIQUE INDEX IF NOT EXISTS idx_refresh_tokens_jti ON "refresh_tokens" ("jti");
```

### Required → NOT NULL

Columns declared with `required: true` in the schema YAML are provisioned with a `NOT NULL` constraint:

```yaml
columns:
  - name: email
    type: string
    required: true
```

Generated DDL:

```sql
"email" TEXT NOT NULL
```

Rows that violate the constraint will be rejected by SQLite at the database level, not merely at the application layer.

## Connection Pool (Feature B3)

The DataLayer opens multiple SQLite connections for concurrent access in WAL mode:

- **1 write connection** -- exclusive for mutations (INSERT, UPDATE, DELETE)
- **3 read connections** -- round-robin for queries (SELECT) via `_reader()`

```js
const db = new DataLayer('data/app.sqlite3', { readPoolSize: 3 });
```

WAL mode allows readers and writers to operate concurrently without locking. `busy_timeout` is set to 5000ms to handle brief contention.

Call `db.close()` to clean up all connections.

## Data Relations (Feature 14)

Declare relationships in your manifest schema and resolve them with a single call:

```yaml
schema:
  tables:
    cards:
      columns:
        id: { type: uuid, primary: true }
        list_id: { type: uuid, null: false }
        name: { type: string, null: false }
        created_by: { type: uuid }
      relations:
        list: { type: belongs_to, table: lists, key: list_id }
        members: { type: has_many, table: card_members, key: card_id }
        labels: { type: has_many, through: card_labels, table: labels }
        creator: { type: foreign_ref, bundle: iam, interface: getUser, key: created_by, map: { userId: created_by } }
```

### Relation Types

| Type | Description | Example |
|------|-------------|---------|
| `belongs_to` | FK to another table in same bundle | `list: { type: belongs_to, table: lists, key: list_id }` |
| `has_many` | Reverse FK from another table | `members: { type: has_many, table: card_members, key: card_id }` |
| `has_many` + `through` | Many-to-many via join table | `labels: { type: has_many, through: card_labels, table: labels }` |
| `foreign_ref` | Cross-bundle reference via coordinator | `creator: { type: foreign_ref, bundle: iam, interface: getUser, ... }` |

### Usage in logic.js

```js
// Single record with all relations
const card = await this.data.findWithRelations('cards', cardId, ['members', 'labels', 'creator']);
// card.members = [{ id, user_id, ... }]
// card.labels = [{ id, name, color }]
// card.creator = { id, name, email }  (resolved from iam bundle)

// Batch query with relations
const cards = await this.data.queryWithRelations('cards', { board_id: boardId }, {}, ['members', 'labels']);
```

`foreign_ref` relations are resolved via `coordinator.call()`, preserving bundle isolation. The bundle never directly accesses another bundle's data.

## Security

`ORDER BY` clauses are whitelist-sanitized against the columns declared in the bundle's manifest to prevent SQL injection.

## Details

- ESM-only
- Tests: `node --test`
- Zero runtime dependencies beyond `js-yaml` inherited via core

## Torque Framework

Part of the [Torque](https://github.com/torque-framework/torque) composable monolith framework.

## License

MIT — see [LICENSE](./LICENSE)
