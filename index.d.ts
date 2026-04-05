/**
 * @torquedev/datalayer - TypeScript declarations
 */

export declare class BundleIsolationError extends Error {
  name: 'BundleIsolationError';
  code: 'BUNDLE_ISOLATION';
  bundle: string;
  table: string;
  constructor(bundle: string, table: string);
}

export declare class ValidationError extends Error {
  name: 'ValidationError';
  code: 'VALIDATION_FAILED';
  errors: Array<{ field: string; rule: string }>;
  constructor(errors: Array<{ field: string; rule: string }>);
}

export interface ColumnSpec {
  type?: 'uuid' | 'string' | 'integer' | 'boolean' | 'float' | 'decimal' | 'timestamp' | 'datetime' | 'text';
  primary?: boolean;
  null?: boolean;
  required?: boolean;
  unique?: boolean;
  default?: string | number | boolean;
}

export interface TableSchema {
  columns: Record<string, ColumnSpec | string>;
  indexes?: Array<{
    columns: string[];
    unique?: boolean;
  }>;
}

export interface InsertOptions {
  validate?: boolean;
}

export interface QueryOptions {
  order?: string;
  limit?: number;
  offset?: number;
}

export interface RelationSpec {
  type: 'belongs_to' | 'has_many' | 'foreign_ref';
  table: string;
  key?: string;
  through?: string;
  bundle?: string;
  interface?: string;
  map?: Record<string, string>;
}

export declare class DataLayer {
  db: import('better-sqlite3').Database;
  schemas: Record<string, Record<string, { fullName: string; columns: Record<string, ColumnSpec | string> }>>;

  constructor(dbPath?: string, opts?: { readPoolSize?: number });

  close(): void;

  registerSchema(
    bundleName: string,
    tables: Record<string, TableSchema>
  ): void;

  insert(
    bundle: string,
    table: string,
    attrs: Record<string, unknown>,
    opts?: InsertOptions
  ): Record<string, unknown>;

  find(
    bundle: string,
    table: string,
    id: string
  ): Record<string, unknown> | null;

  count(
    bundle: string,
    table: string,
    filters?: Record<string, unknown>
  ): number;

  query(
    bundle: string,
    table: string,
    filters?: Record<string, unknown>,
    opts?: QueryOptions
  ): Array<Record<string, unknown>>;

  update(
    bundle: string,
    table: string,
    id: string,
    attrs: Record<string, unknown>
  ): Record<string, unknown> | null;

  delete(
    bundle: string,
    table: string,
    id: string
  ): boolean;

  transaction(
    bundle: string,
    fn: (bundle: string) => void
  ): void;

  tablesFor(bundle: string): string[];

  registerRelations(
    bundle: string,
    table: string,
    relations: Record<string, RelationSpec>
  ): void;

  findWithRelations(
    bundle: string,
    table: string,
    id: string,
    include?: string[],
    coordinator?: unknown
  ): Promise<Record<string, unknown> | null>;

  queryWithRelations(
    bundle: string,
    table: string,
    filters?: Record<string, unknown>,
    opts?: QueryOptions,
    include?: string[],
    coordinator?: unknown
  ): Promise<Array<Record<string, unknown>>>;
}

export declare class BundleScopedData {
  constructor(
    dataLayer: DataLayer,
    bundleName: string,
    coordinator?: unknown
  );

  insert(
    table: string,
    attrs: Record<string, unknown>,
    opts?: InsertOptions
  ): Record<string, unknown>;

  find(
    table: string,
    id: string
  ): Record<string, unknown> | null;

  query(
    table: string,
    filters?: Record<string, unknown>,
    opts?: QueryOptions
  ): Array<Record<string, unknown>>;

  update(
    table: string,
    id: string,
    attrs: Record<string, unknown>
  ): Record<string, unknown> | null;

  delete(table: string, id: string): boolean;

  count(
    table: string,
    filters?: Record<string, unknown>
  ): number;

  transaction(fn: () => void): void;

  findWithRelations(
    table: string,
    id: string,
    include?: string[]
  ): Promise<Record<string, unknown> | null>;

  queryWithRelations(
    table: string,
    filters?: Record<string, unknown>,
    opts?: QueryOptions,
    include?: string[]
  ): Promise<Array<Record<string, unknown>>>;
}
