/**
 * Verification script: checks that all expected exports exist and
 * that package.json has "types" pointing to index.d.ts.
 * Prints "All datalayer exports match" on success, exits 1 on failure.
 */
import { existsSync } from 'fs';
import { readFileSync } from 'fs';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const pkgRoot = join(__dirname, '..');

// 1. Check index.d.ts exists
const dtsPath = join(pkgRoot, 'index.d.ts');
if (!existsSync(dtsPath)) {
  console.error('FAIL: index.d.ts does not exist');
  process.exit(1);
}

// 2. Check package.json has "types": "index.d.ts"
const pkg = JSON.parse(readFileSync(join(pkgRoot, 'package.json'), 'utf8'));
if (pkg.types !== 'index.d.ts') {
  console.error(`FAIL: package.json missing "types": "index.d.ts" (got: ${pkg.types})`);
  process.exit(1);
}

// 3. Check actual JS exports
import('/Users/michaeljabbour/dev/t/torque-service-datalayer/index.js').then(mod => {
  const expected = ['DataLayer', 'BundleScopedData', 'BundleIsolationError', 'ValidationError'];
  const missing = expected.filter(name => !mod[name]);
  if (missing.length > 0) {
    console.error(`FAIL: missing exports: ${missing.join(', ')}`);
    process.exit(1);
  }
  console.log('All datalayer exports match');
}).catch(err => {
  console.error(`FAIL: ${err.message}`);
  process.exit(1);
});
