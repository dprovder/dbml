import { Parser } from '../../src';
import { readFileSync } from 'fs';
import { join } from 'path';

describe('WHERE from parse test', () => {
  it('should use exact DBML from working dbml-parse test', () => {
    // Use the exact file that passes in dbml-parse
    const dbmlPath = join(__dirname, '../../../dbml-parse/tests/interpreter/input/transform_basic.in.dbml');
    const dbml = readFileSync(dbmlPath, 'utf8');

    const db = (new Parser()).parse(dbml, 'dbmlv2');

    console.log('Number of transforms:', db.transforms.length);
    if (db.transforms.length > 0) {
      console.log('First transform WHERE:', db.transforms[0].filters[0]?.expression);
    }

    expect(db.transforms).toBeDefined();
    expect(db.transforms.length).toBeGreaterThan(0);
  });
});
