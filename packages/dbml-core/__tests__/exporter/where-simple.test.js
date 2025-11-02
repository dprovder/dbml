import { Parser } from '../../src';

describe('WHERE Simple', () => {
  it('should parse simple WHERE with string comparison', () => {
    const dbml = `
Table Orders {
  date timestamp
}

Transform test[Orders] {
  Orders.date
  where: Orders.date > '2022-01-01'
}
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    console.log('Columns:', db.transforms[0].columns.map(c => c.sourceColumn || c.expression));
    console.log('WHERE expression:', db.transforms[0].filters[0]?.expression);

    // Check if string got parsed as a column
    expect(db.transforms[0].columns.length).toBe(1); // Should only be Orders.date
    expect(db.transforms[0].filters[0]?.expression).toContain('2022-01-01');
  });
});
