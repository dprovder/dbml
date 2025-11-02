import { Parser } from '../../src';

describe('WHERE Debug', () => {
  it('should parse WHERE with AND correctly', () => {
    const dbml = `
Table Users {
  id int
  active boolean
}

Table Orders {
  order_id int
  date timestamp
}

Transform test[Users, Orders] {
  Users.id
  where: Users.active = true and Orders.date > '2022-01-01'
}
    `;

    console.log('Parsing...');
    const db = (new Parser()).parse(dbml, 'dbmlv2');

    console.log('Transforms:', JSON.stringify(db.transforms, null, 2));
    console.log('Columns:', db.transforms[0].columns.map(c => c.sourceColumn || c.expression));
    console.log('Filters:', db.transforms[0].filters);

    expect(db.transforms).toBeDefined();
    console.log('WHERE expression:', db.transforms[0].filters[0]?.expression);
  });
});
