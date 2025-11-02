import { Parser } from '../../src';
import TransformExporter from '../../src/export/TransformExporter';

describe('Transform SQL Export - Simple', () => {
  it('should parse and export a basic transform', () => {
    // Using exact syntax from transform_basic.in.dbml that we know works
    const dbml = `
table Users {
  id int [pk]
  name varchar
}

table Orders {
  order_id int [pk]
  user_id int
}

transform SelectedUserOrders[Users, Orders] {
  Users.id
  Users.name
  Orders.order_id
  join: Users.id = Orders.user_id
}
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');

    console.log('Database parsed:', db);
    console.log('Transforms:', db.transforms);

    expect(db.transforms).toBeDefined();
    expect(db.transforms.length).toBe(1);
    expect(db.transforms[0].name).toBe('SelectedUserOrders');

    const sql = TransformExporter.exportTransform(db.transforms[0], 'postgres');
    console.log('Generated SQL:', sql);

    expect(sql).toContain('CREATE OR REPLACE VIEW');
    expect(sql).toContain('SelectedUserOrders');
    expect(sql).toContain('INNER JOIN');
  });
});
