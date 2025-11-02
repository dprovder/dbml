import { Parser } from '../../src';
import ModelExporter from '../../src/export/ModelExporter';
import TransformExporter from '../../src/export/TransformExporter';

describe('Transform SQL Export', () => {
  // Test 1: Basic SELECT with JOIN and WHERE
  it('should generate correct SQL for basic transform with join', () => {
    const dbml = `
      table Users {
        id int [pk]
        name varchar
        email varchar
        active boolean
      }

      table Orders {
        order_id int [pk]
        user_id int
        total_amount decimal
        date timestamp
      }

      transform SelectedUserOrders[Users, Orders] {
        Users.id
        Users.name
        Orders.order_id
        Orders.total_amount
        join: Users.id = Orders.user_id
        where: Users.active = true and Orders.date > '2022-01-01'
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('CREATE OR REPLACE VIEW "SelectedUserOrders" AS');
    expect(sql).toContain('SELECT');
    expect(sql).toContain('"Users"."id"');
    expect(sql).toContain('"Users"."name"');
    expect(sql).toContain('FROM "Users"');
    expect(sql).toContain('INNER JOIN "Orders"');
    expect(sql).toContain('"Users"."id" = "Orders"."user_id"');
    expect(sql).toContain('WHERE Users.active = true and Orders.date > \'2022-01-01\'');
  });

  // Test 2: Multiple joins
  it('should generate correct SQL for transform with multiple joins', () => {
    const dbml = `
      table Users {
        id int [pk]
      }

      table Orders {
        order_id int [pk]
        user_id int
      }

      table Payments {
        payment_id int [pk]
        order_id int
      }

      transform MultiJoin[Users, Orders, Payments] {
        Users.id
        Orders.order_id
        Payments.payment_id
        join: Users.id = Orders.user_id
        join: Orders.order_id = Payments.order_id
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('FROM "Users"');
    expect(sql).toContain('INNER JOIN "Orders"');
    expect(sql).toContain('INNER JOIN "Payments"');
    expect(sql).toContain('"Users"."id" = "Orders"."user_id"');
    expect(sql).toContain('"Orders"."order_id" = "Payments"."order_id"');
  });

  // Test 3: Aggregation with GROUP BY
  it('should generate correct SQL for aggregation', () => {
    const dbml = `
      table Orders {
        user_id int
        total_amount decimal
      }

      transform TotalOrderAmount[Orders] {
        Orders.user_id
        Orders.total_amount [agg: sum, as: total_per_user]
        group_by: Orders.user_id
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('SUM("Orders"."total_amount") AS "total_per_user"');
    expect(sql).toContain('GROUP BY "Orders"."user_id"');
  });

  // Test 4: Column aliases
  it('should generate correct SQL for column aliases', () => {
    const dbml = `
      table Users {
        id int
        name varchar
      }

      table Orders {
        order_id int
        user_id int
      }

      transform WithAliases[Users, Orders] {
        Users.id [as: user_id]
        Users.name [as: user_name]
        Orders.order_id [as: order_id]
        join: Users.id = Orders.user_id
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('"Users"."id" AS "user_id"');
    expect(sql).toContain('"Users"."name" AS "user_name"');
    expect(sql).toContain('"Orders"."order_id" AS "order_id"');
  });

  // Test 5: ORDER BY
  it('should generate correct SQL for ORDER BY', () => {
    const dbml = `
      table Orders {
        order_id int
        total_amount decimal
      }

      transform SortedOrders[Orders] {
        Orders.order_id
        Orders.total_amount
        order_by: Orders.total_amount DESC
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('ORDER BY "Orders"."total_amount" DESC');
  });

  // Test 6: LIMIT
  it('should generate correct SQL for LIMIT', () => {
    const dbml = `
      table Orders {
        id int
        total decimal
      }

      transform TopOrders[Orders] {
        Orders.id
        Orders.total
        order_by: Orders.total DESC
        limit: 10
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('LIMIT 10');
  });

  // Test 7: Window function with partition
  it('should generate correct SQL for window function', () => {
    const dbml = `
      table Orders {
        order_id int
        user_id int
        total_amount decimal
      }

      transform Ranked[Orders] {
        Orders.order_id
        Orders.total_amount [window: row_number, partition_by: Orders.user_id, order_by: Orders.total_amount DESC, as: rank]
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('ROW_NUMBER() OVER (PARTITION BY "Orders.user_id" ORDER BY "Orders"."total_amount" DESC)');
    expect(sql).toContain('AS "rank"');
  });

  // Test 8: Windowed aggregation
  it('should generate correct SQL for windowed aggregation', () => {
    const dbml = `
      table Orders {
        user_id int
        total_amount decimal
      }

      transform WindowedSum[Orders] {
        Orders.user_id
        Orders.total_amount [agg: sum, partition_by: Orders.user_id, as: running_total]
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('SUM("Orders"."total_amount") OVER (PARTITION BY "Orders.user_id")');
    expect(sql).toContain('AS "running_total"');
  });

  // Test 9: Full PostgreSQL export (all tables + transforms)
  it('should export transforms through ModelExporter', () => {
    const dbml = `
      table Users {
        id int [pk]
        name varchar
      }

      transform UserView[Users] {
        Users.id
        Users.name
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const sql = ModelExporter.export(db.normalize(), 'postgres');

    expect(sql).toContain('CREATE TABLE "Users"');
    expect(sql).toContain('CREATE OR REPLACE VIEW "UserView"');
  });

  // Test 10: Multiple transforms
  it('should export multiple transforms', () => {
    const dbml = `
      table Users {
        id int
        name varchar
      }

      transform View1[Users] {
        Users.id
      }

      transform View2[Users] {
        Users.name
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const sql = TransformExporter.exportTransforms(db.transforms, 'postgres');

    expect(sql).toContain('CREATE OR REPLACE VIEW "View1"');
    expect(sql).toContain('CREATE OR REPLACE VIEW "View2"');
  });

  // Test 11: Complex example with all features
  it('should handle complex transform with multiple features', () => {
    const dbml = `
      table Users {
        id int [pk]
        name varchar
        active boolean
      }

      table Orders {
        order_id int [pk]
        user_id int
        total_amount decimal
        date timestamp
      }

      transform ComplexView[Users, Orders] {
        Users.id [as: user_id]
        Users.name [as: user_name]
        Orders.total_amount [agg: sum, as: total_spent]
        Orders.total_amount [agg: avg, as: avg_spent]
        join: Users.id = Orders.user_id
        where: Users.active = true
        group_by: Users.id, Users.name
        order_by: Orders.total_amount DESC
        limit: 100
      }
    `;

    const db = (new Parser()).parse(dbml, 'dbmlv2');
    const transform = db.transforms[0];
    const sql = TransformExporter.exportTransform(transform, 'postgres');

    expect(sql).toContain('CREATE OR REPLACE VIEW "ComplexView"');
    expect(sql).toContain('"Users"."id" AS "user_id"');
    expect(sql).toContain('SUM("Orders"."total_amount") AS "total_spent"');
    expect(sql).toContain('AVG("Orders"."total_amount") AS "avg_spent"');
    expect(sql).toContain('INNER JOIN "Orders"');
    expect(sql).toContain('WHERE Users.active = true');
    expect(sql).toContain('GROUP BY "Users"."id", "Users"."name"');
    expect(sql).toContain('ORDER BY "Orders"."total_amount" DESC');
    expect(sql).toContain('LIMIT 100');
  });
});
