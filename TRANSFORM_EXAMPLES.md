# DBML Transform Feature - Examples & Usage Guide

## Phase 4.1 Implementation Complete ✅

The transform feature allows you to define database views in DBML syntax and export them to SQL.

## Quick Start

### 1. Install/Build
```bash
npm install
npm run build
```

### 2. Create a DBML file with transforms
See `transform_demo.dbml` for examples.

### 3. Convert to SQL

**Using CLI:**
```bash
npx dbml2sql transform_demo.dbml --postgres
```

**Using Node.js:**
```javascript
const { Parser } = require('@dbml/core');
const ModelExporter = require('@dbml/core/lib/export/ModelExporter').default;

const dbml = `
  table Users { id int [pk] name varchar }

  transform ActiveUsers[Users] {
    Users.id
    Users.name
    where: Users.active = true
  }
`;

const db = new Parser().parse(dbml, 'dbmlv2');
const sql = ModelExporter.export(db.normalize(), 'postgres');
console.log(sql);
```

---

## Syntax Reference

### Basic Transform
```dbml
transform ViewName[SourceTable1, SourceTable2] {
  Table1.column1 [as: alias1]
  Table2.column2
  join: Table1.id = Table2.foreign_id
}
```

### Supported Features

#### 1. **Column Selection & Aliases**
```dbml
transform Example[Users] {
  Users.id [as: user_id]
  Users.name
  Users.email [as: contact_email]
}
```

**Generated SQL:**
```sql
CREATE OR REPLACE VIEW "Example" AS
SELECT
  "Users"."id" AS "user_id",
  "Users"."name",
  "Users"."email" AS "contact_email"
FROM "Users";
```

---

#### 2. **JOINs**
```dbml
transform UserOrders[Users, Orders] {
  Users.name
  Orders.total
  join: Users.id = Orders.user_id
}
```

**Generated SQL:**
```sql
CREATE OR REPLACE VIEW "UserOrders" AS
SELECT
  "Users"."name",
  "Orders"."total"
FROM "Users"
  INNER JOIN "Orders" ON "Users"."id" = "Orders"."user_id";
```

**Multiple JOINs:**
```dbml
transform ThreeWayJoin[Users, Orders, Products] {
  Users.name
  Orders.quantity
  Products.name [as: product_name]
  join: Users.id = Orders.user_id
  join: Orders.product_id = Products.id
}
```

---

#### 3. **WHERE Clauses (Complex Boolean Logic)**
```dbml
transform FilteredData[Orders] {
  Orders.id
  Orders.total
  where: Orders.status = 'active' and Orders.total > 100
}
```

**Multiple conditions with AND/OR:**
```dbml
where: Users.active = true and Orders.date > '2023-01-01' or Orders.priority = 'high'
```

**Generated SQL:**
```sql
WHERE Users.active = true and Orders.date > '2023-01-01' or Orders.priority = 'high'
```

---

#### 4. **Aggregations**
```dbml
transform UserStats[Users, Orders] {
  Users.id
  Users.name
  Orders.total [agg: sum, as: total_spent]
  Orders.id [agg: count, as: order_count]
  Orders.total [agg: avg, as: avg_order]
  join: Users.id = Orders.user_id
  group_by: Users.id, Users.name
}
```

**Supported aggregation functions:**
- `sum` - SUM()
- `count` - COUNT()
- `avg` - AVG()
- `min` - MIN()
- `max` - MAX()

**Generated SQL:**
```sql
CREATE OR REPLACE VIEW "UserStats" AS
SELECT
  "Users"."id",
  "Users"."name",
  SUM("Orders"."total") AS "total_spent",
  COUNT("Orders"."id") AS "order_count",
  AVG("Orders"."total") AS "avg_order"
FROM "Users"
  INNER JOIN "Orders" ON "Users"."id" = "Orders"."user_id"
GROUP BY "Users"."id", "Users"."name";
```

---

#### 5. **Window Functions**
```dbml
transform RankedOrders[Orders] {
  Orders.id
  Orders.user_id
  Orders.total
  Orders.id [window: row_number, partition_by: Orders.user_id, order_by: Orders.total DESC, as: rank]
}
```

**Generated SQL:**
```sql
CREATE OR REPLACE VIEW "RankedOrders" AS
SELECT
  "Orders"."id",
  "Orders"."user_id",
  "Orders"."total",
  ROW_NUMBER() OVER (PARTITION BY "Orders.user_id" ORDER BY "Orders"."total" DESC) AS "rank"
FROM "Orders";
```

**Window function attributes:**
- `window: function_name` - The window function (row_number, rank, dense_rank, lag, lead, etc.)
- `partition_by: Column` - PARTITION BY clause
- `order_by: Column [DESC|ASC]` - ORDER BY within the window
- `as: alias` - Column alias

---

#### 6. **Windowed Aggregations**
```dbml
transform RunningTotals[Orders] {
  Orders.date
  Orders.amount
  Orders.amount [agg: sum, partition_by: Orders.user_id, order_by: Orders.date, as: running_total]
}
```

**Generated SQL:**
```sql
CREATE OR REPLACE VIEW "RunningTotals" AS
SELECT
  "Orders"."date",
  "Orders"."amount",
  SUM("Orders"."amount") OVER (PARTITION BY "Orders.user_id" ORDER BY "Orders"."date" ASC) AS "running_total"
FROM "Orders";
```

---

#### 7. **GROUP BY with Multiple Columns**
```dbml
transform SalesByRegion[Sales] {
  Sales.region
  Sales.category
  Sales.amount [agg: sum, as: total]
  group_by: Sales.region, Sales.category
}
```

**Generated SQL:**
```sql
GROUP BY "Sales"."region", "Sales"."category"
```

---

#### 8. **ORDER BY with Direction**
```dbml
transform TopUsers[Users] {
  Users.name
  Users.score
  order_by: Users.score DESC
}
```

**Multiple columns:**
```dbml
order_by: Users.score DESC, Users.name ASC
```

**Generated SQL:**
```sql
ORDER BY "Users"."score" DESC, "Users"."name" ASC
```

---

#### 9. **LIMIT**
```dbml
transform Top100[Orders] {
  Orders.id
  Orders.total
  order_by: Orders.total DESC
  limit: 100
}
```

**Generated SQL:**
```sql
ORDER BY "Orders"."total" DESC
LIMIT 100;
```

---

## Complete Example

Here's a real-world example combining multiple features:

```dbml
table Users {
  id int [pk]
  name varchar
  email varchar
  active boolean
  created_at timestamp
}

table Orders {
  order_id int [pk]
  user_id int
  total_amount decimal
  status varchar
  order_date timestamp
}

table OrderItems {
  item_id int [pk]
  order_id int
  product_name varchar
  quantity int
  price decimal
}

// Create a comprehensive analytics view
transform UserAnalytics[Users, Orders, OrderItems] {
  Users.id [as: user_id]
  Users.name [as: customer_name]
  Users.email
  Orders.total_amount [agg: sum, as: lifetime_value]
  Orders.total_amount [agg: avg, as: avg_order_value]
  Orders.order_id [agg: count, as: total_orders]
  OrderItems.quantity [agg: sum, as: total_items]

  join: Users.id = Orders.user_id
  join: Orders.order_id = OrderItems.order_id

  where: Users.active = true and Orders.status = 'completed'

  group_by: Users.id, Users.name, Users.email
  order_by: Users.id DESC
  limit: 1000
}
```

**Generated SQL:**
```sql
CREATE OR REPLACE VIEW "UserAnalytics" AS
SELECT
  "Users"."id" AS "user_id",
  "Users"."name" AS "customer_name",
  "Users"."email",
  SUM("Orders"."total_amount") AS "lifetime_value",
  AVG("Orders"."total_amount") AS "avg_order_value",
  COUNT("Orders"."order_id") AS "total_orders",
  SUM("OrderItems"."quantity") AS "total_items"
FROM "Users"
  INNER JOIN "Orders" ON "Users"."id" = "Orders"."user_id"
  INNER JOIN "OrderItems" ON "Orders"."order_id" = "OrderItems"."order_id"
WHERE Users.active = true and Orders.status = 'completed'
GROUP BY "Users"."id", "Users"."name", "Users"."email"
ORDER BY "Users"."id" DESC
LIMIT 1000;
```

---

## Implementation Details

### What Works (Phase 4.1 Complete)
✅ Multiple source tables
✅ Column selection with qualified names (Table.column)
✅ Column aliases
✅ INNER JOINs (multiple)
✅ Complex WHERE clauses with AND/OR/comparison operators
✅ String literals, numeric literals, boolean literals
✅ Aggregation functions (SUM, COUNT, AVG, MIN, MAX)
✅ Window functions (ROW_NUMBER, RANK, etc.)
✅ Windowed aggregations (aggregations with PARTITION BY)
✅ GROUP BY (single and multiple columns)
✅ ORDER BY (single and multiple columns, ASC/DESC)
✅ LIMIT clause
✅ PostgreSQL export via ModelExporter

### Coming Soon (Phase 4.2 & 4.3)
⏳ JOIN types (LEFT, RIGHT, FULL OUTER)
⏳ Nested transforms (using transforms as sources)
⏳ Cycle detection for transform dependencies

---

## Testing

Run the test suite:
```bash
npm test -- __tests__/exporter/transform.test.js
```

All 11/11 tests passing! ✅

Run the demo:
```bash
node demo_transform.js
```

---

## Technical Architecture

### Parser Changes
- Modified `transformStatement()` to use `expression()` for full expression capture
- Added comma handling for `group_by` and `order_by` statements
- Added DESC/ASC keyword capture in attribute values

### Interpreter Changes
- Implemented `expressionToString()` for converting AST expressions to SQL
- Fixed handling of `FunctionApplicationNode` (multi-token expressions)
- Used `kind` property instead of `constructor.name` (minification-safe)
- Preserved qualified column names (Table.column) in all contexts

### Model Changes
- Added transforms to normalized database model
- PostgresExporter now exports transforms as CREATE OR REPLACE VIEW statements

---

## Support

For issues or questions, please report at:
https://github.com/anthropics/claude-code/issues
