import { Parser } from '../../src';

describe('Transform Debug', () => {
  it('should parse a minimal transform', () => {
    const dbml = `
      Table users {
        id integer
        name varchar
      }

      Table orders {
        order_id integer
        user_id integer
      }

      Transform user_orders [users, orders] {
        users.id
        users.name
        join: users.id = orders.user_id
      }
    `;

    console.log('Attempting to parse...');
    try {
      const db = (new Parser()).parse(dbml, 'dbmlv2');
      console.log('Parsed successfully!');
      console.log('Transforms:', JSON.stringify(db.transforms, null, 2));
    } catch (error) {
      console.log('Parse error:', error);
      if (error.diags) {
        console.log('Diagnostics:', JSON.stringify(error.diags, null, 2));
      }
      throw error;
    }
  });
});
