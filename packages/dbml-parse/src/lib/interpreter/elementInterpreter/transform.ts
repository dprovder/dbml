import {
  ElementInterpreter, InterpreterDatabase, Transform, TransformColumn,
  TransformJoin, TransformFilter, TransformOrderBy, TransformSource, DerivedColumn,
} from '../types';
import {
  BlockExpressionNode, ElementDeclarationNode, InfixExpressionNode,
  ListExpressionNode, SyntaxNode, TransformColumnNode, TransformStatementNode,
} from '../../parser/nodes';
import { CompileError, CompileErrorCode } from '../../errors';
import { extractElementName, getTokenPosition } from '../utils';
import {
  destructureComplexVariable, extractVarNameFromPrimaryVariable,
  extractVariableFromExpression,
} from '../../analyzer/utils';
import { aggregateSettingList } from '../../analyzer/validator/utils';
import { SettingName } from '../../analyzer/types';

export class TransformInterpreter implements ElementInterpreter {
  private declarationNode: ElementDeclarationNode;
  private env: InterpreterDatabase;
  private transform: Partial<Transform>;

  constructor(declarationNode: ElementDeclarationNode, env: InterpreterDatabase) {
    this.declarationNode = declarationNode;
    this.env = env;
    this.transform = {
      name: undefined,
      schemaName: undefined,
      alias: null,
      sources: [],
      columns: [],
      joins: [],
      filters: [],
      token: undefined,
    };
  }

  interpret(): CompileError[] {
    this.transform.token = getTokenPosition(this.declarationNode);
    this.env.transforms.set(this.declarationNode, this.transform as Transform);

    return [
      ...this.interpretName(this.declarationNode.name!),
      ...this.interpretSources(this.declarationNode.sourceList),
      ...this.interpretBody(this.declarationNode.body as BlockExpressionNode),
    ];
  }

  private interpretName(nameNode: SyntaxNode): CompileError[] {
    const { name, schemaName } = extractElementName(nameNode);

    if (schemaName.length > 1) {
      this.transform.name = name;
      this.transform.schemaName = schemaName.join('.');
      return [new CompileError(CompileErrorCode.UNSUPPORTED, 'Nested schema is not supported', nameNode)];
    }

    this.transform.name = name;
    this.transform.schemaName = schemaName.length ? schemaName[0] : null;

    return [];
  }

  private interpretSources(sourceList?: ListExpressionNode): CompileError[] {
    if (!sourceList) {
      return [new CompileError(
        CompileErrorCode.UNEXPECTED_TOKEN,
        'Transform must specify source tables/transforms',
        this.declarationNode
      )];
    }

    const errors: CompileError[] = [];

    // The sourceList is a ListExpressionNode containing AttributeNode elements
    // But for transforms, we're using it to hold just variable names: [Users, Orders]
    // So we need to extract from elementList
    for (const attr of sourceList.elementList) {
      // Each "attribute" is actually just a source name
      // It could be: name, or value (if name is missing)
      const sourceExpr = attr.name || attr.value;
      if (!sourceExpr) continue;

      // Extract source name (could be "Users", "schema.Users", etc.)
      const fragments = destructureComplexVariable(sourceExpr).unwrap_or([]);

      if (fragments.length === 0) {
        errors.push(new CompileError(
          CompileErrorCode.INVALID_NAME,
          'Invalid source specification',
          sourceExpr
        ));
        continue;
      }

      const source: TransformSource = {
        name: fragments[fragments.length - 1],
        schemaName: fragments.length > 1 ? fragments.slice(0, -1).join('.') : undefined,
      };

      this.transform.sources!.push(source);
    }

    return errors;
  }

  private interpretBody(body: BlockExpressionNode): CompileError[] {
    const errors: CompileError[] = [];

    for (const element of body.body) {
      if (element instanceof TransformColumnNode) {
        errors.push(...this.interpretColumn(element));
      } else if (element instanceof TransformStatementNode) {
        errors.push(...this.interpretStatement(element));
      }
    }

    return errors;
  }

  private interpretColumn(columnNode: TransformColumnNode): CompileError[] {
    const errors: CompileError[] = [];
    const column: Partial<TransformColumn> = {};

    // Extract table.column from expression
    // Could be: Users.id, Orders.total_amount, (Orders.total_amount * 0.9), etc.
    if (!columnNode.expression) {
      errors.push(new CompileError(
        CompileErrorCode.INVALID_COLUMN,
        'Column expression is required',
        columnNode
      ));
      return errors;
    }

    // Try to extract table.column
    // If it's a simple member access (Users.id), extract both parts
    // If it's a complex expression, store the whole thing as expression
    const fragments = destructureComplexVariable(columnNode.expression).unwrap_or([]);

    if (fragments.length === 2) {
      // Simple case: Table.column
      column.sourceTable = fragments[0];
      column.sourceColumn = fragments[1];
    } else if (fragments.length === 1) {
      // Just a column name (might be from nested transform)
      column.sourceTable = '';
      column.sourceColumn = fragments[0];
    } else {
      // Complex expression - store as string for now
      // In MVP we don't parse complex expressions
      column.sourceTable = '';
      column.sourceColumn = '';
      column.expression = this.expressionToString(columnNode.expression);
    }

    // Parse attributes: [as: user_id, agg: sum, window: row_number, etc.]
    if (columnNode.attributeList) {
      const settingMap = aggregateSettingList(columnNode.attributeList).getValue();

      // Handle 'as' (alias)
      const asNode = settingMap[SettingName.As]?.at(0);
      if (asNode) {
        column.alias = extractVariableFromExpression(asNode.value).unwrap_or(undefined);
      }

      // Handle 'agg' (aggregation)
      const aggNode = settingMap[SettingName.Agg]?.at(0);
      if (aggNode) {
        column.aggregation = {
          function: extractVariableFromExpression(aggNode.value).unwrap_or(''),
        };

        // Check for partition_by with aggregation (windowed aggregation)
        const partitionNode = settingMap[SettingName.PartitionBy]?.at(0);
        if (partitionNode && partitionNode.value) {
          const partitionColumns = destructureComplexVariable(partitionNode.value).unwrap_or([]);
          column.aggregation.partitionBy = partitionColumns;
        }

        // Check for order_by with aggregation
        const orderByNode = settingMap[SettingName.OrderBy]?.at(0);
        if (orderByNode && orderByNode.value) {
          column.aggregation.orderBy = this.parseOrderBy(orderByNode.value);
        }
      }

      // Handle 'window' (window function)
      const windowNode = settingMap[SettingName.Window]?.at(0);
      if (windowNode) {
        column.window = {
          function: extractVariableFromExpression(windowNode.value).unwrap_or(''),
        };

        // Parse partition_by for window
        const partitionNode = settingMap[SettingName.PartitionBy]?.at(0);
        if (partitionNode && partitionNode.value) {
          const partitionColumns = destructureComplexVariable(partitionNode.value).unwrap_or([]);
          column.window.partitionBy = partitionColumns;
        }

        // Parse order_by for window
        const orderByNode = settingMap[SettingName.OrderBy]?.at(0);
        if (orderByNode && orderByNode.value) {
          column.window.orderBy = this.parseOrderBy(orderByNode.value);
        }

        // Parse frame
        const frameNode = settingMap[SettingName.Frame]?.at(0);
        if (frameNode && frameNode.value) {
          column.window.frame = this.expressionToString(frameNode.value);
        }
      }

      // Handle 'expr' (explicit expression)
      const exprNode = settingMap[SettingName.Expr]?.at(0);
      if (exprNode && exprNode.value) {
        column.expression = this.expressionToString(exprNode.value);
      }
    }

    this.transform.columns!.push(column as TransformColumn);
    return errors;
  }

  private interpretStatement(statement: TransformStatementNode): CompileError[] {
    const keyword = statement.keyword?.value.toLowerCase();

    switch (keyword) {
      case 'join':
        return this.interpretJoin(statement);
      case 'where':
        return this.interpretWhere(statement);
      case 'group_by':
        return this.interpretGroupBy(statement);
      case 'order_by':
        return this.interpretOrderBy(statement);
      case 'limit':
        return this.interpretLimit(statement);
      case 'using':
        return this.interpretUsing(statement);
      case 'add':
        return this.interpretAdd(statement);
      default:
        return [new CompileError(
          CompileErrorCode.UNEXPECTED_TOKEN,
          `Unknown transform keyword: ${keyword}`,
          statement
        )];
    }
  }

  private interpretJoin(statement: TransformStatementNode): CompileError[] {
    // Parse: join: Users.id = Orders.user_id
    // Expression should be an InfixExpressionNode with operator '='
    if (!statement.expression) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Join expression is required',
        statement
      )];
    }

    // Extract join predicate
    if (statement.expression instanceof InfixExpressionNode &&
        statement.expression.leftExpression &&
        statement.expression.rightExpression) {
      const infix = statement.expression;
      const operator = infix.op?.value || '=';

      // Left side: Users.id
      const leftFragments = destructureComplexVariable(infix.leftExpression).unwrap_or([]);
      // Right side: Orders.user_id
      const rightFragments = destructureComplexVariable(infix.rightExpression).unwrap_or([]);

      if (leftFragments.length === 2 && rightFragments.length === 2) {
        const join: TransformJoin = {
          leftTable: leftFragments[0],
          leftColumn: leftFragments[1],
          operator,
          rightTable: rightFragments[0],
          rightColumn: rightFragments[1],
          token: getTokenPosition(statement),
        };
        this.transform.joins!.push(join);
        return [];
      }
    }

    // Fallback: store as string expression
    return [new CompileError(
      CompileErrorCode.INVALID_NAME,
      'Invalid join syntax. Expected: Table.column = Table.column',
      statement
    )];
  }

  private interpretWhere(statement: TransformStatementNode): CompileError[] {
    // Parse: where: Users.active = true and Orders.date > '2022-01-01'
    // Store the whole expression as a string
    if (!statement.expression) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Where expression is required',
        statement
      )];
    }

    const filter: TransformFilter = {
      expression: this.expressionToString(statement.expression),
      token: getTokenPosition(statement),
    };

    this.transform.filters!.push(filter);
    return [];
  }

  private interpretGroupBy(statement: TransformStatementNode): CompileError[] {
    // Parse: group_by: Orders.user_id
    // or: group_by: Orders.user_id, Orders.category
    if (!statement.expression) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Group by expression is required',
        statement
      )];
    }

    // Extract column names
    const columns = destructureComplexVariable(statement.expression).unwrap_or([]);

    if (!this.transform.groupBy) {
      this.transform.groupBy = [];
    }

    // For group_by, we expect fully qualified names like Orders.user_id
    // Store them as strings
    this.transform.groupBy.push(...columns);

    return [];
  }

  private interpretOrderBy(statement: TransformStatementNode): CompileError[] {
    // Parse: order_by: Orders.total_amount DESC
    if (!statement.expression) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Order by expression is required',
        statement
      )];
    }

    const orderBy = this.parseOrderBy(statement.expression);

    if (!this.transform.orderBy) {
      this.transform.orderBy = [];
    }

    this.transform.orderBy.push(...orderBy);
    return [];
  }

  private interpretLimit(statement: TransformStatementNode): CompileError[] {
    // Parse: limit: 10
    if (!statement.expression) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Limit expression is required',
        statement
      )];
    }

    // Try to extract number
    const limitStr = extractVariableFromExpression(statement.expression).unwrap_or('0');
    const limit = parseInt(limitStr, 10);

    if (isNaN(limit)) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Limit must be a number',
        statement
      )];
    }

    this.transform.limit = limit;
    return [];
  }

  private interpretUsing(statement: TransformStatementNode): CompileError[] {
    // Parse: using: SomeOtherTransform[Users, Orders]
    // This indicates a nested transform
    // For MVP, we'll store basic info
    if (!statement.expression) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Using expression is required',
        statement
      )];
    }

    // Extract transform name
    const transformName = extractVariableFromExpression(statement.expression).unwrap_or('');

    this.transform.nestedTransform = {
      transformName,
      sources: [], // Would need to parse sources from expression
    };

    return [];
  }

  private interpretAdd(statement: TransformStatementNode): CompileError[] {
    // Parse: add: new_column = expression
    // This adds a derived column
    if (!statement.expression) {
      return [new CompileError(
        CompileErrorCode.INVALID_NAME,
        'Add expression is required',
        statement
      )];
    }

    // Expect: name = expression
    if (statement.expression instanceof InfixExpressionNode &&
        statement.expression.op?.value === '=' &&
        statement.expression.leftExpression &&
        statement.expression.rightExpression) {
      const name = extractVariableFromExpression(statement.expression.leftExpression).unwrap_or('');
      const expression = this.expressionToString(statement.expression.rightExpression);

      if (!this.transform.derivedColumns) {
        this.transform.derivedColumns = [];
      }

      const derived: DerivedColumn = {
        name,
        expression,
        token: getTokenPosition(statement),
      };

      this.transform.derivedColumns.push(derived);
      return [];
    }

    return [new CompileError(
      CompileErrorCode.INVALID_NAME,
      'Add syntax must be: column_name = expression',
      statement
    )];
  }

  // Helper: Parse order by expression
  private parseOrderBy(expression: SyntaxNode): TransformOrderBy[] {
    // Could be: Orders.total_amount DESC
    // Or: Orders.date ASC, Orders.id DESC
    // For now, parse simple case
    const fragments = destructureComplexVariable(expression).unwrap_or([]);

    const result: TransformOrderBy[] = [];

    // Look for direction (ASC/DESC) at the end
    const lastFragment = fragments[fragments.length - 1];
    let direction: 'ASC' | 'DESC' = 'ASC';

    if (lastFragment?.toLowerCase() === 'desc') {
      direction = 'DESC';
      fragments.pop();
    } else if (lastFragment?.toLowerCase() === 'asc') {
      direction = 'ASC';
      fragments.pop();
    }

    // Now fragments should be [Table, column]
    if (fragments.length === 2) {
      result.push({
        table: fragments[0],
        column: fragments[1],
        direction,
      });
    }

    return result;
  }

  // Helper: Convert expression node to string representation
  private expressionToString(expression: SyntaxNode): string {
    // For MVP, we'll create a simple string representation
    // In a full implementation, we'd recursively traverse the AST
    const fragments = destructureComplexVariable(expression).unwrap_or([]);
    return fragments.join('.');
  }
}
