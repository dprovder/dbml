import _ from 'lodash';

/**
 * TransformExporter - Generates SQL views from Transform definitions
 *
 * Supports:
 * - Column selection with aliases
 * - Aggregation functions (SUM, AVG, COUNT, etc.)
 * - Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
 * - Joins (INNER JOIN by default)
 * - WHERE clauses
 * - GROUP BY, ORDER BY, LIMIT
 */
class TransformExporter {
  /**
   * Export a single transform as a CREATE VIEW statement
   * @param {Object} transform - Transform object from interpreter
   * @param {string} dialect - SQL dialect ('postgres', 'mysql', 'mssql', 'oracle')
   * @returns {string} SQL CREATE VIEW statement
   */
  static exportTransform(transform, dialect = 'postgres') {
    const viewName = transform.name;
    const selectClause = TransformExporter.buildSelectClause(transform, dialect);
    const fromClause = TransformExporter.buildFromClause(transform, dialect);
    const joinClauses = TransformExporter.buildJoinClauses(transform, dialect);
    const whereClause = TransformExporter.buildWhereClause(transform, dialect);
    const groupByClause = TransformExporter.buildGroupByClause(transform, dialect);
    const orderByClause = TransformExporter.buildOrderByClause(transform, dialect);
    const limitClause = TransformExporter.buildLimitClause(transform, dialect);

    const parts = [
      `-- Transform: ${viewName}`,
      `CREATE OR REPLACE VIEW "${viewName}" AS`,
      'SELECT',
      selectClause,
      `FROM ${fromClause}`,
      ...joinClauses,
      whereClause,
      groupByClause,
      orderByClause,
      limitClause,
    ].filter(Boolean);

    return parts.join('\n') + ';\n';
  }

  /**
   * Build SELECT clause with columns, aggregations, and window functions
   */
  static buildSelectClause(transform, dialect) {
    if (!transform.columns || transform.columns.length === 0) {
      return '  *';
    }

    const columnExpressions = transform.columns.map((col) => {
      // Handle aggregation
      if (col.aggregation) {
        return TransformExporter.buildAggregationColumn(col, dialect);
      }

      // Handle window function
      if (col.window) {
        return TransformExporter.buildWindowColumn(col, dialect);
      }

      // Handle explicit expression
      if (col.expression && col.expression !== '' && !col.sourceColumn) {
        const alias = col.alias ? ` AS "${col.alias}"` : '';
        return `  ${col.expression}${alias}`;
      }

      // Handle regular column
      const columnRef = col.sourceTable
        ? `"${col.sourceTable}"."${col.sourceColumn}"`
        : `"${col.sourceColumn}"`;
      const alias = col.alias ? ` AS "${col.alias}"` : '';
      return `  ${columnRef}${alias}`;
    });

    return columnExpressions.join(',\n');
  }

  /**
   * Build aggregation column (with optional windowing via partition_by)
   */
  static buildAggregationColumn(col, dialect) {
    const func = col.aggregation.function.toUpperCase();
    const expr = col.sourceTable
      ? `"${col.sourceTable}"."${col.sourceColumn}"`
      : `"${col.sourceColumn}"`;

    let aggregateExpr = `${func}(${expr})`;

    // Windowed aggregation (has partition_by)
    if (col.aggregation.partitionBy && col.aggregation.partitionBy.length > 0) {
      const partitionCols = col.aggregation.partitionBy.map(p => `"${p}"`).join(', ');
      let overClause = `PARTITION BY ${partitionCols}`;

      if (col.aggregation.orderBy && col.aggregation.orderBy.length > 0) {
        const orderCols = col.aggregation.orderBy.map(o =>
          `"${o.table}"."${o.column}" ${o.direction}`
        ).join(', ');
        overClause += ` ORDER BY ${orderCols}`;
      }

      aggregateExpr = `${func}(${expr}) OVER (${overClause})`;
    }

    const alias = col.alias ? ` AS "${col.alias}"` : '';
    return `  ${aggregateExpr}${alias}`;
  }

  /**
   * Build window function column
   */
  static buildWindowColumn(col, dialect) {
    const func = col.window.function.toUpperCase();
    let args = '';

    // Some window functions take arguments (LAG, LEAD, NTH_VALUE)
    if (['LAG', 'LEAD', 'NTH_VALUE', 'FIRST_VALUE', 'LAST_VALUE'].includes(func)) {
      const expr = col.sourceTable
        ? `"${col.sourceTable}"."${col.sourceColumn}"`
        : `"${col.sourceColumn}"`;
      args = expr;
    } else if (['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'PERCENT_RANK', 'CUME_DIST'].includes(func)) {
      // These don't take arguments
      args = '';
    } else {
      // Default: use the column as argument
      const expr = col.sourceTable
        ? `"${col.sourceTable}"."${col.sourceColumn}"`
        : `"${col.sourceColumn}"`;
      args = expr;
    }

    let overClause = '';

    if (col.window.partitionBy && col.window.partitionBy.length > 0) {
      const partitionCols = col.window.partitionBy.map(p => `"${p}"`).join(', ');
      overClause = `PARTITION BY ${partitionCols}`;
    }

    if (col.window.orderBy && col.window.orderBy.length > 0) {
      const orderCols = col.window.orderBy.map(o =>
        `"${o.table}"."${o.column}" ${o.direction}`
      ).join(', ');
      overClause += (overClause ? ' ' : '') + `ORDER BY ${orderCols}`;
    }

    if (col.window.frame) {
      overClause += (overClause ? ' ' : '') + col.window.frame;
    }

    const windowExpr = args ? `${func}(${args})` : `${func}()`;
    const fullExpr = overClause ? `${windowExpr} OVER (${overClause})` : `${windowExpr} OVER ()`;
    const alias = col.alias ? ` AS "${col.alias}"` : '';

    return `  ${fullExpr}${alias}`;
  }

  /**
   * Build FROM clause (first source)
   */
  static buildFromClause(transform, dialect) {
    if (!transform.sources || transform.sources.length === 0) {
      throw new Error(`Transform ${transform.name} has no sources`);
    }

    const firstSource = transform.sources[0];
    const tableName = firstSource.schemaName
      ? `"${firstSource.schemaName}"."${firstSource.name}"`
      : `"${firstSource.name}"`;

    const alias = firstSource.alias ? ` AS "${firstSource.alias}"` : '';
    return `${tableName}${alias}`;
  }

  /**
   * Build JOIN clauses (for additional sources)
   */
  static buildJoinClauses(transform, dialect) {
    if (!transform.sources || transform.sources.length <= 1) {
      return [];
    }

    const joins = [];
    const remainingSources = transform.sources.slice(1);

    remainingSources.forEach((source) => {
      // Find join conditions for this source
      const joinConditions = (transform.joins || []).filter(j =>
        j.rightTable === source.name || j.leftTable === source.name
      );

      const tableName = source.schemaName
        ? `"${source.schemaName}"."${source.name}"`
        : `"${source.name}"`;

      const alias = source.alias ? ` AS "${source.alias}"` : '';

      if (joinConditions.length > 0) {
        // Build ON clause
        const onClauses = joinConditions.map(j => {
          const leftRef = `"${j.leftTable}"."${j.leftColumn}"`;
          const rightRef = `"${j.rightTable}"."${j.rightColumn}"`;
          return `${leftRef} ${j.operator} ${rightRef}`;
        }).join(' AND ');

        joins.push(`  INNER JOIN ${tableName}${alias} ON ${onClauses}`);
      } else {
        // No explicit join condition - CROSS JOIN
        joins.push(`  CROSS JOIN ${tableName}${alias}`);
      }
    });

    return joins;
  }

  /**
   * Build WHERE clause
   */
  static buildWhereClause(transform, dialect) {
    if (!transform.filters || transform.filters.length === 0) {
      return null;
    }

    const conditions = transform.filters.map(f => f.expression).join(' AND ');
    return `WHERE ${conditions}`;
  }

  /**
   * Build GROUP BY clause
   */
  static buildGroupByClause(transform, dialect) {
    if (!transform.groupBy || transform.groupBy.length === 0) {
      return null;
    }

    // Group by columns might be fully qualified (Table.column) or just column names
    const columns = transform.groupBy.map(col => {
      if (col.includes('.')) {
        const parts = col.split('.');
        return `"${parts[0]}"."${parts[1]}"`;
      }
      return `"${col}"`;
    }).join(', ');

    return `GROUP BY ${columns}`;
  }

  /**
   * Build ORDER BY clause
   */
  static buildOrderByClause(transform, dialect) {
    if (!transform.orderBy || transform.orderBy.length === 0) {
      return null;
    }

    const columns = transform.orderBy.map(o =>
      `"${o.table}"."${o.column}" ${o.direction}`
    ).join(', ');

    return `ORDER BY ${columns}`;
  }

  /**
   * Build LIMIT clause
   */
  static buildLimitClause(transform, dialect) {
    if (!transform.limit) {
      return null;
    }

    return `LIMIT ${transform.limit}`;
  }

  /**
   * Export multiple transforms
   */
  static exportTransforms(transforms, dialect = 'postgres') {
    if (!transforms || transforms.length === 0) {
      return '';
    }

    return transforms.map(t => TransformExporter.exportTransform(t, dialect)).join('\n');
  }
}

export default TransformExporter;
