import fs from 'fs';
import path from 'path';
import _ from 'lodash';

/**
 * DbtExporter - Generates dbt models from Transform definitions
 *
 * Generates:
 * - .sql files for each transform (using {{ ref() }} for sources)
 * - schema.yml with model documentation
 */
class DbtExporter {
  /**
   * Export all transforms as dbt models
   * @param {Array} transforms - Array of Transform objects
   * @param {string} outputDir - Output directory for models
   * @returns {Object} Summary of generated files
   */
  static exportTransforms(transforms, outputDir) {
    if (!transforms || transforms.length === 0) {
      return {
        modelsGenerated: 0,
        outputDir: null,
      };
    }

    const modelsDir = path.join(outputDir, 'models');

    // Create models directory if it doesn't exist
    if (!fs.existsSync(modelsDir)) {
      fs.mkdirSync(modelsDir, { recursive: true });
    }

    // Generate one .sql file per transform
    transforms.forEach((transform) => {
      const sql = DbtExporter.generateDbtModel(transform);
      const filename = path.join(modelsDir, `${_.snakeCase(transform.name)}.sql`);
      fs.writeFileSync(filename, sql);
    });

    // Generate schema.yml
    const schemaYml = DbtExporter.generateSchemaYml(transforms);
    fs.writeFileSync(path.join(modelsDir, 'schema.yml'), schemaYml);

    return {
      modelsGenerated: transforms.length,
      outputDir: modelsDir,
      files: [
        ...transforms.map(t => `${_.snakeCase(t.name)}.sql`),
        'schema.yml',
      ],
    };
  }

  /**
   * Generate a single dbt model SQL file
   */
  static generateDbtModel(transform) {
    const selectClause = DbtExporter.buildSelectClause(transform);
    const fromClause = DbtExporter.buildFromClause(transform);
    const joinClauses = DbtExporter.buildJoinClauses(transform);
    const whereClause = DbtExporter.buildWhereClause(transform);
    const groupByClause = DbtExporter.buildGroupByClause(transform);
    const orderByClause = DbtExporter.buildOrderByClause(transform);
    const limitClause = DbtExporter.buildLimitClause(transform);

    const parts = [
      '{{',
      '  config(',
      '    materialized=\'view\'',
      '  )',
      '}}',
      '',
      `-- Transform: ${transform.name}`,
      '-- Generated from DBML',
      '',
      'SELECT',
      selectClause,
      `FROM ${fromClause}`,
      ...joinClauses,
      whereClause,
      groupByClause,
      orderByClause,
      limitClause,
    ].filter(Boolean);

    return parts.join('\n') + '\n';
  }

  /**
   * Build SELECT clause (same logic as TransformExporter but with dbt quoting)
   */
  static buildSelectClause(transform) {
    if (!transform.columns || transform.columns.length === 0) {
      return '  *';
    }

    const columnExpressions = transform.columns.map((col) => {
      if (col.aggregation) {
        return DbtExporter.buildAggregationColumn(col);
      }

      if (col.window) {
        return DbtExporter.buildWindowColumn(col);
      }

      if (col.expression && col.expression !== '' && !col.sourceColumn) {
        const alias = col.alias ? ` AS ${col.alias}` : '';
        return `  ${col.expression}${alias}`;
      }

      const columnRef = col.sourceTable
        ? `${col.sourceTable}.${col.sourceColumn}`
        : col.sourceColumn;
      const alias = col.alias ? ` AS ${col.alias}` : '';
      return `  ${columnRef}${alias}`;
    });

    return columnExpressions.join(',\n');
  }

  static buildAggregationColumn(col) {
    const func = col.aggregation.function.toUpperCase();
    const expr = col.sourceTable
      ? `${col.sourceTable}.${col.sourceColumn}`
      : col.sourceColumn;

    let aggregateExpr = `${func}(${expr})`;

    if (col.aggregation.partitionBy && col.aggregation.partitionBy.length > 0) {
      const partitionCols = col.aggregation.partitionBy.join(', ');
      let overClause = `PARTITION BY ${partitionCols}`;

      if (col.aggregation.orderBy && col.aggregation.orderBy.length > 0) {
        const orderCols = col.aggregation.orderBy.map(o =>
          `${o.table}.${o.column} ${o.direction}`
        ).join(', ');
        overClause += ` ORDER BY ${orderCols}`;
      }

      aggregateExpr = `${func}(${expr}) OVER (${overClause})`;
    }

    const alias = col.alias ? ` AS ${col.alias}` : '';
    return `  ${aggregateExpr}${alias}`;
  }

  static buildWindowColumn(col) {
    const func = col.window.function.toUpperCase();
    let args = '';

    if (['LAG', 'LEAD', 'NTH_VALUE', 'FIRST_VALUE', 'LAST_VALUE'].includes(func)) {
      const expr = col.sourceTable
        ? `${col.sourceTable}.${col.sourceColumn}`
        : col.sourceColumn;
      args = expr;
    } else if (!['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'PERCENT_RANK', 'CUME_DIST'].includes(func)) {
      const expr = col.sourceTable
        ? `${col.sourceTable}.${col.sourceColumn}`
        : col.sourceColumn;
      args = expr;
    }

    let overClause = '';

    if (col.window.partitionBy && col.window.partitionBy.length > 0) {
      const partitionCols = col.window.partitionBy.join(', ');
      overClause = `PARTITION BY ${partitionCols}`;
    }

    if (col.window.orderBy && col.window.orderBy.length > 0) {
      const orderCols = col.window.orderBy.map(o =>
        `${o.table}.${o.column} ${o.direction}`
      ).join(', ');
      overClause += (overClause ? ' ' : '') + `ORDER BY ${orderCols}`;
    }

    if (col.window.frame) {
      overClause += (overClause ? ' ' : '') + col.window.frame;
    }

    const windowExpr = args ? `${func}(${args})` : `${func}()`;
    const fullExpr = overClause ? `${windowExpr} OVER (${overClause})` : `${windowExpr} OVER ()`;
    const alias = col.alias ? ` AS ${col.alias}` : '';

    return `  ${fullExpr}${alias}`;
  }

  /**
   * Build FROM clause with {{ ref() }}
   */
  static buildFromClause(transform) {
    if (!transform.sources || transform.sources.length === 0) {
      throw new Error(`Transform ${transform.name} has no sources`);
    }

    const firstSource = transform.sources[0];
    const refName = _.snakeCase(firstSource.name);
    const alias = firstSource.alias || firstSource.name;

    return `{{ ref('${refName}') }} AS ${alias}`;
  }

  /**
   * Build JOIN clauses with {{ ref() }}
   */
  static buildJoinClauses(transform) {
    if (!transform.sources || transform.sources.length <= 1) {
      return [];
    }

    const joins = [];
    const remainingSources = transform.sources.slice(1);

    remainingSources.forEach((source) => {
      const joinConditions = (transform.joins || []).filter(j =>
        j.rightTable === source.name || j.leftTable === source.name
      );

      const refName = _.snakeCase(source.name);
      const alias = source.alias || source.name;

      if (joinConditions.length > 0) {
        const onClauses = joinConditions.map(j => {
          return `${j.leftTable}.${j.leftColumn} ${j.operator} ${j.rightTable}.${j.rightColumn}`;
        }).join(' AND ');

        joins.push(`  INNER JOIN {{ ref('${refName}') }} AS ${alias} ON ${onClauses}`);
      } else {
        joins.push(`  CROSS JOIN {{ ref('${refName}') }} AS ${alias}`);
      }
    });

    return joins;
  }

  static buildWhereClause(transform) {
    if (!transform.filters || transform.filters.length === 0) {
      return null;
    }

    const conditions = transform.filters.map(f => f.expression).join(' AND ');
    return `WHERE ${conditions}`;
  }

  static buildGroupByClause(transform) {
    if (!transform.groupBy || transform.groupBy.length === 0) {
      return null;
    }

    const columns = transform.groupBy.join(', ');
    return `GROUP BY ${columns}`;
  }

  static buildOrderByClause(transform) {
    if (!transform.orderBy || transform.orderBy.length === 0) {
      return null;
    }

    const columns = transform.orderBy.map(o =>
      `${o.table}.${o.column} ${o.direction}`
    ).join(', ');

    return `ORDER BY ${columns}`;
  }

  static buildLimitClause(transform) {
    if (!transform.limit) {
      return null;
    }

    return `LIMIT ${transform.limit}`;
  }

  /**
   * Generate schema.yml for dbt
   */
  static generateSchemaYml(transforms) {
    const models = transforms.map((t) => {
      const columns = (t.columns || []).map((c) => {
        const columnName = c.alias || c.sourceColumn || 'unknown';
        let description = '';

        if (c.aggregation) {
          description = `Aggregated column: ${c.aggregation.function}(${c.sourceColumn})`;
        } else if (c.window) {
          description = `Window function: ${c.window.function}(${c.sourceColumn})`;
        } else {
          description = `Column from ${c.sourceTable}.${c.sourceColumn}`;
        }

        return {
          name: columnName,
          description,
        };
      });

      return {
        name: _.snakeCase(t.name),
        description: `Transform: ${t.name}`,
        columns,
      };
    });

    // Generate YAML manually (simple format)
    const yaml = ['version: 2', '', 'models:'];

    models.forEach((model) => {
      yaml.push(`  - name: ${model.name}`);
      yaml.push(`    description: ${model.description}`);
      if (model.columns && model.columns.length > 0) {
        yaml.push('    columns:');
        model.columns.forEach((col) => {
          yaml.push(`      - name: ${col.name}`);
          yaml.push(`        description: ${col.description}`);
        });
      }
      yaml.push('');
    });

    return yaml.join('\n');
  }
}

export default DbtExporter;
