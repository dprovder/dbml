import _ from 'lodash';
import {
  hasWhiteSpaceOrUpperCase,
  shouldPrintSchema,
  buildJunctionFields1,
  buildJunctionFields2,
  buildNewTableName,
  hasWhiteSpace,
} from './utils';
import { shouldPrintSchemaName } from '../model_structure/utils';
import TransformExporter from './TransformExporter';

// DuckDB built-in data types
// DuckDB is PostgreSQL-compatible with some extensions
// Based on https://duckdb.org/docs/sql/data_types/overview

const DUCKDB_BUILTIN_TYPES = [
  // Numeric types
  'TINYINT',
  'SMALLINT',
  'INTEGER',
  'INT',
  'BIGINT',
  'HUGEINT',  // DuckDB-specific: 128-bit integer
  'UTINYINT', // DuckDB-specific: unsigned 8-bit
  'USMALLINT', // DuckDB-specific: unsigned 16-bit
  'UINTEGER', // DuckDB-specific: unsigned 32-bit
  'UBIGINT',  // DuckDB-specific: unsigned 64-bit
  'UHUGEINT', // DuckDB-specific: unsigned 128-bit
  'DECIMAL',
  'NUMERIC',
  'REAL',
  'DOUBLE',
  'DOUBLE PRECISION',
  'FLOAT',
  'FLOAT4',
  'FLOAT8',

  // String types
  'VARCHAR',
  'CHAR',
  'CHARACTER',
  'CHARACTER VARYING',
  'BPCHAR',
  'TEXT',
  'STRING', // DuckDB alias for VARCHAR

  // Binary data types
  'BLOB',
  'BYTEA',
  'BINARY',
  'VARBINARY',

  // Date/time types
  'DATE',
  'TIME',
  'TIMESTAMP',
  'TIMESTAMPTZ',
  'TIMESTAMP WITH TIME ZONE',
  'TIMESTAMP WITHOUT TIME ZONE',
  'INTERVAL',

  // Boolean type
  'BOOLEAN',
  'BOOL',

  // UUID type
  'UUID',

  // JSON type
  'JSON',

  // Bit string types
  'BIT',
  'BITSTRING',

  // Special types
  'NULL',
];

const DUCKDB_RESERVED_KEYWORDS = [
  'USER',
  'ORDER',
  'GROUP',
];

class DuckDBExporter {
  static exportEnums (enumIds, model) {
    return enumIds.map((enumId) => {
      const _enum = model.enums[enumId];
      const schema = model.schemas[_enum.schemaId];

      const enumName = `${shouldPrintSchema(schema, model) ? `"${schema.name}".` : ''}"${_enum.name}"`;

      const enumValueArr = _enum.valueIds.map((valueId) => {
        const value = model.enumValues[valueId];
        return `  '${value.name}'`;
      });
      const enumValueStr = enumValueArr.join(',\n');
      const enumLine = `CREATE TYPE ${enumName} AS ENUM (\n${enumValueStr}\n);\n`;

      return [enumName, enumLine];
    });
  }

  static getFieldLines (tableId, model, enumSet) {
    const table = model.tables[tableId];

    const lines = table.fieldIds.map((fieldId) => {
      const field = model.fields[fieldId];

      let line = '';
      if (field.increment) {
        // DuckDB auto-increment: use INTEGER with PRIMARY KEY (creates implicit sequence)
        // or BIGINT for larger sequences
        const typeRaw = field.type.type_name.toUpperCase();
        let type = '';
        if (typeRaw === 'BIGINT') {
          type = 'BIGINT';
        } else {
          type = 'INTEGER';
        }
        line = `"${field.name}" ${type}`;
      } else if (!field.type.schemaName || !shouldPrintSchemaName(field.type.schemaName)) {
        const originalTypeName = field.type.type_name;
        const upperCaseTypeName = originalTypeName.toUpperCase();

        const shouldDoubleQuote = !DUCKDB_BUILTIN_TYPES.includes(upperCaseTypeName)
          && (hasWhiteSpaceOrUpperCase(originalTypeName) || DUCKDB_RESERVED_KEYWORDS.includes(upperCaseTypeName));

        const typeName = shouldDoubleQuote ? `"${originalTypeName}"` : originalTypeName;
        line = `"${field.name}" ${typeName}`;
      } else if (field.type.originalTypeName) {
        line = `"${field.name}" "${field.type.schemaName}"."${field.type.originalTypeName}"`;
      } else {
        const schemaName = hasWhiteSpaceOrUpperCase(field.type.schemaName) ? `"${field.type.schemaName}".` : `${field.type.schemaName}.`;
        const typeName = hasWhiteSpaceOrUpperCase(field.type.type_name) ? `"${field.type.type_name}"` : field.type.type_name;
        let typeWithSchema = `${schemaName}${typeName}`;
        const typeAsEnum = `"${field.type.schemaName}"."${field.type.type_name}"`;
        if (!enumSet.has(typeAsEnum) && !hasWhiteSpace(typeAsEnum)) typeWithSchema = typeWithSchema.replaceAll('"', '');
        line = `"${field.name}" ${typeWithSchema}`;
      }

      if (field.unique) {
        line += ' UNIQUE';
      }
      if (field.pk) {
        line += ' PRIMARY KEY';
      }
      if (field.not_null) {
        line += ' NOT NULL';
      }
      if (field.checkIds && field.checkIds.length > 0) {
        if (field.checkIds.length === 1) {
          const check = model.checks[field.checkIds[0]];
          if (check.name) {
            line += ` CONSTRAINT "${check.name}"`;
          }
          line += ` CHECK (${check.expression})`;
        } else {
          const checkExpressions = field.checkIds.map(checkId => {
            const check = model.checks[checkId];
            return `(${check.expression})`;
          });
          line += ` CHECK (${checkExpressions.join(' AND ')})`;
        }
      }
      if (field.dbdefault) {
        if (field.dbdefault.type === 'expression') {
          line += ` DEFAULT (${field.dbdefault.value})`;
        } else if (field.dbdefault.type === 'string') {
          line += ` DEFAULT '${field.dbdefault.value}'`;
        } else {
          line += ` DEFAULT ${field.dbdefault.value}`;
        }
      }

      return line;
    });

    return lines;
  }

  static getCompositePKs (tableId, model) {
    const table = model.tables[tableId];

    const compositePkIds = table.indexIds ? table.indexIds.filter(indexId => model.indexes[indexId].pk) : [];
    const lines = compositePkIds.map((keyId) => {
      const key = model.indexes[keyId];
      let line = 'PRIMARY KEY';
      const columnArr = [];

      key.columnIds.forEach((columnId) => {
        const column = model.indexColumns[columnId];
        let columnStr = '';
        if (column.type === 'expression') {
          columnStr = `(${column.value})`;
        } else {
          columnStr = `"${column.value}"`;
        }
        columnArr.push(columnStr);
      });

      line += ` (${columnArr.join(', ')})`;

      return line;
    });

    return lines;
  }

  static getCheckLines (tableId, model) {
    const table = model.tables[tableId];

    if (!table.checkIds || table.checkIds.length === 0) {
      return [];
    }

    const lines = table.checkIds.map((checkId) => {
      const check = model.checks[checkId];
      let line = '';

      if (check.name) {
        line = `CONSTRAINT "${check.name}" `;
      }

      line += `CHECK (${check.expression})`;

      return line;
    });

    return lines;
  }

  static getTableContentArr (tableIds, model, enumSet) {
    const tableContentArr = tableIds.map((tableId) => {
      const fieldContents = DuckDBExporter.getFieldLines(tableId, model, enumSet);
      const checkContents = DuckDBExporter.getCheckLines(tableId, model);
      const compositePKs = DuckDBExporter.getCompositePKs(tableId, model);

      return {
        tableId,
        fieldContents,
        checkContents,
        compositePKs,
      };
    });

    return tableContentArr;
  }

  static exportTables (tableIds, model, enumSet) {
    const tableContentArr = DuckDBExporter.getTableContentArr(tableIds, model, enumSet);

    const tableStrs = tableContentArr.map((tableContent) => {
      const content = [...tableContent.fieldContents, ...tableContent.checkContents, ...tableContent.compositePKs];
      const table = model.tables[tableContent.tableId];
      const schema = model.schemas[table.schemaId];
      const tableStr = `CREATE TABLE ${shouldPrintSchema(schema, model)
        ? `"${schema.name}".` : ''}"${table.name}" (\n${content.map(line => `  ${line}`).join(',\n')}\n);\n`;
      return tableStr;
    });

    return tableStrs;
  }

  static buildFieldName (fieldIds, model) {
    const fieldNames = fieldIds.map(fieldId => `"${model.fields[fieldId].name}"`).join(', ');
    return `(${fieldNames})`;
  }

  static buildTableManyToMany (firstTableFieldsMap, secondTableFieldsMap, tableName, refEndpointSchema, model) {
    let line = `CREATE TABLE ${shouldPrintSchema(refEndpointSchema, model)
      ? `"${refEndpointSchema.name}".` : ''}"${tableName}" (\n`;
    const key1s = [...firstTableFieldsMap.keys()].join('", "');
    const key2s = [...secondTableFieldsMap.keys()].join('", "');
    firstTableFieldsMap.forEach((fieldType, fieldName) => {
      line += `  "${fieldName}" ${fieldType},\n`;
    });
    secondTableFieldsMap.forEach((fieldType, fieldName) => {
      line += `  "${fieldName}" ${fieldType},\n`;
    });
    line += `  PRIMARY KEY ("${key1s}", "${key2s}")\n`;
    line += ');\n\n';
    return line;
  }

  static buildForeignKeyManyToMany (fieldsMap, foreignEndpointFields, refEndpointTableName, foreignEndpointTableName, refEndpointSchema, foreignEndpointSchema, model) {
    const refEndpointFields = [...fieldsMap.keys()].join('", "');
    const line = `ALTER TABLE ${shouldPrintSchema(refEndpointSchema, model)
      ? `"${refEndpointSchema.name}".` : ''}"${refEndpointTableName}" ADD FOREIGN KEY ("${refEndpointFields}") REFERENCES ${shouldPrintSchema(foreignEndpointSchema, model)
      ? `"${foreignEndpointSchema.name}".` : ''}"${foreignEndpointTableName}" ${foreignEndpointFields};\n\n`;
    return line;
  }

  static exportRefs (refIds, model, usedTableNames) {
    const strArr = refIds.map((refId) => {
      let line = '';
      const ref = model.refs[refId];
      const refOneIndex = ref.endpointIds.findIndex(endpointId => model.endpoints[endpointId].relation === '1');
      const refEndpointIndex = refOneIndex === -1 ? 0 : refOneIndex;
      const foreignEndpointId = ref.endpointIds[1 - refEndpointIndex];
      const refEndpointId = ref.endpointIds[refEndpointIndex];
      const foreignEndpoint = model.endpoints[foreignEndpointId];
      const refEndpoint = model.endpoints[refEndpointId];

      const refEndpointField = model.fields[refEndpoint.fieldIds[0]];
      const refEndpointTable = model.tables[refEndpointField.tableId];
      const refEndpointSchema = model.schemas[refEndpointTable.schemaId];
      const refEndpointFieldName = this.buildFieldName(refEndpoint.fieldIds, model, 'duckdb');

      const foreignEndpointField = model.fields[foreignEndpoint.fieldIds[0]];
      const foreignEndpointTable = model.tables[foreignEndpointField.tableId];
      const foreignEndpointSchema = model.schemas[foreignEndpointTable.schemaId];
      const foreignEndpointFieldName = this.buildFieldName(foreignEndpoint.fieldIds, model, 'duckdb');

      if (refOneIndex === -1) { // many to many relationship
        const firstTableFieldsMap = buildJunctionFields1(refEndpoint.fieldIds, model);
        const secondTableFieldsMap = buildJunctionFields2(foreignEndpoint.fieldIds, model, firstTableFieldsMap);

        const newTableName = buildNewTableName(refEndpointTable.name, foreignEndpointTable.name, usedTableNames);
        line += this.buildTableManyToMany(firstTableFieldsMap, secondTableFieldsMap, newTableName, refEndpointSchema, model);

        line += this.buildForeignKeyManyToMany(firstTableFieldsMap, refEndpointFieldName, newTableName, refEndpointTable.name, refEndpointSchema, refEndpointSchema, model);
        line += this.buildForeignKeyManyToMany(secondTableFieldsMap, foreignEndpointFieldName, newTableName, foreignEndpointTable.name, refEndpointSchema, foreignEndpointSchema, model);
      } else {
        line = `ALTER TABLE ${shouldPrintSchema(foreignEndpointSchema, model)
          ? `"${foreignEndpointSchema.name}".` : ''}"${foreignEndpointTable.name}" ADD `;
        if (ref.name) { line += `CONSTRAINT "${ref.name}" `; }
        line += `FOREIGN KEY ${foreignEndpointFieldName} REFERENCES ${shouldPrintSchema(refEndpointSchema, model)
          ? `"${refEndpointSchema.name}".` : ''}"${refEndpointTable.name}" ${refEndpointFieldName}`;
        if (ref.onDelete) {
          line += ` ON DELETE ${ref.onDelete.toUpperCase()}`;
        }
        if (ref.onUpdate) {
          line += ` ON UPDATE ${ref.onUpdate.toUpperCase()}`;
        }
        line += ';\n';
      }
      return line;
    });

    return strArr;
  }

  static exportIndexes (indexIds, model) {
    // exclude composite pk index
    const indexArr = indexIds.filter((indexId) => !model.indexes[indexId].pk).map((indexId) => {
      const index = model.indexes[indexId];
      const table = model.tables[index.tableId];
      const schema = model.schemas[table.schemaId];

      let line = 'CREATE';
      if (index.unique) {
        line += ' UNIQUE';
      }
      const indexName = index.name ? `"${index.name}"` : '';
      line += ' INDEX';
      if (indexName) {
        line += ` ${indexName}`;
      }
      line += ` ON ${shouldPrintSchema(schema, model)
        ? `"${schema.name}".` : ''}"${table.name}"`;

      // DuckDB supports index types like BTREE, HASH, etc.
      if (index.type) {
        line += ` USING ${index.type.toUpperCase()}`;
      }

      const columnArr = [];
      index.columnIds.forEach((columnId) => {
        const column = model.indexColumns[columnId];
        let columnStr = '';
        if (column.type === 'expression') {
          columnStr = `(${column.value})`;
        } else {
          columnStr = `"${column.value}"`;
        }
        columnArr.push(columnStr);
      });

      line += ` (${columnArr.join(', ')})`;
      line += ';\n';

      return line;
    });

    return indexArr;
  }

  static exportComments (comments, model) {
    const commentArr = comments.map((comment) => {
      let line = 'COMMENT ON';
      const table = model.tables[comment.tableId];
      const schema = model.schemas[table.schemaId];
      switch (comment.type) {
        case 'table': {
          line += ` TABLE ${shouldPrintSchema(schema, model)
            ? `"${schema.name}".` : ''}"${table.name}" IS '${table.note.replace(/'/g, "''")}'`;
          break;
        }
        case 'column': {
          const field = model.fields[comment.fieldId];
          line += ` COLUMN ${shouldPrintSchema(schema, model)
            ? `"${schema.name}".` : ''}"${table.name}"."${field.name}" IS '${field.note.replace(/'/g, "''")}'`;
          break;
        }
        default:
          break;
      }

      line += ';\n';

      return line;
    });

    return commentArr;
  }

  static export (model) {
    const database = model.database['1'];

    const usedTableNames = new Set(Object.values(model.tables).map(table => table.name));

    // Pre-collect all user-defined enum names to distinguish them from built-in DuckDB types
    const enumSet = new Set();

    const schemaEnumStatements = database.schemaIds.reduce((prevStatements, schemaId) => {
      const schema = model.schemas[schemaId];
      const { enumIds } = schema;

      if (shouldPrintSchema(schema, model)) {
        prevStatements.schemas.push(`CREATE SCHEMA "${schema.name}";\n`);
      }

      if (!_.isEmpty(enumIds)) {
        const enumPairs = DuckDBExporter.exportEnums(enumIds, model);

        enumPairs.forEach((enumPair) => {
          const [enumName, enumLine] = enumPair;
          prevStatements.enums.push(enumLine);
          enumSet.add(enumName);
        });
      }

      return prevStatements;
    }, {
      schemas: [],
      enums: [],
      tables: [],
      indexes: [],
      comments: [],
      refs: [],
    });

    const statements = database.schemaIds.reduce((prevStatements, schemaId) => {
      const schema = model.schemas[schemaId];
      const { tableIds, refIds } = schema;

      if (!_.isEmpty(tableIds)) {
        prevStatements.tables.push(...DuckDBExporter.exportTables(tableIds, model, enumSet));
      }

      const indexIds = _.flatten(tableIds.map((tableId) => model.tables[tableId].indexIds));
      if (!_.isEmpty(indexIds)) {
        prevStatements.indexes.push(...DuckDBExporter.exportIndexes(indexIds, model));
      }

      const commentNodes = _.flatten(tableIds.map((tableId) => {
        const { fieldIds, note } = model.tables[tableId];
        const fieldObjects = fieldIds
          .filter((fieldId) => model.fields[fieldId].note)
          .map((fieldId) => ({ type: 'column', fieldId, tableId }));
        return note ? [{ type: 'table', tableId }].concat(fieldObjects) : fieldObjects;
      }));
      if (!_.isEmpty(commentNodes)) {
        prevStatements.comments.push(...DuckDBExporter.exportComments(commentNodes, model));
      }

      if (!_.isEmpty(refIds)) {
        prevStatements.refs.push(...DuckDBExporter.exportRefs(refIds, model, usedTableNames));
      }

      return prevStatements;
    }, schemaEnumStatements);

    // Export transforms as views
    let transformStatements = '';
    if (database.transforms && database.transforms.length > 0) {
      transformStatements = '\n' + TransformExporter.exportTransforms(database.transforms, 'duckdb');
    }

    const res = _.concat(
      statements.schemas,
      statements.enums,
      statements.tables,
      statements.indexes,
      statements.comments,
      statements.refs,
    ).join('\n') + transformStatements;
    return res;
  }
}

export default DuckDBExporter;
