import mysql, { Pool } from 'mysql2/promise';
import { DatabaseAdapter } from './DatabaseAdapter';

export class MySQLAdapter implements DatabaseAdapter {
  private pool!: Pool;

  async connect(config: mysql.PoolOptions): Promise<this> {
    this.pool = mysql.createPool({ ...config, connectionLimit: config.connectionLimit ?? 10 });
    return this;
  }

  createQueryStream(query: string, params: any[] = []): NodeJS.ReadableStream {
    // The mysql2 query stream returns a Readable
    // @ts-ignore – type definitions for queryStream are not complete
    return this.pool.queryStream({ sql: query, values: params, rowsAsArray: false });
  }

  async getSchema(): Promise<any> {
    const [rows] = await this.pool.query(`SHOW TABLES`);
    return rows;
  }

  async getTables(): Promise<string[]> {
    const [rows] = await this.pool.query(`SHOW TABLES`);
    // rows shape: { 'Tables_in_<db>': 'tableName' }
    return (rows as any[]).map((r) => Object.values(r)[0] as string);
  }

  async getPrimaryKeys(): Promise<{ table: string; column: string }[]> {
    const [rows] = await this.pool.query(
      `SELECT TABLE_NAME as tableName, COLUMN_NAME as columnName
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY'`
    );
    return (rows as any[]).map((r) => ({ table: r.tableName, column: r.columnName }));
  }

  async getForeignKeys(): Promise<{ table: string; column: string; referencedTable: string; referencedColumn: string }[]> {
    const [rows] = await this.pool.query(
      `SELECT TABLE_NAME as tableName, COLUMN_NAME as columnName, REFERENCED_TABLE_NAME as refTable, REFERENCED_COLUMN_NAME as refColumn
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`
    );
    return (rows as any[]).map((r) => ({
      table: r.tableName,
      column: r.columnName,
      referencedTable: r.refTable,
      referencedColumn: r.refColumn
    }));
  }

  optimizeQuery(query: string): string {
    // Simple hint to limit execution time to 5s and leverage index if exists.
    if (!/MAX_EXECUTION_TIME/i.test(query)) {
      return `/*+ MAX_EXECUTION_TIME(5000) */ ${query}`;
    }
    return query;
  }

  handleSpecialTypes(record: any) {
    for (const key of Object.keys(record)) {
      const val = record[key];
      if (Buffer.isBuffer(val)) {
        record[key] = val.toString('base64');
      } else if (val instanceof Date) {
        record[key] = val.toISOString();
      } else if (typeof val === 'bigint') {
        record[key] = val.toString();
      }
    }
    return record;
  }
} 