import oracledb from 'oracledb';
import { Readable } from 'stream';
import { DatabaseAdapter } from './DatabaseAdapter';

export class OracleAdapter implements DatabaseAdapter {
  private connection!: oracledb.Connection;

  async connect(config: oracledb.ConnectionAttributes): Promise<this> {
    this.connection = await oracledb.getConnection(config);
    return this;
  }

  createQueryStream(query: string, params: any[] = []): NodeJS.ReadableStream {
    return this.connection.queryStream(query, params, { outFormat: oracledb.OUT_FORMAT_OBJECT });
  }

  async getSchema(): Promise<any> {
    const result = await this.connection.execute(`SELECT table_name FROM user_tables`);
    return result.rows || [];
  }

  async getTables(): Promise<string[]> {
    const result = await this.connection.execute(`SELECT table_name FROM user_tables`);
    return (result.rows || []).map((r: any) => r[0]);
  }

  async getPrimaryKeys(): Promise<{ table: string; column: string }[]> {
    const result = await this.connection.execute(
      `SELECT cols.table_name, cols.column_name
       FROM all_constraints cons, all_cons_columns cols
       WHERE cons.constraint_type = 'P'
         AND cons.constraint_name = cols.constraint_name
         AND cons.owner = cols.owner`);
    return (result.rows || []).map((r: any) => ({ table: r[0], column: r[1] }));
  }

  async getForeignKeys(): Promise<{ table: string; column: string; referencedTable: string; referencedColumn: string }[]> {
    const result = await this.connection.execute(
      `SELECT a.table_name, a.column_name, c_pk.table_name r_table_name, b.column_name r_column_name
       FROM user_cons_columns a
       JOIN user_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name
       JOIN user_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name
       JOIN user_cons_columns b ON c_pk.owner = b.owner AND c_pk.constraint_name = b.constraint_name AND a.position = b.position
       WHERE c.constraint_type = 'R'`);
    return (result.rows || []).map((r: any) => ({
      table: r[0],
      column: r[1],
      referencedTable: r[2],
      referencedColumn: r[3]
    }));
  }

  optimizeQuery(query: string): string {
    // Add /*+ PARALLEL */ hint if not present
    if (!/PARALLEL/i.test(query)) {
      return `/*+ PARALLEL */ ${query}`;
    }
    return query;
  }

  handleSpecialTypes(record: any) {
    for (const key of Object.keys(record)) {
      const val = record[key];
      if (val instanceof Date) {
        record[key] = val.toISOString();
      } else if (typeof val === 'bigint') {
        record[key] = val.toString();
      } else if (val && typeof val === 'object' && val.type === 'BUFFER') {
        record[key] = Buffer.from(val).toString('base64');
      }
    }
    return record;
  }
} 