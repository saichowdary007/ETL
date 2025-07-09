import { Client, QueryConfig } from 'pg';
import { Readable } from 'stream';
import { DatabaseAdapter } from './DatabaseAdapter';

export class PostgreSQLAdapter implements DatabaseAdapter {
  private client!: Client;

  async connect(config: any): Promise<this> {
    this.client = new Client(config);
    await this.client.connect();
    // Set statement timeout once during connection
    await this.client.query('SET statement_timeout = 5000');
    return this;
  }

  createQueryStream(query: string, params: any[] = []): NodeJS.ReadableStream {
    // pg-query-stream would be ideal; to avoid extra dependency we use simple cursor
    const QueryStream = require('pg-query-stream');
    const qs = new QueryStream(query, params);
    return this.client.query(qs);
  }

  async getSchema(): Promise<any> {
    const res = await this.client.query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`);
    return res.rows;
  }

  async getTables(): Promise<string[]> {
    const res = await this.client.query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`);
    return res.rows.map((r: any) => r.table_name as string);
  }

  async getPrimaryKeys(): Promise<{ table: string; column: string }[]> {
    const res = await this.client.query(
      `SELECT tc.table_name, kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'`
    );
    return res.rows.map((r: any) => ({ table: r.table_name, column: r.column_name }));
  }

  async getForeignKeys(): Promise<{ table: string; column: string; referencedTable: string; referencedColumn: string }[]> {
    const res = await this.client.query(
      `SELECT kcu.table_name, kcu.column_name, ccu.table_name AS referenced_table_name, ccu.column_name AS referenced_column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
       JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
       WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'`
    );
    return res.rows.map((r: any) => ({
      table: r.table_name,
      column: r.column_name,
      referencedTable: r.referenced_table_name,
      referencedColumn: r.referenced_column_name
    }));
  }

  optimizeQuery(query: string): string {
    // Statement timeout is now set during connection
    return query;
  }

  handleSpecialTypes(record: any) {
    for (const key of Object.keys(record)) {
      const val = record[key];
      if (val instanceof Date) {
        record[key] = val.toISOString();
      } else if (typeof val === 'bigint') {
        record[key] = val.toString();
      } else if (val && val.constructor && val.constructor.name === 'Buffer') {
        record[key] = (val as Buffer).toString('base64');
      } else if (val && typeof val === 'object') {
        // Likely JSONB – keep as-is, but ensure no circular refs.
        record[key] = JSON.parse(JSON.stringify(val));
      } else if (typeof val === 'string' && /^POINT\(/.test(val)) {
        // Simple geometry point -> {x,y}
        const match = val.match(/^POINT\(([-0-9\.]+) ([-0-9\.]+)\)$/);
        if (match) {
          record[key] = { x: parseFloat(match[1]), y: parseFloat(match[2]) };
        }
      }
    }
    return record;
  }
} 