import sql from 'mssql';
import { Readable } from 'stream';
import { DatabaseAdapter } from './DatabaseAdapter';

export class MSSQLAdapter implements DatabaseAdapter {
  private pool!: sql.ConnectionPool;

  async connect(config: sql.config): Promise<this> {
    this.pool = await sql.connect({ ...config, options: { enableArithAbort: true, ...(config.options || {}) } });
    return this;
  }

  createQueryStream(query: string, params: any[] = []): NodeJS.ReadableStream {
    const request = new sql.Request(this.pool);
    request.stream = true;
    params.forEach((p, i) => request.input(`p${i}`, p));

    const out = new Readable({ objectMode: true, read() {} });
    request.on('row', (row) => out.push(row));
    request.on('error', (err) => out.destroy(err));
    request.on('done', () => out.push(null));
    request.query(query).catch((err) => out.destroy(err));
    return out;
  }

  async getSchema(): Promise<any> {
    const result = await this.pool.request().query(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`);
    return result.recordset;
  }

  async getTables(): Promise<string[]> {
    const result = await this.pool.request().query(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`);
    return result.recordset.map((r: any) => r.TABLE_NAME as string);
  }

  async getPrimaryKeys(): Promise<{ table: string; column: string }[]> {
    const result = await this.pool.request().query(`SELECT KU.TABLE_NAME, KU.COLUMN_NAME
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
      JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
      ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
         TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME`);
    return result.recordset.map((r: any) => ({ table: r.TABLE_NAME, column: r.COLUMN_NAME }));
  }

  async getForeignKeys(): Promise<{ table: string; column: string; referencedTable: string; referencedColumn: string }[]> {
    const result = await this.pool.request().query(`SELECT FK.TABLE_NAME, CU.COLUMN_NAME, PK.TABLE_NAME AS REF_TABLE_NAME, PT.COLUMN_NAME AS REF_COLUMN_NAME
      FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
      INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
      INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
      INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
      INNER JOIN (
        SELECT i1.TABLE_NAME, i2.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
        WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
      ) PT ON PT.TABLE_NAME = PK.TABLE_NAME`);
    return result.recordset.map((r: any) => ({
      table: r.TABLE_NAME,
      column: r.COLUMN_NAME,
      referencedTable: r.REF_TABLE_NAME,
      referencedColumn: r.REF_COLUMN_NAME
    }));
  }

  optimizeQuery(query: string): string {
    // Use OPTION (FAST 100) to get initial rows quickly for streaming
    if (!/OPTION \(FAST/i.test(query)) {
      return `${query} OPTION (FAST 100)`;
    }
    return query;
  }

  handleSpecialTypes(record: any) {
    for (const key of Object.keys(record)) {
      const val = record[key];
      if (val instanceof Date) {
        record[key] = val.toISOString();
      } else if (val && val.constructor && val.constructor.name === 'Buffer') {
        record[key] = (val as Buffer).toString('base64');
      } else if (typeof val === 'bigint') {
        record[key] = val.toString();
      }
    }
    return record;
  }
} 