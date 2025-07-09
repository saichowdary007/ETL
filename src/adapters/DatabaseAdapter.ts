export interface DatabaseAdapter {
  connect(config: any): Promise<this>;
  createQueryStream(query: string, params?: any[]): NodeJS.ReadableStream;
  getSchema(): Promise<any>;
  getTables(): Promise<string[]>;
  getPrimaryKeys(): Promise<{ table: string; column: string }[]>;
  getForeignKeys(): Promise<{ table: string; column: string; referencedTable: string; referencedColumn: string }[]>;
  getIndexes?(): Promise<any[]>;
  getUniqueConstraints?(): Promise<{ table: string; column: string }[]>;
  getCheckConstraints?(): Promise<{ table: string; expression: string }[]>;
  getColumnDefaults?(): Promise<{ table: string; column: string; default: any }[]>;
  getRowCounts?(): Promise<{ table: string; rows: number }[]>;

  /** Adjust query for adapter-specific optimisation (e.g., hints, limit clauses) */
  optimizeQuery?(query: string, relationshipMap?: any): string;

  /** Convert adapter-specific types (Buffer, Date, BigInt) into JSON-friendly */
  handleSpecialTypes?(record: any): any;
} 