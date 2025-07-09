import { MySQLAdapter } from '../adapters/MySQLAdapter';
import { PostgreSQLAdapter } from '../adapters/PostgreSQLAdapter';
import { MSSQLAdapter } from '../adapters/MSSQLAdapter';
import { OracleAdapter } from '../adapters/OracleAdapter';
import { DatabaseAdapter } from '../adapters/DatabaseAdapter';

export type DbType = 'mysql' | 'postgresql' | 'mssql' | 'oracle';

export interface DatabaseConfig {
  type: DbType;
  host: string;
  user: string;
  password: string;
  database?: string;
}

export class MultiDatabaseSupportEngine {
  private adapters: Record<DbType, () => DatabaseAdapter> = {
    mysql: () => new MySQLAdapter(),
    postgresql: () => new PostgreSQLAdapter(),
    mssql: () => new MSSQLAdapter(),
    oracle: () => new OracleAdapter()
  } as const;

  async createConnection(config: DatabaseConfig): Promise<DatabaseAdapter> {
    const factory = this.adapters[config.type];
    if (!factory) {
      throw new Error(`Unsupported database type ${config.type}`);
    }
    const adapter = factory();
    return adapter.connect(config as any);
  }
} 