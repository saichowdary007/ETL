"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySQLAdapter = void 0;
const promise_1 = __importDefault(require("mysql2/promise"));
class MySQLAdapter {
    async connect(config) {
        this.pool = promise_1.default.createPool({ ...config, connectionLimit: config.connectionLimit ?? 10 });
        return this;
    }
    createQueryStream(query, params = []) {
        // The mysql2 query stream returns a Readable
        // @ts-ignore – type definitions for queryStream are not complete
        return this.pool.queryStream({ sql: query, values: params, rowsAsArray: false });
    }
    async getSchema() {
        const [rows] = await this.pool.query(`SHOW TABLES`);
        return rows;
    }
    async getTables() {
        const [rows] = await this.pool.query(`SHOW TABLES`);
        // rows shape: { 'Tables_in_<db>': 'tableName' }
        return rows.map((r) => Object.values(r)[0]);
    }
    async getPrimaryKeys() {
        const [rows] = await this.pool.query(`SELECT TABLE_NAME as tableName, COLUMN_NAME as columnName
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY'`);
        return rows.map((r) => ({ table: r.tableName, column: r.columnName }));
    }
    async getForeignKeys() {
        const [rows] = await this.pool.query(`SELECT TABLE_NAME as tableName, COLUMN_NAME as columnName, REFERENCED_TABLE_NAME as refTable, REFERENCED_COLUMN_NAME as refColumn
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`);
        return rows.map((r) => ({
            table: r.tableName,
            column: r.columnName,
            referencedTable: r.refTable,
            referencedColumn: r.refColumn
        }));
    }
    optimizeQuery(query) {
        // Simple hint to limit execution time to 5s and leverage index if exists.
        if (!/MAX_EXECUTION_TIME/i.test(query)) {
            return `/*+ MAX_EXECUTION_TIME(5000) */ ${query}`;
        }
        return query;
    }
    handleSpecialTypes(record) {
        for (const key of Object.keys(record)) {
            const val = record[key];
            if (Buffer.isBuffer(val)) {
                record[key] = val.toString('base64');
            }
            else if (val instanceof Date) {
                record[key] = val.toISOString();
            }
            else if (typeof val === 'bigint') {
                record[key] = val.toString();
            }
        }
        return record;
    }
}
exports.MySQLAdapter = MySQLAdapter;
