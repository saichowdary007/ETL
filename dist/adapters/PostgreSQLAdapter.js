"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PostgreSQLAdapter = void 0;
const pg_1 = require("pg");
class PostgreSQLAdapter {
    async connect(config) {
        this.client = new pg_1.Client(config);
        await this.client.connect();
        // Set statement timeout once during connection
        await this.client.query('SET statement_timeout = 5000');
        return this;
    }
    createQueryStream(query, params = []) {
        // pg-query-stream would be ideal; to avoid extra dependency we use simple cursor
        const QueryStream = require('pg-query-stream');
        const qs = new QueryStream(query, params);
        return this.client.query(qs);
    }
    async getSchema() {
        const res = await this.client.query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`);
        return res.rows;
    }
    async getTables() {
        const res = await this.client.query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`);
        return res.rows.map((r) => r.table_name);
    }
    async getPrimaryKeys() {
        const res = await this.client.query(`SELECT tc.table_name, kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'`);
        return res.rows.map((r) => ({ table: r.table_name, column: r.column_name }));
    }
    async getForeignKeys() {
        const res = await this.client.query(`SELECT kcu.table_name, kcu.column_name, ccu.table_name AS referenced_table_name, ccu.column_name AS referenced_column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
       JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
       WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'`);
        return res.rows.map((r) => ({
            table: r.table_name,
            column: r.column_name,
            referencedTable: r.referenced_table_name,
            referencedColumn: r.referenced_column_name
        }));
    }
    optimizeQuery(query) {
        // Statement timeout is now set during connection
        return query;
    }
    handleSpecialTypes(record) {
        for (const key of Object.keys(record)) {
            const val = record[key];
            if (val instanceof Date) {
                record[key] = val.toISOString();
            }
            else if (typeof val === 'bigint') {
                record[key] = val.toString();
            }
            else if (val && val.constructor && val.constructor.name === 'Buffer') {
                record[key] = val.toString('base64');
            }
            else if (val && typeof val === 'object') {
                // Likely JSONB – keep as-is, but ensure no circular refs.
                record[key] = JSON.parse(JSON.stringify(val));
            }
            else if (typeof val === 'string' && /^POINT\(/.test(val)) {
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
exports.PostgreSQLAdapter = PostgreSQLAdapter;
