"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiDatabaseSupportEngine = void 0;
const MySQLAdapter_1 = require("../adapters/MySQLAdapter");
const PostgreSQLAdapter_1 = require("../adapters/PostgreSQLAdapter");
const MSSQLAdapter_1 = require("../adapters/MSSQLAdapter");
const OracleAdapter_1 = require("../adapters/OracleAdapter");
class MultiDatabaseSupportEngine {
    constructor() {
        this.adapters = {
            mysql: () => new MySQLAdapter_1.MySQLAdapter(),
            postgresql: () => new PostgreSQLAdapter_1.PostgreSQLAdapter(),
            mssql: () => new MSSQLAdapter_1.MSSQLAdapter(),
            oracle: () => new OracleAdapter_1.OracleAdapter()
        };
    }
    async createConnection(config) {
        const factory = this.adapters[config.type];
        if (!factory) {
            throw new Error(`Unsupported database type ${config.type}`);
        }
        const adapter = factory();
        return adapter.connect(config);
    }
}
exports.MultiDatabaseSupportEngine = MultiDatabaseSupportEngine;
