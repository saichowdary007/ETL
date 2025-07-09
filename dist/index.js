#!/usr/bin/env node
"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const MySQLAdapter_1 = require("./adapters/MySQLAdapter");
const PostgreSQLAdapter_1 = require("./adapters/PostgreSQLAdapter");
const MSSQLAdapter_1 = require("./adapters/MSSQLAdapter");
const OracleAdapter_1 = require("./adapters/OracleAdapter");
const MasterController_1 = require("./master/MasterController");
const yargs_1 = __importDefault(require("yargs"));
const helpers_1 = require("yargs/helpers");
(async () => {
    const argv = await (0, yargs_1.default)((0, helpers_1.hideBin)(process.argv))
        .option('type', { choices: ['mysql', 'postgresql', 'mssql', 'oracle'], demandOption: true })
        .option('host', { type: 'string', demandOption: true })
        .option('user', { type: 'string', demandOption: true })
        .option('password', { type: 'string', demandOption: true })
        .option('database', { type: 'string', demandOption: true })
        .option('flush-records', { type: 'number', default: parseInt(process.env.FLUSH_RECORDS || '1000') })
        .option('flush-interval', { type: 'number', default: parseInt(process.env.FLUSH_INTERVAL || '30000') })
        .parse();
    let adapter;
    if (argv.type === 'mysql') {
        adapter = await new MySQLAdapter_1.MySQLAdapter().connect({
            host: argv.host,
            user: argv.user,
            password: argv.password,
            database: argv.database
        });
    }
    else if (argv.type === 'postgresql') {
        adapter = await new PostgreSQLAdapter_1.PostgreSQLAdapter().connect({
            host: argv.host,
            user: argv.user,
            password: argv.password,
            database: argv.database
        });
    }
    else if (argv.type === 'mssql') {
        adapter = await new MSSQLAdapter_1.MSSQLAdapter().connect({
            server: argv.host,
            user: argv.user,
            password: argv.password,
            database: argv.database
        });
    }
    else {
        adapter = await new OracleAdapter_1.OracleAdapter().connect({
            connectString: argv.host,
            user: argv.user,
            password: argv.password
        });
    }
    const controller = new MasterController_1.MasterController({ adapter, flushRecordInterval: argv['flush-records'], flushTimeIntervalMs: argv['flush-interval'] });
    await controller.execute();
})();
