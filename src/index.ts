#!/usr/bin/env node
import { MySQLAdapter } from './adapters/MySQLAdapter';
import { PostgreSQLAdapter } from './adapters/PostgreSQLAdapter';
import { MSSQLAdapter } from './adapters/MSSQLAdapter';
import { OracleAdapter } from './adapters/OracleAdapter';
import { MasterController } from './master/MasterController';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

(async () => {
  const argv = await yargs(hideBin(process.argv))
    .option('type', { choices: ['mysql', 'postgresql', 'mssql', 'oracle'] as const, demandOption: true })
    .option('host', { type: 'string', demandOption: true })
    .option('user', { type: 'string', demandOption: true })
    .option('password', { type: 'string', demandOption: true })
    .option('database', { type: 'string', demandOption: true })
    .option('flush-records', { type: 'number', default: parseInt(process.env.FLUSH_RECORDS || '1000') })
    .option('flush-interval', { type: 'number', default: parseInt(process.env.FLUSH_INTERVAL || '30000') })
    .parse();

  let adapter;
  if (argv.type === 'mysql') {
    adapter = await new MySQLAdapter().connect({
      host: argv.host,
      user: argv.user,
      password: argv.password,
      database: argv.database
    });
  } else if (argv.type === 'postgresql') {
    adapter = await new PostgreSQLAdapter().connect({
      host: argv.host,
      user: argv.user,
      password: argv.password,
      database: argv.database
    });
  } else if (argv.type === 'mssql') {
    adapter = await new MSSQLAdapter().connect({
      server: argv.host,
      user: argv.user,
      password: argv.password,
      database: argv.database
    });
  } else {
    adapter = await new OracleAdapter().connect({
      connectString: argv.host,
      user: argv.user,
      password: argv.password
    });
  }

  const controller = new MasterController({ adapter, flushRecordInterval: argv['flush-records'], flushTimeIntervalMs: argv['flush-interval'] });
  await controller.execute();
})(); 