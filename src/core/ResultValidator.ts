export interface ValidationReport {
  table: string;
  sourceRows: number;
  outputRows: number;
  integrityInvalid: number;
  integrityTotal: number;
  passed: boolean;
}

import fs from 'fs-extra';

export class ResultValidator {
  constructor(private outputDir = 'output') {}

  async countOutputLines(filePath: string): Promise<number> {
    let count = 0;
    const stream = fs.createReadStream(filePath);
    for await (const chunk of stream) {
      for (const c of chunk.toString()) if (c === '\n') count++;
    }
    return count;
  }

  async validateTable(table: string, adapter: { getRowCounts?: () => Promise<{ table: string; rows: number }[]> }, integrityStats: { invalid: number; processed: number }): Promise<ValidationReport> {
    const sourceRows = (await adapter.getRowCounts?.())?.find((r) => r.table === table)?.rows ?? 0;
    const outFile = `${this.outputDir}/${table}.ndjson`;
    const outputRows = await this.countOutputLines(outFile).catch(() => 0);
    const passed = sourceRows === outputRows && integrityStats.invalid === 0;
    return {
      table,
      sourceRows,
      outputRows,
      integrityInvalid: integrityStats.invalid,
      integrityTotal: integrityStats.processed,
      passed
    };
  }

  async validateResults(plan: { entities: { table: string }[] }, adapter: any, integrityMap: Map<string, { invalid: number; processed: number }>) {
    const reports: ValidationReport[] = [];
    for (const entity of plan.entities) {
      const stats = integrityMap.get(entity.table) ?? { invalid: 0, processed: 0 };
      reports.push(await this.validateTable(entity.table, adapter, stats));
    }
    const failed = reports.filter((r) => !r.passed);
    if (failed.length) {
      console.error('[Validator] Validation failed for tables', failed.map((f) => f.table));
    } else {
      console.log('[Validator] All tables validated successfully');
    }
    await fs.writeJson('validation_report.json', reports, { spaces: 2 });
  }
} 