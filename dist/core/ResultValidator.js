"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ResultValidator = void 0;
const fs_extra_1 = __importDefault(require("fs-extra"));
class ResultValidator {
    constructor(outputDir = 'output') {
        this.outputDir = outputDir;
    }
    async countOutputLines(filePath) {
        let count = 0;
        const stream = fs_extra_1.default.createReadStream(filePath);
        for await (const chunk of stream) {
            for (const c of chunk.toString())
                if (c === '\n')
                    count++;
        }
        return count;
    }
    async validateTable(table, adapter, integrityStats) {
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
    async validateResults(plan, adapter, integrityMap) {
        const reports = [];
        for (const entity of plan.entities) {
            const stats = integrityMap.get(entity.table) ?? { invalid: 0, processed: 0 };
            reports.push(await this.validateTable(entity.table, adapter, stats));
        }
        const failed = reports.filter((r) => !r.passed);
        if (failed.length) {
            console.error('[Validator] Validation failed for tables', failed.map((f) => f.table));
        }
        else {
            console.log('[Validator] All tables validated successfully');
        }
        await fs_extra_1.default.writeJson('validation_report.json', reports, { spaces: 2 });
    }
}
exports.ResultValidator = ResultValidator;
