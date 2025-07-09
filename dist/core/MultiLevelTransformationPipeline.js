"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiLevelTransformationPipeline = void 0;
const stream_1 = require("stream");
class MultiLevelTransformationPipeline {
    constructor(globalRules = {}, tableOverrides = {}) {
        this.globalRules = globalRules;
        this.tableOverrides = tableOverrides;
        this.rules = {};
    }
    mergedRules(table) {
        const base = { ...this.globalRules };
        if (table && this.tableOverrides[table]) {
            const override = this.tableOverrides[table];
            return {
                validators: { ...base.validators, ...override.validators },
                cleaners: { ...base.cleaners, ...override.cleaners },
                transformers: { ...base.transformers, ...override.transformers },
                enrichers: [...(base.enrichers || []), ...(override.enrichers || [])],
                formatters: { ...base.formatters, ...override.formatters }
            };
        }
        return base;
    }
    createPipeline(inputStream, table) {
        const active = this.mergedRules(table);
        // Temporarily swap this.rules to active for stage creation
        const prev = this.rules;
        // @ts-ignore
        this.rules = active;
        const stages = [
            this.createValidationStage(),
            this.createCleansingStage(),
            this.createTransformationStage(),
            this.createEnrichmentStage(),
            this.createOutputFormattingStage()
        ];
        // @ts-ignore restore
        this.rules = prev;
        return stages.reduce((stream, stage) => stream.pipe(stage), inputStream);
    }
    createValidationStage() {
        const { validators = {} } = this.rules;
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                const errors = [];
                for (const key of Object.keys(validators)) {
                    const fn = validators[key];
                    if (!fn(record[key])) {
                        errors.push(`Field ${key} failed validation`);
                    }
                }
                if (errors.length) {
                    // Drop invalid
                    return cb();
                }
                cb(null, record);
            }
        });
    }
    createCleansingStage() {
        const { cleaners = {} } = this.rules;
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                for (const key of Object.keys(cleaners)) {
                    record[key] = cleaners[key](record[key]);
                }
                cb(null, record);
            }
        });
    }
    createTransformationStage() {
        const { transformers = {} } = this.rules;
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                for (const key of Object.keys(transformers)) {
                    record[key] = transformers[key](record[key]);
                }
                cb(null, record);
            }
        });
    }
    createEnrichmentStage() {
        const { enrichers = [] } = this.rules;
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                let enriched = record;
                for (const fn of enrichers) {
                    enriched = fn(enriched);
                }
                cb(null, enriched);
            }
        });
    }
    createOutputFormattingStage() {
        const { formatters = {} } = this.rules;
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                for (const key of Object.keys(formatters)) {
                    if (record[key] !== undefined && record[key] !== null) {
                        record[key] = formatters[key](record[key]);
                    }
                }
                cb(null, record);
            }
        });
    }
}
exports.MultiLevelTransformationPipeline = MultiLevelTransformationPipeline;
