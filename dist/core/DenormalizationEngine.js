"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DenormalizationEngine = void 0;
const stream_1 = require("stream");
class DenormalizationEngine {
    constructor(config = {}) {
        this.config = config;
    }
    createDenormalizer(strategy = this.config.strategy ?? 'hybrid') {
        switch (strategy) {
            case 'flat':
                return this.createFlatTransform();
            case 'nested':
                return this.createNestedTransform();
            default:
                return this.createHybridTransform();
        }
    }
    createFlatTransform() {
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                // For flat, strip relationship objects to foreign-key ids only.
                if (record.__rel) {
                    for (const key of Object.keys(record.__rel)) {
                        const rel = record.__rel[key];
                        if (rel && typeof rel === 'object' && 'id' in rel) {
                            record[key] = rel.id;
                        }
                    }
                    delete record.__rel;
                }
                cb(null, record);
            }
        });
    }
    createNestedTransform() {
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                const nested = {
                    id: record.id ?? record.ID ?? record.Id,
                    data: { ...record },
                    relationships: {
                        children: record.children ?? [],
                        parents: record.parents ?? [],
                        associations: record.associations ?? []
                    }
                };
                cb(null, nested);
            }
        });
    }
    createHybridTransform() {
        // Hybrid keeps FK ids but nests many-to-many associations.
        const cfg = this.config;
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                if (record.__rel && record.__rel.__m2m) {
                    // Keep M2M array nested
                    record.associations = record.__rel.__m2m;
                }
                // Inline one-to-one child objects if present and config enabled
                if (cfg.embedOneToOne && record.children) {
                    record.children = record.children; // placeholder – would need child fetch
                }
                // Optionally embed small one-to-many arrays directly
                if (cfg.embedSmallArrays && Array.isArray(record.children)) {
                    const limit = cfg.smallArrayLimit ?? 5;
                    if (record.children.length <= limit) {
                        record.embeddedChildren = record.children;
                        delete record.children;
                    }
                }
                cb(null, record);
            }
        });
    }
}
exports.DenormalizationEngine = DenormalizationEngine;
