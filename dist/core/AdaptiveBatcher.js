"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AdaptiveBatcher = void 0;
const stream_1 = require("stream");
class AdaptiveBatcher {
    /**
     * Creates a Transform stream that groups JSON-encoded records into NDJSON batches.
     * Batch size dynamically shrinks when relationship complexity or memory pressure increases.
     */
    createBatcher(batchSizeOrOptions = 1000) {
        const opts = typeof batchSizeOrOptions === 'number' ? { baseBatchSize: batchSizeOrOptions } : batchSizeOrOptions;
        let baseBatchSize = opts.baseBatchSize ?? 1000;
        let complexity = opts.complexityFactor ?? 1;
        const memoryPressureFn = opts.memoryPressureProvider ?? defaultMemoryPressure;
        const siblingField = opts.parentField ?? 'parent_id';
        function calcEffectiveSize() {
            const memPressure = memoryPressureFn(); // 0-1
            const adjusted = baseBatchSize / complexity;
            // if memory pressure high (>0.8) cut batch size by half
            const memoryAdjusted = memPressure > 0.8 ? adjusted / 2 : adjusted;
            return Math.max(100, Math.floor(memoryAdjusted));
        }
        let effectiveSize = calcEffectiveSize();
        let buffer = [];
        const onMetrics = opts.onMetrics;
        const flushInterval = opts.flushIntervalMs ?? 5000;
        let flushTimer;
        const metrics = {
            batches: 0,
            records: 0
        };
        function shouldKeepTogether(record, currentBatch) {
            if (!currentBatch.length)
                return true;
            // heuristic: keep siblings (same parent) in same batch
            if (record && typeof record === 'object' && siblingField in record) {
                const first = JSON.parse(currentBatch[0]);
                return first[siblingField] === record[siblingField];
            }
            return false;
        }
        function flushBatch(done) {
            if (buffer.length) {
                this.push(buffer.join(''));
                metrics.batches++;
                buffer = [];
                if (onMetrics)
                    onMetrics({ ...metrics });
            }
            if (done)
                done();
        }
        function startNewBatch(recordStr) {
            buffer.push(recordStr);
        }
        const transformStream = new stream_1.Transform({
            readableObjectMode: false,
            writableObjectMode: true,
            transform(record, _enc, cb) {
                // recompute occasionally to adapt to runtime conditions
                if (buffer.length === 0) {
                    effectiveSize = calcEffectiveSize();
                }
                const recordStr = JSON.stringify(record) + '\n';
                metrics.records++;
                if (!shouldKeepTogether(record, buffer)) {
                    flushBatch.call(this);
                }
                buffer.push(recordStr);
                if (buffer.length >= effectiveSize) {
                    flushBatch.call(this);
                }
                cb();
            },
            flush(cb) {
                flushBatch.call(this, cb);
                this.emit('batchMetrics', metrics);
            },
            final(cb) {
                if (flushTimer)
                    clearInterval(flushTimer);
                cb();
            }
        });
        // start periodic flush timer
        if (flushInterval > 0) {
            flushTimer = setInterval(() => {
                flushBatch.call(transformStream);
            }, flushInterval);
        }
        return transformStream;
    }
}
exports.AdaptiveBatcher = AdaptiveBatcher;
function defaultMemoryPressure() {
    const mem = process.memoryUsage();
    const rss = mem.rss;
    // Rough heuristic: assume 1.5GB safe RSS on typical node proc
    const pressure = rss / (1.5 * 1024 * 1024 * 1024);
    return Math.min(1, pressure);
}
