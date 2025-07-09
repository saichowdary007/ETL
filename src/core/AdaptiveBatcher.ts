import { Transform } from 'stream';

export interface BatcherOptions {
  /** Base batch size before adjustments */
  baseBatchSize?: number;
  /** Estimated relationship / transformation complexity (1 = trivial, higher = complex) */
  complexityFactor?: number;
  /** Function that returns 0-1 memory pressure ratio; if omitted we derive from RSS/heap limit */
  memoryPressureProvider?: () => number;
  /** Field which links siblings (default parent_id) */
  parentField?: string;
  /** Max time (ms) before batch auto-flush regardless of size */
  flushIntervalMs?: number;
  /** Optional callback to receive batch metrics */
  onMetrics?: (metrics: { batches: number; records: number }) => void;
}

export class AdaptiveBatcher {
  /**
   * Creates a Transform stream that groups JSON-encoded records into NDJSON batches.
   * Batch size dynamically shrinks when relationship complexity or memory pressure increases.
   */
  createBatcher(batchSizeOrOptions: number | BatcherOptions = 1000) {
    const opts: BatcherOptions = typeof batchSizeOrOptions === 'number' ? { baseBatchSize: batchSizeOrOptions } : batchSizeOrOptions;

    let baseBatchSize = opts.baseBatchSize ?? 1000;
    let complexity = opts.complexityFactor ?? 1;
    const memoryPressureFn = opts.memoryPressureProvider ?? defaultMemoryPressure;
    const siblingField = opts.parentField ?? 'parent_id';

    function calcEffectiveSize(): number {
      const memPressure = memoryPressureFn(); // 0-1
      const adjusted = baseBatchSize / complexity;
      // if memory pressure high (>0.8) cut batch size by half
      const memoryAdjusted = memPressure > 0.8 ? adjusted / 2 : adjusted;
      return Math.max(100, Math.floor(memoryAdjusted));
    }

    let effectiveSize = calcEffectiveSize();
    let buffer: string[] = [];
    const onMetrics = opts.onMetrics;
    const flushInterval = opts.flushIntervalMs ?? 5000;
    let flushTimer: NodeJS.Timeout | undefined;

    const metrics = {
      batches: 0,
      records: 0
    };

    function shouldKeepTogether(record: any, currentBatch: string[]): boolean {
      if (!currentBatch.length) return true;
      // heuristic: keep siblings (same parent) in same batch
      if (record && typeof record === 'object' && siblingField in record) {
        const first = JSON.parse(currentBatch[0]);
        return first[siblingField] === record[siblingField];
      }
      return false;
    }

    function flushBatch(this: Transform, done?: () => void) {
      if (buffer.length) {
        this.push(buffer.join(''));
        metrics.batches++;
        buffer = [];
        if (onMetrics) onMetrics({ ...metrics });
      }
      if (done) done();
    }

    function startNewBatch(recordStr: string) {
      buffer.push(recordStr);
    }

    const transformStream = new Transform({
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
        (this as any).emit('batchMetrics', metrics);
      },
      final(cb) {
        if (flushTimer) clearInterval(flushTimer);
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

function defaultMemoryPressure(): number {
  const mem = process.memoryUsage();
  const rss = mem.rss;
  // Rough heuristic: assume 1.5GB safe RSS on typical node proc
  const pressure = rss / (1.5 * 1024 * 1024 * 1024);
  return Math.min(1, pressure);
} 