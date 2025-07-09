import { Transform, Readable } from 'stream';

export interface ValidationResult {
  isValid: boolean;
  errors?: string[];
}

export interface TransformationRules {
  validators?: Record<string, (value: any) => boolean>;
  cleaners?: Record<string, (value: any) => any>;
  transformers?: Record<string, (value: any) => any>;
  enrichers?: ((record: any) => any)[];
  formatters?: Record<string, (value: any) => any>;
}

export class MultiLevelTransformationPipeline {
  private rules: TransformationRules = {};
  
  constructor(private globalRules: TransformationRules = {}, private tableOverrides: Record<string, TransformationRules> = {}) {}

  private mergedRules(table?: string): TransformationRules {
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

  createPipeline(inputStream: Readable, table?: string): Readable {
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

  private createValidationStage() {
    const { validators = {} } = this.rules;
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
        const errors: string[] = [];
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

  private createCleansingStage() {
    const { cleaners = {} } = this.rules;
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
        for (const key of Object.keys(cleaners)) {
          record[key] = cleaners[key](record[key]);
        }
        cb(null, record);
      }
    });
  }

  private createTransformationStage() {
    const { transformers = {} } = this.rules;
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
        for (const key of Object.keys(transformers)) {
          record[key] = transformers[key](record[key]);
        }
        cb(null, record);
      }
    });
  }

  private createEnrichmentStage() {
    const { enrichers = [] } = this.rules;
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
        let enriched = record;
        for (const fn of enrichers) {
          enriched = fn(enriched);
        }
        cb(null, enriched);
      }
    });
  }

  private createOutputFormattingStage() {
    const { formatters = {} } = this.rules;
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
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