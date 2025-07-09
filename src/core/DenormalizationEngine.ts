import { Transform } from 'stream';

export type DenormStrategy = 'flat' | 'nested' | 'hybrid';

export interface DenormalizationConfig {
  strategy?: DenormStrategy;
  embedOneToOne?: boolean;
  embedSmallArrays?: boolean;
  smallArrayLimit?: number;
}

interface NestedOutput {
  id: any;
  data: any;
  relationships: {
    children: any[];
    parents: any[];
    associations: any[];
  };
}

export class DenormalizationEngine {
  constructor(private config: DenormalizationConfig = {}) {}

  createDenormalizer(strategy: DenormStrategy = this.config.strategy ?? 'hybrid') {
    switch (strategy) {
      case 'flat':
        return this.createFlatTransform();
      case 'nested':
        return this.createNestedTransform();
      default:
        return this.createHybridTransform();
    }
  }

  private createFlatTransform() {
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
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

  private createNestedTransform() {
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
        const nested: NestedOutput = {
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

  private createHybridTransform() {
    // Hybrid keeps FK ids but nests many-to-many associations.
    const cfg = this.config;
    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
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