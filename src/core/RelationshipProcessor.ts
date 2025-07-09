import { Transform } from 'stream';

import { RelationshipInfo } from './SchemaDetectionEngine';

interface ProcessorOptions {
  /** When true, records that fail FK integrity validation will be dropped */
  strict?: boolean;
}

interface IntegrityStats {
  processed: number;
  invalid: number;
}

export class RelationshipProcessor {
  private tracker = new Map<string, Set<any>>();

  private stats: IntegrityStats = { processed: 0, invalid: 0 };

  getStats() {
    return { ...this.stats };
  }

  /** Utility to track PKs for a table */
  private track(pk: any, table: string) {
    if (!this.tracker.has(table)) this.tracker.set(table, new Set());
    this.tracker.get(table)!.add(pk);
  }

  /** Checks existence of referenced id in previously tracked PKs */
  private isValidReference(table: string, id: any) {
    return this.tracker.get(table)?.has(id);
  }

  /**
   * Creates a Transform stream that enriches records with relationship metadata and validates integrity.
   * @param table The table currently being processed
   * @param relationships Full relationship list from the schema detector
   */
  createProcessor(table: string, relationships: RelationshipInfo[], options: ProcessorOptions = {}) {
    const { strict = false } = options;

    // FK relationships where current table is child (belongsTo)
    const outgoing = relationships.filter((r) => (r.type === 'one-to-many' || r.type === 'one-to-one') && r.table === table);

    // many-to-many where current table appears on either side
    const m2m = relationships.filter((r) => r.type === 'many-to-many' && (r.table === table || r.referencedTable === table));

    const self = this;

    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
        const relMeta: Record<string, any> = {};

        // Track PK for integrity (assuming 'id' or pk property exists)
        const pkValue = record.id ?? record.ID ?? record.Id;
        if (pkValue !== undefined) {
          self.track(pkValue, table);
        }

        // Validate and annotate FKs
        for (const rel of outgoing) {
          const value = record[rel.column];
          if (value !== undefined && value !== null) {
            // strict validation
            if (strict && !self.isValidReference(rel.referencedTable, value)) {
              self.stats.invalid++;
              return cb(); // drop invalid record
            }
            relMeta[rel.column] = { table: rel.referencedTable, id: value };
          }
        }

        // For many-to-many, just annotate table names for downstream handler
        if (m2m.length) {
          relMeta.__m2m = m2m.map((rel) => ({ through: rel.junctionTable, target: rel.table === table ? rel.referencedTable : rel.table }));
        }

        if (Object.keys(relMeta).length) {
          record.__rel = relMeta;
        }

        self.stats.processed++;
        cb(null, record);
      },
      flush(cb) {
        // emit stats at the end
        (this as any).emit('integrity', self.getStats());
        cb();
      }
    });
  }
} 