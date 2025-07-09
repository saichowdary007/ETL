import { RelationshipInfo, ExtractionPlan } from './SchemaDetectionEngine';

export interface OutputPreferences {
  /** When true, prefer fewer, larger files */
  consolidate?: boolean;
  /** Desired denormalization strategy */
  denormalization?: 'flat' | 'nested' | 'hybrid';
}

export interface OptimizedStructure {
  entities: Record<string, string>; // table -> file path
  relationships: RelationshipInfo[];
  denormalization: 'flat' | 'nested' | 'hybrid';
  indexingSuggestions: Record<string, string[]>; // table -> suggested indexes
}

/**
 * Generates a best-guess NDJSON output layout given schema analysis and user preferences.
 */
export class OutputStructureOptimizer {
  generateOptimalStructure(plan: ExtractionPlan, prefs: OutputPreferences = {}): OptimizedStructure {
    const consolidate = prefs.consolidate ?? false;
    const denorm = prefs.denormalization ?? 'hybrid';

    // Decide entity files
    const entities: Record<string, string> = {};
    if (consolidate) {
      const consolidatedPath = 'output/data.ndjson';
      plan.entities.forEach((e) => (entities[e.table] = consolidatedPath));
    } else {
      plan.entities.forEach((e) => (entities[e.table] = `output/${e.table}.ndjson`));
    }

    const indexingSuggestions: Record<string, string[]> = {};
    plan.entities.forEach((e) => {
      const fks = plan.relationships.filter((r) => r.table === e.table);
      indexingSuggestions[e.table] = fks.map((fk) => fk.column);
    });

    return {
      entities,
      relationships: plan.relationships,
      denormalization: denorm,
      indexingSuggestions
    };
  }
} 