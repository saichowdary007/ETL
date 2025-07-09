"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OutputStructureOptimizer = void 0;
/**
 * Generates a best-guess NDJSON output layout given schema analysis and user preferences.
 */
class OutputStructureOptimizer {
    generateOptimalStructure(plan, prefs = {}) {
        const consolidate = prefs.consolidate ?? false;
        const denorm = prefs.denormalization ?? 'hybrid';
        // Decide entity files
        const entities = {};
        if (consolidate) {
            const consolidatedPath = 'output/data.ndjson';
            plan.entities.forEach((e) => (entities[e.table] = consolidatedPath));
        }
        else {
            plan.entities.forEach((e) => (entities[e.table] = `output/${e.table}.ndjson`));
        }
        const indexingSuggestions = {};
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
exports.OutputStructureOptimizer = OutputStructureOptimizer;
