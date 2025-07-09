"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SchemaDetectionEngine = void 0;
class SchemaDetectionEngine {
    async analyzeDatabase(connection) {
        const tables = await connection.getTables();
        const pks = await connection.getPrimaryKeys();
        const fks = await connection.getForeignKeys();
        const pkMap = new Map();
        pks.forEach((pk) => pkMap.set(pk.table, pk.column));
        // 1. Entities & hierarchy flag
        const entities = tables.map((table) => {
            const selfRef = fks.find((fk) => fk.table === table && fk.referencedTable === table);
            return {
                table,
                nestingLevels: selfRef ? Number.MAX_SAFE_INTEGER : undefined
            };
        });
        // 2. Hierarchies
        const hierarchies = this.detectHierarchies(fks, pkMap);
        // 3. Relationships – includes many-to-many detection
        const relationships = this.mapRelationships(fks, pkMap);
        // 4. Constraint analysis (unique, check, defaults)
        const uniqueKeys = connection.getUniqueConstraints ? await connection.getUniqueConstraints() : [];
        const checkConstraints = connection.getCheckConstraints ? await connection.getCheckConstraints() : [];
        const defaults = connection.getColumnDefaults ? await connection.getColumnDefaults() : [];
        // 5. Index catalogue (best-effort)
        const indexes = connection.getIndexes ? await connection.getIndexes() : [];
        // 6. Row counts / stats
        const stats = connection.getRowCounts ? await connection.getRowCounts() : [];
        const plan = {
            entities,
            relationships,
            hierarchies,
            constraints: { primaryKeys: pks, uniqueKeys, checkConstraints, defaults },
            indexes,
            stats
        };
        return this.generateExtractionPlan(plan);
    }
    detectHierarchies(fks, pkMap) {
        return fks
            .filter((fk) => fk.table === fk.referencedTable)
            .map((fk) => ({ table: fk.table, pk: pkMap.get(fk.table) || 'id', fk: fk.column }));
    }
    mapRelationships(fks, pkMap) {
        const rels = [];
        // Simple FK relationships assumed one-to-many unless FK col is unique/PK (heuristic 1-to-1)
        for (const fk of fks) {
            const type = fk.column === pkMap.get(fk.table) ? 'one-to-one' : 'one-to-many';
            rels.push({ type, table: fk.table, column: fk.column, referencedTable: fk.referencedTable, referencedColumn: fk.referencedColumn });
        }
        const byTable = new Map();
        fks.forEach((fk) => {
            if (!byTable.has(fk.table)) {
                byTable.set(fk.table, { fks: [] });
            }
            byTable.get(fk.table).fks.push(fk);
        });
        for (const [table, info] of byTable) {
            if (info.fks.length === 2) {
                const [fk1, fk2] = info.fks;
                if (fk1.referencedTable !== fk2.referencedTable) {
                    // Mark both directions as many-to-many
                    rels.push({
                        type: 'many-to-many',
                        table: fk1.referencedTable,
                        column: fk1.referencedColumn,
                        referencedTable: fk2.referencedTable,
                        referencedColumn: fk2.referencedColumn,
                        junctionTable: table
                    });
                    rels.push({
                        type: 'many-to-many',
                        table: fk2.referencedTable,
                        column: fk2.referencedColumn,
                        referencedTable: fk1.referencedTable,
                        referencedColumn: fk1.referencedColumn,
                        junctionTable: table
                    });
                }
            }
        }
        return rels;
    }
    /**
     * Enriches the preliminary plan with simple cost/size heuristics or re-ordering.
     * For now we just sort entities descending by estimated row count to optimise streaming order.
     */
    generateExtractionPlan(plan) {
        if (plan.stats && plan.stats.length) {
            const rowMap = new Map(plan.stats.map((s) => [s.table, s.rows]));
            plan.entities.sort((a, b) => (rowMap.get(b.table) ?? 0) - (rowMap.get(a.table) ?? 0));
        }
        return plan;
    }
}
exports.SchemaDetectionEngine = SchemaDetectionEngine;
