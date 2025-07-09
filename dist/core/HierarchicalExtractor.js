"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HierarchicalExtractor = void 0;
class HierarchicalExtractor {
    constructor(adapter) {
        this.adapter = adapter;
    }
    async extract(table, options = {}) {
        const { hierarchical = false, parentField = 'parent_id', idField = 'id', minPk, pkColumn, recursiveSupported = true } = options;
        // Build query
        let query;
        let params = [];
        if (hierarchical) {
            if (recursiveSupported) {
                query = `WITH RECURSIVE ${table}_hierarchy AS (
          SELECT *, 0 AS level FROM ${table} WHERE ${parentField} IS NULL
            ${minPk !== undefined && pkColumn ? `AND ${pkColumn} > $1` : ''}
          UNION ALL
          SELECT t.*, h.level + 1 AS level FROM ${table} t JOIN ${table}_hierarchy h ON t.${parentField} = h.${idField}
        ) SELECT * FROM ${table}_hierarchy ORDER BY level, ${parentField}, ${idField}`;
                if (minPk !== undefined && pkColumn) {
                    params.push(minPk);
                }
            }
            else {
                // Fallback: iterative expansion using temporary variable – may be less efficient but widely supported
                query = `SELECT *, 0 as level FROM ${table} WHERE ${parentField} IS NULL`;
                // Note: For simplicity, the non-recursive fallback relies on consumer performing client-side walks.
            }
        }
        else {
            query = `SELECT * FROM ${table}`;
            if (minPk !== undefined && pkColumn) {
                query += ` WHERE ${pkColumn} > $1`;
                params.push(minPk);
            }
        }
        // Allow adapter to optimise query if supported
        const finalQuery = this.adapter.optimizeQuery ? this.adapter.optimizeQuery(query) : query;
        console.log(`[Extractor] Executing query for table ${table}:`, { query: finalQuery, params });
        return this.adapter.createQueryStream(finalQuery, params);
    }
}
exports.HierarchicalExtractor = HierarchicalExtractor;
