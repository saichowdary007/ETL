export interface TableInfo {
  name: string;
  columns: string[];
  primaryKey?: string;
  foreignKeys?: Array<{ column: string; referencedTable: string; referencedColumn: string }>;
}

export type RelationshipType = 'one-to-one' | 'one-to-many' | 'many-to-many';

export interface RelationshipInfo {
  type: RelationshipType;
  table: string;
  column: string;
  referencedTable: string;
  referencedColumn: string;
  /** Populated for many-to-many to indicate the junction table */
  junctionTable?: string;
}

export interface HierarchyInfo {
  table: string;
  pk: string;
  fk: string;
}

export interface ExtractionPlan {
  entities: { table: string; nestingLevels?: number }[];
  relationships: RelationshipInfo[];
  hierarchies: HierarchyInfo[];
  constraints: {
    primaryKeys: { table: string; column: string }[];
    uniqueKeys: { table: string; column: string }[];
    checkConstraints: { table: string; expression: string }[];
    defaults: { table: string; column: string; default: any }[];
  };
  indexes: any[]; // adapter-specific – filled if available
  stats?: { table: string; rows: number }[];
}

export class SchemaDetectionEngine {
  async analyzeDatabase(connection: {
    getTables: () => Promise<string[]>;
    getPrimaryKeys: () => Promise<{ table: string; column: string }[]>;
    getForeignKeys: () => Promise<{ table: string; column: string; referencedTable: string; referencedColumn: string }[]>;
    getIndexes?: () => Promise<any[]>;
    getUniqueConstraints?: () => Promise<{ table: string; column: string }[]>;
    getCheckConstraints?: () => Promise<{ table: string; expression: string }[]>;
    getColumnDefaults?: () => Promise<{ table: string; column: string; default: any }[]>;
    getRowCounts?: () => Promise<{ table: string; rows: number }[]>;
  }): Promise<ExtractionPlan> {
    const tables = await connection.getTables();
    const pks = await connection.getPrimaryKeys();
    const fks = await connection.getForeignKeys();

    const pkMap = new Map<string, string>();
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
    const hierarchies: HierarchyInfo[] = this.detectHierarchies(fks, pkMap);

    // 3. Relationships – includes many-to-many detection
    const relationships: RelationshipInfo[] = this.mapRelationships(fks, pkMap);

    // 4. Constraint analysis (unique, check, defaults)
    const uniqueKeys = connection.getUniqueConstraints ? await connection.getUniqueConstraints() : [];
    const checkConstraints = connection.getCheckConstraints ? await connection.getCheckConstraints() : [];
    const defaults = connection.getColumnDefaults ? await connection.getColumnDefaults() : [];

    // 5. Index catalogue (best-effort)
    const indexes = connection.getIndexes ? await connection.getIndexes() : [];

    // 6. Row counts / stats
    const stats = connection.getRowCounts ? await connection.getRowCounts() : [];

    const plan: ExtractionPlan = {
      entities,
      relationships,
      hierarchies,
      constraints: { primaryKeys: pks, uniqueKeys, checkConstraints, defaults },
      indexes,
      stats
    };

    return this.generateExtractionPlan(plan);
  }

  private detectHierarchies(
    fks: { table: string; column: string; referencedTable: string; referencedColumn: string }[],
    pkMap: Map<string, string>
  ): HierarchyInfo[] {
    return fks
      .filter((fk) => fk.table === fk.referencedTable)
      .map((fk) => ({ table: fk.table, pk: pkMap.get(fk.table) || 'id', fk: fk.column }));
  }

  private mapRelationships(
    fks: { table: string; column: string; referencedTable: string; referencedColumn: string }[],
    pkMap: Map<string, string>
  ): RelationshipInfo[] {
    const rels: RelationshipInfo[] = [];

    // Simple FK relationships assumed one-to-many unless FK col is unique/PK (heuristic 1-to-1)
    for (const fk of fks) {
      const type: RelationshipType = fk.column === pkMap.get(fk.table) ? 'one-to-one' : 'one-to-many';
      rels.push({ type, table: fk.table, column: fk.column, referencedTable: fk.referencedTable, referencedColumn: fk.referencedColumn });
    }

    // Detect potential many-to-many via junction tables (two FKs to distinct tables, no extra columns)
    type FK = { table: string; column: string; referencedTable: string; referencedColumn: string };
    const byTable = new Map<string, { fks: FK[] }>();
    fks.forEach((fk) => {
      if (!byTable.has(fk.table)) {
        byTable.set(fk.table, { fks: [] });
      }
      byTable.get(fk.table)!.fks.push(fk);
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
  private generateExtractionPlan(plan: ExtractionPlan): ExtractionPlan {
    if (plan.stats && plan.stats.length) {
      const rowMap = new Map(plan.stats.map((s) => [s.table, s.rows]));
      plan.entities.sort((a, b) => (rowMap.get(b.table) ?? 0) - (rowMap.get(a.table) ?? 0));
    }
    return plan;
  }
} 