import { Transform } from 'stream';
// @ts-ignore – fs-extra may lack type declarations in some envs
import fs from 'fs-extra';

interface HandlerOptions {
  /** Key name for the primary id field */
  idField?: string;
  /** Parent reference field name */
  parentField?: string;
  /** Placeholder value used when a circular reference is detected */
  placeholder?: any;
  /** Optional file path to store mapping of circular refs */
  outputFile?: string;
}

/**
 * Simple circular reference handler that prevents self-referencing or ancestor loops from cascading endlessly.
 * It maintains a processing stack and inserts a placeholder when a circular path is detected.
 */
export class CircularReferenceHandler {
  constructor(private opts: HandlerOptions = {}) {}

  /** Simple DFS helper to detect if assigning parent would create a cycle */
  private wouldCreateCycle(childId: any, parentId: any, ancestryMap: Map<any, any>): boolean {
    let current = parentId;
    while (current !== undefined && current !== null) {
      if (current === childId) return true;
      current = ancestryMap.get(current);
    }
    return false;
  }

  /** Returns a Transform stream that breaks circular paths and adds metadata */
  createHandler() {
    const idField = this.opts.idField ?? 'id';
    const parentField = this.opts.parentField ?? 'parent_id';
    const placeholder = this.opts.placeholder ?? null;

    // Map<id, parentId> to reconstruct ancestry for DFS check
    const ancestry = new Map<any, any>();
    const circularMappings: any[] = [];
    const outFile = this.opts.outputFile;

    return new Transform({
      objectMode: true,
      transform: (record: any, _enc, cb) => {
        const id = record[idField];
        const parent = record[parentField];

        if (id !== undefined) {
          // Self loop
          if (id === parent) {
            record[parentField] = placeholder;
            record.__circular = true;
            circularMappings.push({ id, parent });
          } else if (parent !== undefined && parent !== null) {
            // Indirect cycle detection via ancestry map
            if (this.wouldCreateCycle(id, parent, ancestry)) {
              record[parentField] = placeholder;
              record.__circular = true;
              circularMappings.push({ id, parent });
            }
          }

          // After resolving, update ancestry map
          ancestry.set(id, record[parentField]);
        }

        cb(null, record);
      },
      flush(cb) {
        if (outFile && circularMappings.length) {
          try {
            fs.ensureFileSync(outFile);
            fs.appendFileSync(outFile, circularMappings.map((m) => JSON.stringify(m)).join('\n') + '\n');
          } catch (err) {
            return cb(err as Error);
          }
        }
        cb();
      }
    });
  }

  resolveCircularReference(record: any) {
    // For now, we simply set parent to placeholder & tag record; already handled in transform.
    return { ...record, __circular: true };
  }
} 