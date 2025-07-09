"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CircularReferenceHandler = void 0;
const stream_1 = require("stream");
// @ts-ignore – fs-extra may lack type declarations in some envs
const fs_extra_1 = __importDefault(require("fs-extra"));
/**
 * Simple circular reference handler that prevents self-referencing or ancestor loops from cascading endlessly.
 * It maintains a processing stack and inserts a placeholder when a circular path is detected.
 */
class CircularReferenceHandler {
    constructor(opts = {}) {
        this.opts = opts;
    }
    /** Simple DFS helper to detect if assigning parent would create a cycle */
    wouldCreateCycle(childId, parentId, ancestryMap) {
        let current = parentId;
        while (current !== undefined && current !== null) {
            if (current === childId)
                return true;
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
        const ancestry = new Map();
        const circularMappings = [];
        const outFile = this.opts.outputFile;
        return new stream_1.Transform({
            objectMode: true,
            transform: (record, _enc, cb) => {
                const id = record[idField];
                const parent = record[parentField];
                if (id !== undefined) {
                    // Self loop
                    if (id === parent) {
                        record[parentField] = placeholder;
                        record.__circular = true;
                        circularMappings.push({ id, parent });
                    }
                    else if (parent !== undefined && parent !== null) {
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
                        fs_extra_1.default.ensureFileSync(outFile);
                        fs_extra_1.default.appendFileSync(outFile, circularMappings.map((m) => JSON.stringify(m)).join('\n') + '\n');
                    }
                    catch (err) {
                        return cb(err);
                    }
                }
                cb();
            }
        });
    }
    resolveCircularReference(record) {
        // For now, we simply set parent to placeholder & tag record; already handled in transform.
        return { ...record, __circular: true };
    }
}
exports.CircularReferenceHandler = CircularReferenceHandler;
