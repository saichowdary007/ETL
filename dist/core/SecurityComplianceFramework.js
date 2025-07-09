"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SecurityComplianceFramework = void 0;
const stream_1 = require("stream");
const fs_extra_1 = __importDefault(require("fs-extra"));
const crypto_1 = __importDefault(require("crypto"));
class AuditLogger {
    constructor() {
        this.stream = fs_extra_1.default.createWriteStream('logs/audit.log', { flags: 'a' });
    }
    write(entry) {
        this.stream.write(JSON.stringify({ ts: new Date().toISOString(), ...entry }) + '\n');
    }
    logDataAccess(record) {
        this.write({ event: 'DATA_ACCESS', id: record.id ?? null, table: record.__table ?? null });
    }
    logAccessDenied(record) {
        this.write({ event: 'ACCESS_DENIED', id: record.id ?? null, table: record.__table ?? null });
    }
    logErase(id, table) {
        this.write({ event: 'DATA_ERASE', id, table });
    }
}
class SecurityComplianceFramework {
    constructor(config = {}) {
        this.config = config;
        this.auditLogger = new AuditLogger();
    }
    createComplianceProcessor(tableName) {
        const globalCfg = this.config;
        const tableCfg = tableName && globalCfg.tableRules ? globalCfg.tableRules[tableName] ?? {} : {};
        const protectedFields = [...(globalCfg.protectedFields ?? []), ...(tableCfg.protectedFields ?? [])];
        const protectedPatterns = [...(globalCfg.protectedPatterns ?? []), ...(tableCfg.protectedPatterns ?? [])];
        const redactionRules = [...(globalCfg.redactionRules ?? []), ...(tableCfg.redactionRules ?? [])];
        const regexes = protectedPatterns.map((p) => (p instanceof RegExp ? p : new RegExp(p, 'i')));
        let validated = false;
        return new stream_1.Transform({
            objectMode: true,
            transform(record, _enc, cb) {
                if (!validated) {
                    // Validate protected fields
                    for (const field of protectedFields) {
                        if (!(field in record)) {
                            console.warn(`[Compliance] Warning: protected field '${field}' not present in record schema`);
                        }
                    }
                    validated = true;
                }
                // Field masking & redaction
                for (const key of Object.keys(record)) {
                    if (protectedFields.includes(key) || regexes.some((r) => r.test(key))) {
                        record[key] = '***';
                        continue;
                    }
                    // Apply redaction rules on string values
                    if (typeof record[key] === 'string') {
                        let value = record[key];
                        redactionRules.forEach(({ pattern, replacement }) => {
                            const regex = new RegExp(pattern, 'g');
                            value = value.replace(regex, replacement);
                        });
                        record[key] = value;
                    }
                }
                cb(null, record);
            }
        });
    }
    implementGDPRCompliance() {
        // Right to be forgotten
        // Data minimization
        // Consent tracking
        // Privacy by design
    }
    /**
     * GDPR – Right to be forgotten. This will invoke adapter deletion callback and write audit log.
     */
    async eraseRecord(table, id, deleter) {
        await deleter(table, id);
        this.auditLogger.logErase(id, table);
    }
}
exports.SecurityComplianceFramework = SecurityComplianceFramework;
// ---- Concrete helper classes ----
class DataProtector {
    constructor(secret = 'default_secret') {
        this.key = crypto_1.default.createHash('sha256').update(secret).digest();
    }
    /** Very simple AES-256-GCM encryption for PII fields */
    encrypt(text) {
        const iv = crypto_1.default.randomBytes(12);
        const cipher = crypto_1.default.createCipheriv('aes-256-gcm', this.key, iv);
        const enc = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
        const tag = cipher.getAuthTag();
        return Buffer.concat([iv, tag, enc]).toString('base64');
    }
    protect(record) {
        const clone = { ...record };
        for (const key of Object.keys(clone)) {
            if (typeof clone[key] === 'string' && /email|phone|ssn|password/i.test(key)) {
                clone[key] = this.encrypt(clone[key]);
            }
        }
        return clone;
    }
}
class AccessController {
    constructor(rules) {
        this.perms = {};
        for (const role of Object.keys(rules)) {
            this.perms[role] = new Set(rules[role]);
        }
    }
    hasPermission(table, role = 'default') {
        if (!this.perms[role])
            return false;
        return this.perms[role].has('*') || this.perms[role].has(table);
    }
}
