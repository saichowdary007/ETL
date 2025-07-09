import { Transform } from 'stream';
import fs from 'fs-extra';
import crypto from 'crypto';

class AuditLogger {
  private stream = fs.createWriteStream('logs/audit.log', { flags: 'a' });

  private write(entry: any) {
    this.stream.write(JSON.stringify({ ts: new Date().toISOString(), ...entry }) + '\n');
  }

  logDataAccess(record: any) {
    this.write({ event: 'DATA_ACCESS', id: record.id ?? null, table: record.__table ?? null });
  }

  logAccessDenied(record: any) {
    this.write({ event: 'ACCESS_DENIED', id: record.id ?? null, table: record.__table ?? null });
  }

  logErase(id: any, table: string) {
    this.write({ event: 'DATA_ERASE', id, table });
  }
}

interface TableRule {
  protectedFields?: string[];
  protectedPatterns?: string[];
  redactionRules?: { pattern: string; replacement: string }[];
}

interface ComplianceConfig {
  protectedFields?: string[]; // names to mask
  protectedPatterns?: string[]; // patterns to mask
  redactionRules?: { pattern: string; replacement: string }[];
  tableRules?: Record<string, TableRule>;
}

export class SecurityComplianceFramework {
  private auditLogger = new AuditLogger();
  
  constructor(private config: ComplianceConfig = {}) {}

  createComplianceProcessor(tableName?: string) {
    const globalCfg = this.config as ComplianceConfig;
    const tableCfg = tableName && globalCfg.tableRules ? globalCfg.tableRules[tableName] ?? {} : {};

    const protectedFields = [...(globalCfg.protectedFields ?? []), ...(tableCfg.protectedFields ?? [])];
    const protectedPatterns = [...(globalCfg.protectedPatterns ?? []), ...(tableCfg.protectedPatterns ?? [])];
    const redactionRules = [...(globalCfg.redactionRules ?? []), ...(tableCfg.redactionRules ?? [])];

    const regexes = protectedPatterns.map((p: any) => (p instanceof RegExp ? p : new RegExp(p, 'i')));

    let validated = false;

    return new Transform({
      objectMode: true,
      transform(record: any, _enc, cb) {
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
            let value = record[key] as string;
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
  async eraseRecord(table: string, id: any, deleter: (table: string, id: any) => Promise<void>) {
    await deleter(table, id);
    this.auditLogger.logErase(id, table);
  }
}

// ---- Concrete helper classes ----

class DataProtector {
  private key: Buffer;

  constructor(secret: string = 'default_secret') {
    this.key = crypto.createHash('sha256').update(secret).digest();
  }

  /** Very simple AES-256-GCM encryption for PII fields */
  encrypt(text: string): string {
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', this.key, iv);
    const enc = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag();
    return Buffer.concat([iv, tag, enc]).toString('base64');
  }

  protect(record: any) {
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
  private perms: Record<string, Set<string>>; // role -> tables

  constructor(rules: Record<string, string[]>) {
    this.perms = {};
    for (const role of Object.keys(rules)) {
      this.perms[role] = new Set(rules[role]);
    }
  }

  hasPermission(table: string, role: string = 'default'): boolean {
    if (!this.perms[role]) return false;
    return this.perms[role].has('*') || this.perms[role].has(table);
  }
} 