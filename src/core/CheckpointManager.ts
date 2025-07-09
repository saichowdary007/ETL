// @ts-ignore lack types in some env
import fs from 'fs-extra';

export interface CheckpointState {
  processedRecords: number;
  errorCount?: number;
  lastTable?: string;
  completedTables?: string[];
  lastPk?: Record<string, any>;
  startTime?: number;
}

export class CheckpointManager {
  private state: CheckpointState = { processedRecords: 0 };
  private filePath = 'checkpoint.json';
  private flushTimer?: NodeJS.Timer;
  private flushIntervalMs = 30000;
  private lockFile = 'checkpoint.lock';

  async initialize(autoFlushMs: number = 30000) {
    await this.createLock();
    this.flushIntervalMs = autoFlushMs;
    await this.load();
    this.startAutoFlush();
  }

  private async load() {
    if (await fs.pathExists(this.filePath)) {
      this.state = await fs.readJson(this.filePath);
    }
    if (!this.state.startTime) {
      this.state.startTime = Date.now();
    }
  }

  async save() {
    const elapsedSec = this.state.startTime ? (Date.now() - this.state.startTime) / 1000 : 0;
    if (elapsedSec > 0) {
      (this.state as any).processingRate = this.state.processedRecords / elapsedSec;
    }
    (this.state as any).memoryRss = process.memoryUsage().rss;
    await fs.writeJson(this.filePath, this.state);
  }

  update(partial: Partial<CheckpointState>) {
    Object.assign(this.state, partial);
  }

  recordProcessed(n = 1) {
    this.state.processedRecords += n;
  }

  recordError(n = 1) {
    if (!this.state.errorCount) this.state.errorCount = 0;
    this.state.errorCount += n;
  }

  getState() {
    return this.state;
  }

  markTableCompleted(table: string) {
    if (!this.state.completedTables) this.state.completedTables = [];
    if (!this.state.completedTables.includes(table)) {
      this.state.completedTables.push(table);
    }
  }

  isTableCompleted(table: string): boolean {
    return this.state.completedTables?.includes(table) ?? false;
  }

  updateTablePk(table: string, pkValue: any) {
    if (!this.state.lastPk) this.state.lastPk = {};
    this.state.lastPk[table] = pkValue;
  }

  getLastPk(table: string): any | undefined {
    return this.state.lastPk?.[table];
  }

  private startAutoFlush() {
    if (this.flushTimer) clearInterval(this.flushTimer as unknown as NodeJS.Timeout);
    this.flushTimer = setInterval(() => this.save().catch(console.error), this.flushIntervalMs) as unknown as NodeJS.Timer;
  }

  async resumeProcessing() {
    return this.state;
  }

  dispose() {
    if (this.flushTimer) clearInterval(this.flushTimer as unknown as NodeJS.Timeout);
    this.clearLock();
  }

  private async createLock() {
    await fs.ensureFile(this.lockFile);
  }

  private clearLock() {
    fs.remove(this.lockFile).catch(() => {});
  }

  async wasInterrupted(): Promise<boolean> {
    return fs.pathExists(this.lockFile);
  }
} 