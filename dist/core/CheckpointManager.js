"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CheckpointManager = void 0;
// @ts-ignore lack types in some env
const fs_extra_1 = __importDefault(require("fs-extra"));
class CheckpointManager {
    constructor() {
        this.state = { processedRecords: 0 };
        this.filePath = 'checkpoint.json';
        this.flushIntervalMs = 30000;
        this.lockFile = 'checkpoint.lock';
    }
    async initialize(autoFlushMs = 30000) {
        await this.createLock();
        this.flushIntervalMs = autoFlushMs;
        await this.load();
        this.startAutoFlush();
    }
    async load() {
        if (await fs_extra_1.default.pathExists(this.filePath)) {
            this.state = await fs_extra_1.default.readJson(this.filePath);
        }
        if (!this.state.startTime) {
            this.state.startTime = Date.now();
        }
    }
    async save() {
        const elapsedSec = this.state.startTime ? (Date.now() - this.state.startTime) / 1000 : 0;
        if (elapsedSec > 0) {
            this.state.processingRate = this.state.processedRecords / elapsedSec;
        }
        this.state.memoryRss = process.memoryUsage().rss;
        await fs_extra_1.default.writeJson(this.filePath, this.state);
    }
    update(partial) {
        Object.assign(this.state, partial);
    }
    recordProcessed(n = 1) {
        this.state.processedRecords += n;
    }
    recordError(n = 1) {
        if (!this.state.errorCount)
            this.state.errorCount = 0;
        this.state.errorCount += n;
    }
    getState() {
        return this.state;
    }
    markTableCompleted(table) {
        if (!this.state.completedTables)
            this.state.completedTables = [];
        if (!this.state.completedTables.includes(table)) {
            this.state.completedTables.push(table);
        }
    }
    isTableCompleted(table) {
        return this.state.completedTables?.includes(table) ?? false;
    }
    updateTablePk(table, pkValue) {
        if (!this.state.lastPk)
            this.state.lastPk = {};
        this.state.lastPk[table] = pkValue;
    }
    getLastPk(table) {
        return this.state.lastPk?.[table];
    }
    startAutoFlush() {
        if (this.flushTimer)
            clearInterval(this.flushTimer);
        this.flushTimer = setInterval(() => this.save().catch(console.error), this.flushIntervalMs);
    }
    async resumeProcessing() {
        return this.state;
    }
    dispose() {
        if (this.flushTimer)
            clearInterval(this.flushTimer);
        this.clearLock();
    }
    async createLock() {
        await fs_extra_1.default.ensureFile(this.lockFile);
    }
    clearLock() {
        fs_extra_1.default.remove(this.lockFile).catch(() => { });
    }
    async wasInterrupted() {
        return fs_extra_1.default.pathExists(this.lockFile);
    }
}
exports.CheckpointManager = CheckpointManager;
