"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MasterController = void 0;
const SchemaDetectionEngine_1 = require("../core/SchemaDetectionEngine");
const HierarchicalExtractor_1 = require("../core/HierarchicalExtractor");
const RelationshipProcessor_1 = require("../core/RelationshipProcessor");
const CheckpointManager_1 = require("../core/CheckpointManager");
const fs_extra_1 = __importDefault(require("fs-extra"));
const AdaptiveBatcher_1 = require("../core/AdaptiveBatcher");
const RealTimeMonitoringSystem_1 = require("../core/RealTimeMonitoringSystem");
const SecurityComplianceFramework_1 = require("../core/SecurityComplianceFramework");
const CircularReferenceHandler_1 = require("../core/CircularReferenceHandler");
const MultiLevelTransformationPipeline_1 = require("../core/MultiLevelTransformationPipeline");
const DenormalizationEngine_1 = require("../core/DenormalizationEngine");
const OutputStructureOptimizer_1 = require("../core/OutputStructureOptimizer");
const ScalabilityManager_1 = require("../core/ScalabilityManager");
class MasterController {
    constructor(config) {
        this.config = config;
        this.schemaDetector = new SchemaDetectionEngine_1.SchemaDetectionEngine();
        this.relationshipProcessor = new RelationshipProcessor_1.RelationshipProcessor();
        this.checkpoint = new CheckpointManager_1.CheckpointManager();
        this.batcher = new AdaptiveBatcher_1.AdaptiveBatcher();
        this.monitor = new RealTimeMonitoringSystem_1.RealTimeMonitoringSystem();
        this.security = new SecurityComplianceFramework_1.SecurityComplianceFramework();
        this.circularHandler = new CircularReferenceHandler_1.CircularReferenceHandler();
        this.transformer = new MultiLevelTransformationPipeline_1.MultiLevelTransformationPipeline();
        this.denormalizer = new DenormalizationEngine_1.DenormalizationEngine();
        this.optimizer = new OutputStructureOptimizer_1.OutputStructureOptimizer();
        this.scaler = new ScalabilityManager_1.ScalabilityManager();
        this.integrityMap = new Map();
        this.recordsSinceFlush = 0;
        this.lastFlush = Date.now();
        this.extractor = new HierarchicalExtractor_1.HierarchicalExtractor(this.config.adapter);
        this.flushRecordInterval = config.flushRecordInterval ?? 1000;
        this.flushTimeIntervalMs = config.flushTimeIntervalMs ?? 30000;
    }
    async executeWithRetry(maxRetries = 3) {
        let attempt = 0;
        const baseDelay = 2000;
        while (attempt <= maxRetries) {
            try {
                return await this.executeInternal();
            }
            catch (err) {
                attempt++;
                console.error(`[Master] Error on attempt ${attempt}`, err);
                if (attempt > maxRetries)
                    throw err;
                const delay = baseDelay * 2 ** (attempt - 1);
                await new Promise((r) => setTimeout(r, delay));
            }
        }
    }
    async execute() {
        return this.executeWithRetry(3);
    }
    async executeInternal() {
        console.log('[Master] Starting execution...');
        this.monitor.start(1000, 9091, 9092);
        await this.checkpoint.initialize(30000);
        if (await this.checkpoint.wasInterrupted()) {
            console.warn('[Master] Detected previous interruption. Resuming from checkpoint.');
        }
        console.log('[Master] Analyzing database schema...');
        this.currentPlan = await this.schemaDetector.analyzeDatabase(this.config.adapter);
        console.log('[Master] Schema analysis complete:', JSON.stringify(this.currentPlan, null, 2));
        // Determine optimal output mapping
        console.log('[Master] Generating optimal structure...');
        this.outputPlan = this.optimizer.generateOptimalStructure(this.currentPlan, {});
        console.log('[Master] Output plan generated:', JSON.stringify(this.outputPlan, null, 2));
        this.monitor.on('metrics', (m) => {
            console.log('[Master] Metrics update:', m);
            this.scaler.evaluateMetrics({ rate: m.rate, memory: m.memory, errors: m.errors });
        });
        console.log('[Master] Processing entities...');
        for (const entity of this.currentPlan.entities) {
            console.log(`[Master] Processing entity: ${entity.table}`);
            if (this.checkpoint.isTableCompleted(entity.table)) {
                console.log(`[Master] Skipping already completed table: ${entity.table}`);
                continue;
            }
            // Determine PK column and lastPk offset
            const pkColumnEntry = this.currentPlan.constraints.primaryKeys.find((pk) => pk.table === entity.table);
            const pkColumn = pkColumnEntry?.column;
            const lastPk = pkColumn ? this.checkpoint.getLastPk(entity.table) : undefined;
            console.log(`[Master] Table ${entity.table} - PK Column: ${pkColumn}, Last PK: ${lastPk}`);
            console.log(`[Master] Starting extraction for table: ${entity.table}`);
            const stream = await this.extractor.extract(entity.table, {
                hierarchical: entity.nestingLevels !== undefined,
                parentField: 'parent_id',
                idField: 'id',
                pkColumn,
                minPk: lastPk
            });
            const outputPath = this.outputPlan?.entities[entity.table] ?? `output/${entity.table}.ndjson`;
            console.log(`[Master] Processing stream to ${outputPath}`);
            await this.processEntityStream(entity.table, stream, outputPath, pkColumn);
            // After processing table, assume integrity 100 for now
            this.monitor.recordRelationshipIntegrity(100);
            this.checkpoint.markTableCompleted(entity.table);
            await this.checkpoint.save();
            console.log(`[Master] Completed processing table: ${entity.table}`);
        }
        // Phase 5: Validation and Completion
        console.log('[Master] Starting validation phase...');
        const validator = new (await Promise.resolve().then(() => __importStar(require('../core/ResultValidator')))).ResultValidator();
        await validator.validateResults(this.currentPlan, this.config.adapter, this.integrityMap);
        console.log('[Master] Validation complete');
    }
    async processEntityStream(table, stream, outputPath, pkColumn) {
        console.log(`[Stream] Starting stream processing for table: ${table}`);
        await fs_extra_1.default.ensureFile(outputPath);
        const writeStream = fs_extra_1.default.createWriteStream(outputPath, { flags: 'a' });
        const relationshipStage = this.relationshipProcessor.createProcessor(table, this.currentPlan?.relationships || []);
        // Monitor processed records and update checkpoint pk per record
        relationshipStage.on('data', (record) => {
            this.monitor.recordProcessed();
            this.checkpoint.recordProcessed();
            if (pkColumn) {
                this.checkpoint.updateTablePk(table, record[pkColumn]);
            }
            console.log(`[Stream] Processed record from ${table}:`, record);
        });
        relationshipStage.on('integrity', (stats) => {
            const invalidRatio = stats.invalid / Math.max(1, stats.processed);
            if (invalidRatio > 0) {
                console.warn(`[Integrity] Table ${table} invalid records ratio ${(invalidRatio * 100).toFixed(2)}%`);
            }
            this.integrityMap.set(table, stats);
        });
        const complianceStage = this.security.createComplianceProcessor(table);
        const complexity = (this.currentPlan?.relationships || []).filter((r) => r.table === table).length + 1;
        const batchingStage = this.batcher.createBatcher({ baseBatchSize: 1000, complexityFactor: complexity });
        // Build pipeline
        let pipelineStream = stream;
        console.log(`[Stream] Setting up pipeline for ${table}`);
        pipelineStream = pipelineStream.pipe(relationshipStage);
        pipelineStream = pipelineStream.pipe(this.circularHandler.createHandler());
        // Transformation pipeline returns new readable
        pipelineStream = this.transformer.createPipeline(pipelineStream, table);
        pipelineStream = pipelineStream.pipe(this.denormalizer.createDenormalizer());
        pipelineStream = pipelineStream.pipe(complianceStage);
        pipelineStream = pipelineStream.pipe(batchingStage);
        batchingStage.on('batchMetrics', (m) => {
            // Could extend RealTimeMonitoringSystem to track batches; for now just log
            console.log(`[Batch] table=${table} batches=${m.batches} records=${m.records}`);
        });
        return new Promise((resolve, reject) => {
            pipelineStream
                .on('data', (chunk) => {
                writeStream.write(chunk);
                console.log(`[Stream] Wrote chunk for ${table}`);
            })
                .on('end', () => {
                console.log(`[Stream] Completed stream for ${table}`);
                resolve();
            })
                .on('error', (err) => {
                console.error(`[Stream] Error processing ${table}:`, err);
                reject(err);
            });
        });
    }
}
exports.MasterController = MasterController;
