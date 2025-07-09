import { DatabaseAdapter } from '../adapters/DatabaseAdapter';
import { SchemaDetectionEngine } from '../core/SchemaDetectionEngine';
import { HierarchicalExtractor } from '../core/HierarchicalExtractor';
import { RelationshipProcessor } from '../core/RelationshipProcessor';
import { CheckpointManager } from '../core/CheckpointManager';
import { Readable } from 'stream';
import fs from 'fs-extra';
import { AdaptiveBatcher } from '../core/AdaptiveBatcher';
import { RealTimeMonitoringSystem } from '../core/RealTimeMonitoringSystem';
import { SecurityComplianceFramework } from '../core/SecurityComplianceFramework';
import { CircularReferenceHandler } from '../core/CircularReferenceHandler';
import { MultiLevelTransformationPipeline } from '../core/MultiLevelTransformationPipeline';
import { DenormalizationEngine } from '../core/DenormalizationEngine';
import { OutputStructureOptimizer, OptimizedStructure } from '../core/OutputStructureOptimizer';
import { ScalabilityManager } from '../core/ScalabilityManager';

interface ControllerConfig {
  adapter: DatabaseAdapter;
  flushRecordInterval?: number;
  flushTimeIntervalMs?: number;
}

export class MasterController {
  private schemaDetector = new SchemaDetectionEngine();
  private extractor!: HierarchicalExtractor;
  private relationshipProcessor = new RelationshipProcessor();
  private checkpoint = new CheckpointManager();
  private currentPlan: any;
  private batcher = new AdaptiveBatcher();
  private monitor = new RealTimeMonitoringSystem();
  private security = new SecurityComplianceFramework();
  private circularHandler = new CircularReferenceHandler();
  private transformer = new MultiLevelTransformationPipeline();
  private denormalizer = new DenormalizationEngine();
  private optimizer = new OutputStructureOptimizer();
  private outputPlan?: OptimizedStructure;
  private scaler = new ScalabilityManager();
  private integrityMap = new Map<string, { invalid: number; processed: number }>();

  // checkpoint flush config
  private flushRecordInterval: number;
  private flushTimeIntervalMs: number;
  private recordsSinceFlush = 0;
  private lastFlush = Date.now();

  constructor(private config: ControllerConfig) {
    this.extractor = new HierarchicalExtractor(this.config.adapter);
    this.flushRecordInterval = config.flushRecordInterval ?? 1000;
    this.flushTimeIntervalMs = config.flushTimeIntervalMs ?? 30000;
  }

  private async executeWithRetry(maxRetries = 3) {
    let attempt = 0;
    const baseDelay = 2000;
    while (attempt <= maxRetries) {
      try {
        return await this.executeInternal();
      } catch (err) {
        attempt++;
        console.error(`[Master] Error on attempt ${attempt}`, err);
        if (attempt > maxRetries) throw err;
        const delay = baseDelay * 2 ** (attempt - 1);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }

  async execute() {
    return this.executeWithRetry(3);
  }

  private async executeInternal() {
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

    this.monitor.on('metrics', (m: any) => {
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
      const pkColumnEntry = (this.currentPlan.constraints.primaryKeys as { table: string; column: string }[]).find((pk: any) => pk.table === entity.table);
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
    const validator = new (await import('../core/ResultValidator')).ResultValidator();
    await validator.validateResults(this.currentPlan, this.config.adapter, this.integrityMap);
    console.log('[Master] Validation complete');
  }

  private async processEntityStream(table: string, stream: NodeJS.ReadableStream, outputPath: string, pkColumn?: string) {
    console.log(`[Stream] Starting stream processing for table: ${table}`);
    await fs.ensureFile(outputPath);
    const writeStream = fs.createWriteStream(outputPath, { flags: 'a' });

    const relationshipStage = this.relationshipProcessor.createProcessor(table, this.currentPlan?.relationships || []);

    // Monitor processed records and update checkpoint pk per record
    relationshipStage.on('data', (record: any) => {
      this.monitor.recordProcessed();
      this.checkpoint.recordProcessed();
      if (pkColumn) {
        this.checkpoint.updateTablePk(table, record[pkColumn]);
      }
      console.log(`[Stream] Processed record from ${table}:`, record);
    });

    relationshipStage.on('integrity', (stats: any) => {
      const invalidRatio = stats.invalid / Math.max(1, stats.processed);
      if (invalidRatio > 0) {
        console.warn(`[Integrity] Table ${table} invalid records ratio ${(invalidRatio * 100).toFixed(2)}%`);
      }
      this.integrityMap.set(table, stats);
    });

    const complianceStage = this.security.createComplianceProcessor(table);
    const complexity = (this.currentPlan?.relationships || []).filter((r: any) => r.table === table).length + 1;
    const batchingStage = this.batcher.createBatcher({ baseBatchSize: 1000, complexityFactor: complexity });

    // Build pipeline
    let pipelineStream: Readable | NodeJS.ReadableStream = stream;

    console.log(`[Stream] Setting up pipeline for ${table}`);
    pipelineStream = pipelineStream.pipe(relationshipStage);
    pipelineStream = pipelineStream.pipe(this.circularHandler.createHandler());

    // Transformation pipeline returns new readable
    pipelineStream = this.transformer.createPipeline(pipelineStream as Readable, table);

    pipelineStream = pipelineStream.pipe(this.denormalizer.createDenormalizer());
    pipelineStream = pipelineStream.pipe(complianceStage);
    pipelineStream = pipelineStream.pipe(batchingStage);

    batchingStage.on('batchMetrics', (m: any) => {
      // Could extend RealTimeMonitoringSystem to track batches; for now just log
      console.log(`[Batch] table=${table} batches=${m.batches} records=${m.records}`);
    });

    return new Promise<void>((resolve, reject) => {
      (pipelineStream as any)
        .on('data', (chunk: Buffer | string) => {
          writeStream.write(chunk);
          console.log(`[Stream] Wrote chunk for ${table}`);
        })
        .on('end', () => {
          console.log(`[Stream] Completed stream for ${table}`);
          resolve();
        })
        .on('error', (err: Error) => {
          console.error(`[Stream] Error processing ${table}:`, err);
          reject(err);
        });
    });
  }
} 