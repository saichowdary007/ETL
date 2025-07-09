import os from 'os';
import http from 'http';
// @ts-ignore – prom-client may not have types bundled
import prom from 'prom-client';
// @ts-ignore – using ws without types; for full types run: npm i -D @types/ws
import { WebSocketServer, WebSocket } from 'ws';
import { EventEmitter } from 'events';

const collectDefaultMetrics = prom.collectDefaultMetrics;
collectDefaultMetrics();

const recordCounter = new prom.Counter({ name: 'records_processed_total', help: 'Total number of records processed' });
const processingRateGauge = new prom.Gauge({ name: 'processing_rate_per_sec', help: 'Processing rate per second' });
const memoryGauge = new prom.Gauge({ name: 'memory_rss_mb', help: 'RSS memory usage in MB' });
const integrityGauge = new prom.Gauge({ name: 'relationship_integrity', help: 'Parent-child relationship integrity % (0-100)' });
const errorCounter = new prom.Counter({ name: 'records_error_total', help: 'Total number of errored records' });
const diskGauge = new prom.Gauge({ name: 'disk_used_mb', help: 'Output directory disk usage MB' });
const netGauge = new prom.Gauge({ name: 'network_out_kbps', help: 'Approx network output kB/s' });

export class RealTimeMonitoringSystem extends EventEmitter {
  private processed = 0;
  private lastProcessed = 0;
  private startTime = Date.now();
  private lastReportTime = Date.now();
  private intervalId?: NodeJS.Timer;
  private server?: http.Server;
  private wss?: WebSocketServer;
  private history: any[] = [];
  private maxHistory = 600; // keep last 600 samples (~10 minutes if interval=1s)
  private errors = 0;
  private lastBytesSent = 0;

  constructor(private outputDir = 'output', private alertConfig: { errorRate?: number } = {}) {
    super();
  }

  recordProcessed(n = 1) {
    this.processed += n;
    recordCounter.inc(n);
  }

  recordRelationshipIntegrity(percent: number) {
    integrityGauge.set(percent);
  }

  recordError(n = 1) {
    this.errors += n;
    errorCounter.inc(n);
  }

  start(intervalMs = 1000, metricsPort = 9091, wsPort = 9092) {
    if (this.intervalId) return;
    this.intervalId = setInterval(() => this.report(), intervalMs) as unknown as NodeJS.Timeout;

    this.server = http.createServer(async (_req, res) => {
      if (_req.url === '/metrics') {
        res.setHeader('Content-Type', prom.register.contentType);
        res.end(await prom.register.metrics());
      } else {
        res.statusCode = 404;
        res.end();
      }
    });
    this.server.listen(metricsPort, () => console.log(`[Monitor] Prometheus metrics exposed on :${metricsPort}/metrics`));

    // WebSocket server
    this.wss = new WebSocketServer({ port: wsPort });
    this.wss.on('connection', (ws: WebSocket) => {
      ws.send(JSON.stringify({ history: this.history }));
    });
    console.log(`[Monitor] WebSocket metrics on :${wsPort}`);
  }

  stop() {
    if (this.intervalId) clearInterval(this.intervalId as unknown as NodeJS.Timeout);
    if (this.server) this.server.close();
    if (this.wss) this.wss.close();
  }

  private report() {
    const now = Date.now();
    const elapsedSec = (now - this.lastReportTime) / 1000;
    const processedInInterval = this.processed - this.lastProcessed;
    const rate = elapsedSec > 0 ? processedInInterval / elapsedSec : 0;
    
    this.lastProcessed = this.processed;
    this.lastReportTime = now;

    const mem = process.memoryUsage().rss / (1024 * 1024);
    processingRateGauge.set(rate);
    memoryGauge.set(mem);
    const integrityMetric = prom.register.getSingleMetric('relationship_integrity')?.get();
    const integrity = integrityMetric && 'values' in integrityMetric ? (integrityMetric as any).values[0]?.value ?? null : null;

    // Disk usage for output dir
    try {
      const du = this.dirSize(this.outputDir) / (1024 * 1024);
      diskGauge.set(du);
    } catch {}

    // Approx network throughput (assume output file size increase ~ network)
    const bytesSent = this.processed * 100; // rough 100 bytes avg per record
    const deltaBytes = bytesSent - this.lastBytesSent;
    this.lastBytesSent = bytesSent;
    netGauge.set((deltaBytes / 1024));

    const payloadObj = { processed: this.processed, rate, memory: mem, integrity, errors: this.errors, disk: mem, timestamp: Date.now() };
    this.history.push(payloadObj);
    if (this.history.length > this.maxHistory) this.history.shift();
    const payload = JSON.stringify(payloadObj);
    this.wss?.clients.forEach((client: WebSocket) => {
      if ((client as WebSocket).readyState === 1) {
        client.send(payload);
      }
    });
    console.log(`[Monitor] processed=${this.processed} rate=${rate.toFixed(1)}/s memory=${mem.toFixed(1)}MB load=${os.loadavg()[0].toFixed(2)}`);

    this.emit('metrics', payloadObj);

    this.checkAlerts(rate, integrity);
  }

  private dirSize(path: string): number {
    const fsStat = require('fs');
    const { readdirSync, statSync } = fsStat;
    let total = 0;
    const list = readdirSync(path);
    for (const f of list) {
      const s = statSync(`${path}/${f}`);
      total += s.isDirectory() ? this.dirSize(`${path}/${f}`) : s.size;
    }
    return total;
  }

  private checkAlerts(rate: number, integrity: number | null) {
    if (this.alertConfig.errorRate) {
      const errRate = this.errors / Math.max(1, this.processed);
      if (errRate > this.alertConfig.errorRate) {
        console.error(`[ALERT] Error rate ${(errRate * 100).toFixed(2)}% exceeds threshold`);
      }
    }
    if (integrity !== null && integrity < 90) {
      console.error(`[ALERT] Relationship integrity low: ${integrity}%`);
    }
  }
} 