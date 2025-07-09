"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RealTimeMonitoringSystem = void 0;
const os_1 = __importDefault(require("os"));
const http_1 = __importDefault(require("http"));
// @ts-ignore – prom-client may not have types bundled
const prom_client_1 = __importDefault(require("prom-client"));
// @ts-ignore – using ws without types; for full types run: npm i -D @types/ws
const ws_1 = require("ws");
const events_1 = require("events");
const collectDefaultMetrics = prom_client_1.default.collectDefaultMetrics;
collectDefaultMetrics();
const recordCounter = new prom_client_1.default.Counter({ name: 'records_processed_total', help: 'Total number of records processed' });
const processingRateGauge = new prom_client_1.default.Gauge({ name: 'processing_rate_per_sec', help: 'Processing rate per second' });
const memoryGauge = new prom_client_1.default.Gauge({ name: 'memory_rss_mb', help: 'RSS memory usage in MB' });
const integrityGauge = new prom_client_1.default.Gauge({ name: 'relationship_integrity', help: 'Parent-child relationship integrity % (0-100)' });
const errorCounter = new prom_client_1.default.Counter({ name: 'records_error_total', help: 'Total number of errored records' });
const diskGauge = new prom_client_1.default.Gauge({ name: 'disk_used_mb', help: 'Output directory disk usage MB' });
const netGauge = new prom_client_1.default.Gauge({ name: 'network_out_kbps', help: 'Approx network output kB/s' });
class RealTimeMonitoringSystem extends events_1.EventEmitter {
    constructor(outputDir = 'output', alertConfig = {}) {
        super();
        this.outputDir = outputDir;
        this.alertConfig = alertConfig;
        this.processed = 0;
        this.lastProcessed = 0;
        this.startTime = Date.now();
        this.lastReportTime = Date.now();
        this.history = [];
        this.maxHistory = 600; // keep last 600 samples (~10 minutes if interval=1s)
        this.errors = 0;
        this.lastBytesSent = 0;
    }
    recordProcessed(n = 1) {
        this.processed += n;
        recordCounter.inc(n);
    }
    recordRelationshipIntegrity(percent) {
        integrityGauge.set(percent);
    }
    recordError(n = 1) {
        this.errors += n;
        errorCounter.inc(n);
    }
    start(intervalMs = 1000, metricsPort = 9091, wsPort = 9092) {
        if (this.intervalId)
            return;
        this.intervalId = setInterval(() => this.report(), intervalMs);
        this.server = http_1.default.createServer(async (_req, res) => {
            if (_req.url === '/metrics') {
                res.setHeader('Content-Type', prom_client_1.default.register.contentType);
                res.end(await prom_client_1.default.register.metrics());
            }
            else {
                res.statusCode = 404;
                res.end();
            }
        });
        this.server.listen(metricsPort, () => console.log(`[Monitor] Prometheus metrics exposed on :${metricsPort}/metrics`));
        // WebSocket server
        this.wss = new ws_1.WebSocketServer({ port: wsPort });
        this.wss.on('connection', (ws) => {
            ws.send(JSON.stringify({ history: this.history }));
        });
        console.log(`[Monitor] WebSocket metrics on :${wsPort}`);
    }
    stop() {
        if (this.intervalId)
            clearInterval(this.intervalId);
        if (this.server)
            this.server.close();
        if (this.wss)
            this.wss.close();
    }
    report() {
        const now = Date.now();
        const elapsedSec = (now - this.lastReportTime) / 1000;
        const processedInInterval = this.processed - this.lastProcessed;
        const rate = elapsedSec > 0 ? processedInInterval / elapsedSec : 0;
        this.lastProcessed = this.processed;
        this.lastReportTime = now;
        const mem = process.memoryUsage().rss / (1024 * 1024);
        processingRateGauge.set(rate);
        memoryGauge.set(mem);
        const integrityMetric = prom_client_1.default.register.getSingleMetric('relationship_integrity')?.get();
        const integrity = integrityMetric && 'values' in integrityMetric ? integrityMetric.values[0]?.value ?? null : null;
        // Disk usage for output dir
        try {
            const du = this.dirSize(this.outputDir) / (1024 * 1024);
            diskGauge.set(du);
        }
        catch { }
        // Approx network throughput (assume output file size increase ~ network)
        const bytesSent = this.processed * 100; // rough 100 bytes avg per record
        const deltaBytes = bytesSent - this.lastBytesSent;
        this.lastBytesSent = bytesSent;
        netGauge.set((deltaBytes / 1024));
        const payloadObj = { processed: this.processed, rate, memory: mem, integrity, errors: this.errors, disk: mem, timestamp: Date.now() };
        this.history.push(payloadObj);
        if (this.history.length > this.maxHistory)
            this.history.shift();
        const payload = JSON.stringify(payloadObj);
        this.wss?.clients.forEach((client) => {
            if (client.readyState === 1) {
                client.send(payload);
            }
        });
        console.log(`[Monitor] processed=${this.processed} rate=${rate.toFixed(1)}/s memory=${mem.toFixed(1)}MB load=${os_1.default.loadavg()[0].toFixed(2)}`);
        this.emit('metrics', payloadObj);
        this.checkAlerts(rate, integrity);
    }
    dirSize(path) {
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
    checkAlerts(rate, integrity) {
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
exports.RealTimeMonitoringSystem = RealTimeMonitoringSystem;
