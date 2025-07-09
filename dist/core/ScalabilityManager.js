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
Object.defineProperty(exports, "__esModule", { value: true });
exports.ScalabilityManager = void 0;
class ScalabilityManager {
    constructor() {
        this.threshold = {
            memoryPerMillion: 256, // MB
            cpuPerMillion: 0.5, // cores
            storagePerMillion: 200, // MB output
            networkPerMillion: 10 // Mbps
        };
    }
    calculateResourceNeeds(workload) {
        const factor = workload.records / 1000000;
        return {
            memory: Math.ceil(factor * this.threshold.memoryPerMillion),
            cpu: Math.ceil(factor * this.threshold.cpuPerMillion + workload.relationshipComplexity * 0.2),
            storage: Math.ceil(factor * this.threshold.storagePerMillion),
            network: Math.ceil(factor * this.threshold.networkPerMillion)
        };
    }
    needsScaling(needs) {
        const memLimit = 1500; // MB safe
        const cpuLimit = 4; // cores
        return needs.memory > memLimit || needs.cpu > cpuLimit;
    }
    async scaleResources(needs) {
        console.log('[Scalability] Scaling resources to', needs);
        await this.autoscaleK8s(needs).catch((err) => console.error('[Scalability] autoscaleK8s failed', err));
    }
    /** Attempt to call kubectl to patch deployment resources */
    async autoscaleK8s(needs) {
        if (!process.env.KUBERNETES_SERVICE_HOST)
            return; // not running in cluster
        const { exec } = await Promise.resolve().then(() => __importStar(require('child_process')));
        const cpu = Math.ceil(needs.cpu);
        const memMi = `${needs.memory}Mi`;
        const cmd = `kubectl set resources deployment etl-worker --limits=cpu=${cpu},memory=${memMi} --requests=cpu=${cpu},memory=${memMi}`;
        return new Promise((resolve, reject) => {
            exec(cmd, (err, stdout, stderr) => {
                if (err)
                    return reject(err);
                console.log('[Scalability] kubectl output:', stdout || stderr);
                resolve();
            });
        });
    }
    async manageScalability(workload) {
        const needs = this.calculateResourceNeeds(workload);
        if (this.needsScaling(needs)) {
            await this.scaleResources(needs);
        }
        return needs;
    }
    /** Accept metrics from RealTimeMonitoringSystem and decide if re-scaling is needed */
    async evaluateMetrics(metrics) {
        // Simple heuristic: if memory > 80% of limit (1500MB) or processing rate < threshold
        const memLimit = 1500;
        if (metrics.memory > memLimit * 0.8 || metrics.rate < 100) {
            console.log('[Scalability] Triggering scale up based on metrics', metrics);
            await this.scaleResources({ memory: memLimit * 2, cpu: 8, storage: 0, network: 0 });
        }
    }
    async autoscaleStub() {
        console.log('[Scalability] Invoking cloud auto-scaler (stub)');
    }
}
exports.ScalabilityManager = ScalabilityManager;
