export interface WorkloadProfile {
  records: number;
  tables: number;
  relationshipComplexity: number; // 1-10
}

export interface ResourceNeeds {
  memory: number; // MB
  cpu: number; // cores
  storage: number; // MB
  network: number; // Mbps
}

export class ScalabilityManager {
  private threshold = {
    memoryPerMillion: 256, // MB
    cpuPerMillion: 0.5, // cores
    storagePerMillion: 200, // MB output
    networkPerMillion: 10 // Mbps
  };

  calculateResourceNeeds(workload: WorkloadProfile): ResourceNeeds {
    const factor = workload.records / 1_000_000;
    return {
      memory: Math.ceil(factor * this.threshold.memoryPerMillion),
      cpu: Math.ceil(factor * this.threshold.cpuPerMillion + workload.relationshipComplexity * 0.2),
      storage: Math.ceil(factor * this.threshold.storagePerMillion),
      network: Math.ceil(factor * this.threshold.networkPerMillion)
    };
  }

  needsScaling(needs: ResourceNeeds): boolean {
    const memLimit = 1500; // MB safe
    const cpuLimit = 4; // cores
    return needs.memory > memLimit || needs.cpu > cpuLimit;
  }

  async scaleResources(needs: ResourceNeeds) {
    console.log('[Scalability] Scaling resources to', needs);
    await this.autoscaleK8s(needs).catch((err) => console.error('[Scalability] autoscaleK8s failed', err));
  }

  /** Attempt to call kubectl to patch deployment resources */
  private async autoscaleK8s(needs: ResourceNeeds) {
    if (!process.env.KUBERNETES_SERVICE_HOST) return; // not running in cluster
    const { exec } = await import('child_process');
    const cpu = Math.ceil(needs.cpu);
    const memMi = `${needs.memory}Mi`;
    const cmd = `kubectl set resources deployment etl-worker --limits=cpu=${cpu},memory=${memMi} --requests=cpu=${cpu},memory=${memMi}`;
    return new Promise<void>((resolve, reject) => {
      exec(cmd, (err, stdout, stderr) => {
        if (err) return reject(err);
        console.log('[Scalability] kubectl output:', stdout || stderr);
        resolve();
      });
    });
  }

  async manageScalability(workload: WorkloadProfile) {
    const needs = this.calculateResourceNeeds(workload);
    if (this.needsScaling(needs)) {
      await this.scaleResources(needs);
    }
    return needs;
  }

  /** Accept metrics from RealTimeMonitoringSystem and decide if re-scaling is needed */
  async evaluateMetrics(metrics: { rate: number; memory: number; errors: number }) {
    // Simple heuristic: if memory > 80% of limit (1500MB) or processing rate < threshold
    const memLimit = 1500;
    if (metrics.memory > memLimit * 0.8 || metrics.rate < 100) {
      console.log('[Scalability] Triggering scale up based on metrics', metrics);
      await this.scaleResources({ memory: memLimit * 2, cpu: 8, storage: 0, network: 0 } as any);
    }
  }

  async autoscaleStub() {
    console.log('[Scalability] Invoking cloud auto-scaler (stub)');
  }
} 