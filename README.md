# Advanced SQL-to-NDJSON Migration System

This repository contains an enterprise-grade data-migration engine that streams relational databases into optimised NDJSON.

## Core Modules
| Module | Path | Purpose |
|--------|------|---------|
| SchemaDetectionEngine | `src/core/SchemaDetectionEngine.ts` | Discovers tables, keys, relationships, hierarchies |
| HierarchicalExtractor | `src/core/HierarchicalExtractor.ts` | Builds recursive CTEs, streams hierarchical data |
| RelationshipProcessor | `src/core/RelationshipProcessor.ts` | Enriches records with FK / M2M meta, validates integrity |
| AdaptiveBatcher | `src/core/AdaptiveBatcher.ts` | Memory-aware batching with sibling grouping |
| DenormalizationEngine | `src/core/DenormalizationEngine.ts` | Flat / nested / hybrid output strategies |
| CircularReferenceHandler | `src/core/CircularReferenceHandler.ts` | Detects & resolves circular dependencies |
| RealTimeMonitoringSystem | `src/core/RealTimeMonitoringSystem.ts` | Prometheus + WebSocket metrics & alerts |
| SecurityComplianceFramework | `src/core/SecurityComplianceFramework.ts` | Field masking, audit logging, GDPR erase |
| ScalabilityManager | `src/core/ScalabilityManager.ts` | Estimates resources, triggers k8s autoscale |
| ResultValidator | `src/core/ResultValidator.ts` | Verifies row-count parity & relationship integrity |

Full API docs are generated via **TypeDoc**. Run:
```bash
npm run docs
```
and open `docs/index.html`.

## Running End-to-End Migration
```bash
# Example using MySQL
ts-node src/index.ts --db mysql --host localhost --user root --password secret --database sample
```

Output NDJSON files will be placed in `./output` and validation results in `validation_report.json`.

## Tests
```bash
npm test
```
Runs Jest unit tests located in `tests/`. 