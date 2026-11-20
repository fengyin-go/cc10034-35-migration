/** 小数列（DECIMAL）精度迁移：试运行、执行、续做、下游查询、目标核对。 */

export type DecimalFailure = {
  row_id: number | string
  value: string
  failed_check: string
  reason: string
}

export type DecimalColumnReport = {
  column: string
  source_type: string
  legacy_target_type: string
  target_type: string
  mapping_kind: string
  definition_errors: string[]
  value_check: {
    total: number
    blocked: number
    ok: boolean
    reason: string
    failures: DecimalFailure[]
  }
  status: string
  downstreams: string[]
  record_id: string
  statement: string
}

export type DecimalDelivery = {
  mode: string
  deliverable_columns: string[]
  blocked_columns: string[]
  blocked_downstreams: string[]
  reason: string
}

export type DecimalDryRun = {
  batch_id: string
  batch_name: string
  source_table: string
  target_table: string
  release_version: number
  strict_batch: boolean
  generated_at: string
  columns: DecimalColumnReport[]
  delivery: DecimalDelivery
  conflict: {
    summary: string
    note: string
    strict_batch_alternative: DecimalDelivery
  }
  migration_records: DecimalMigrationRecord[]
}

export type DecimalMigrationRecord = {
  record_id: string
  column: string
  source_type: string
  legacy_target_type: string
  target_type: string
  mapping_kind: string
  status: string
  effective: boolean
  statement: string
  rollback_statement: string
  release_version: number
  value_check?: { total: number; blocked: number } | null
}

export type DecimalExecution = {
  ok: boolean
  batch_id: string
  release_version: number
  strict_batch: boolean
  executed: DecimalColumnResult[]
  skipped: DecimalColumnResult[]
  blocked: DecimalColumnResult[]
  results: DecimalColumnResult[]
  deliverable_columns: string[]
  target: DecimalTarget
}

export type DecimalColumnResult = {
  column: string
  status: string
  target_type: string
  current_type: string | null
  effective: boolean
  message: string
  statement?: string
  value_check: { total: number; blocked: number; failures: DecimalFailure[] }
}

export type DecimalTarget = {
  table: string
  columns: Record<string, string>
  rows: Record<string, string | number>[]
}

export type DecimalDownstream = {
  downstream: string
  reads: string[]
  sql: string
  mapping_status: Record<string, { target_type: string; source_type: string; effective: boolean }>
  row_count: number
  rows: Record<string, string | number>[]
}

export type DecimalVerify = {
  batch_id: string
  target_table: string
  checked_at: string
  all_fidelity_ok: boolean
  row_count: number
  columns: {
    column: string
    source_type: string
    declared_target_type: string
    types_match: boolean
    definition_errors: string[]
    value_check: { total: number; blocked: number; failures: DecimalFailure[] }
    conflicts: string[]
    fidelity_ok: boolean
  }[]
}

export type DecimalResume = {
  ok: boolean
  batch_id: string
  rechecked: {
    column: string
    mapping_version: number
    declared_type: string
    actual_type: string
    original_definition: string
    type_ok: boolean
    values_ok: boolean
    online: boolean
  }[]
  retried_columns: string[]
  skipped_columns: { column: string; status: string; reason: string; target_type: string }[]
  execution: DecimalExecution
}

export type DecimalLedger = Record<string, Record<string, {
  column: string
  source_definition: string
  original_precision: number | null
  original_scale: number | null
  mapping_type: string
  mapping_version: number
  effective: boolean
  execution_results: Record<string, unknown>[]
}>>
