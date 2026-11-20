<template>
  <section class="decimal-panel" data-testid="decimal-panel">
    <header class="decimal-head">
      <div>
        <h3>小数列精度迁移（DECIMAL → 保留源精度）</h3>
        <p class="page-desc">
          旧版映射把 DECIMAL 固定成 DECIMAL(38,18)。这里按 V2 解析源精度、逐列检查整数位与小数位，
          试运行后只执行能无损迁移的列。批次：<code>{{ dry?.batch_id ?? '…' }}</code>
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="decimal-dry-run" type="button" @click="runDryRun">
          {{ loading ? '处理中…' : '试运行整批' }}
        </button>
        <button class="btn primary" data-testid="decimal-execute" type="button" @click="execute">
          执行可迁移列
        </button>
        <button class="btn" data-testid="decimal-resume" type="button" @click="resume">
          按实际失败范围续做
        </button>
        <button class="btn ghost" data-testid="decimal-query-fee" type="button" @click="queryFee">
          下游查询 fee
        </button>
        <button class="btn ghost" data-testid="decimal-reset" type="button" @click="resetAll">重置</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="decimal-error">{{ errorMessage }}</p>

    <template v-if="dry">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">切换模式</span>
          <strong class="stat-value small">{{ dry.delivery.mode }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">本次可提供的新版列</span>
          <strong class="stat-value small" data-testid="decimal-deliverable">
            {{ dry.delivery.deliverable_columns.join('、') || '无' }}
          </strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">阻塞列</span>
          <strong class="stat-value small" data-testid="decimal-blocked">
            {{ dry.delivery.blocked_columns.join('、') || '无' }}
          </strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">受影响下游</span>
          <strong class="stat-value small">{{ dry.delivery.blocked_downstreams.join('、') || '无' }}</strong>
        </article>
      </div>

      <p class="decimal-note" data-testid="decimal-conflict">{{ dry.conflict.summary }}。{{ dry.conflict.note }}。</p>

      <h4>列检查结果（先整数位，再小数位）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>列</th><th>源精度</th><th>旧映射</th><th>目标类型</th>
            <th>整数位</th><th>小数位</th><th>状态</th><th>下游</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="column in dry.columns" :key="column.column" :data-testid="`decimal-column-${column.column}`">
            <td>{{ column.column }}</td>
            <td>{{ column.source_type }}</td>
            <td>{{ column.legacy_target_type }}</td>
            <td>{{ column.target_type }}</td>
            <td>{{ integerLimit(column.target_type) }}</td>
            <td>{{ scaleOf(column.target_type) }}</td>
            <td :data-status="column.status">{{ column.status }}</td>
            <td>{{ column.downstreams.join('、') }}</td>
          </tr>
        </tbody>
      </table>

      <div v-for="column in blockedColumns" :key="`fail-${column.column}`" class="decimal-failures">
        <h4>{{ column.column }}：{{ column.value_check.blocked }} 条存量值装不进 {{ column.target_type }}</h4>
        <table class="data-table">
          <thead>
            <tr><th>行</th><th>存量值</th><th>失败检查</th><th>原因</th></tr>
          </thead>
          <tbody>
            <tr v-for="failure in column.value_check.failures" :key="`${column.column}-${failure.row_id}`">
              <td>{{ failure.row_id }}</td>
              <td>{{ failure.value }}</td>
              <td>{{ failure.failed_check }}</td>
              <td>{{ failure.reason }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <details class="decimal-records" open>
        <summary>新版小数列迁移记录（{{ records.length }} 条）</summary>
        <table class="data-table">
          <thead>
            <tr><th>记录ID</th><th>列</th><th>源 → 目标</th><th>类型</th><th>状态</th><th>目标语句</th></tr>
          </thead>
          <tbody>
            <tr v-for="record in records" :key="record.record_id" :data-testid="`decimal-record-${record.column}`">
              <td>{{ record.record_id }}</td>
              <td>{{ record.column }}</td>
              <td>{{ record.source_type }} → {{ record.target_type }}</td>
              <td>{{ record.mapping_kind }}</td>
              <td :data-status="record.status">{{ record.status }}</td>
              <td><code>{{ record.statement }}</code></td>
            </tr>
          </tbody>
        </table>
      </details>
    </template>

    <section v-if="target" class="decimal-sub">
      <h4>目标数据表列定义</h4>
      <table class="data-table">
        <thead><tr><th>列</th><th>当前类型</th></tr></thead>
        <tbody>
          <tr v-for="(type, name) in target.columns" :key="String(name)">
            <td>{{ name }}</td>
            <td :data-testid="`decimal-type-${name}`">{{ type }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="feeQuery" class="decimal-sub" data-testid="decimal-fee-query">
      <h4>下游 {{ feeQuery.downstream }} 查询结果（{{ feeQuery.sql }}）</h4>
      <p class="page-desc">
        {{ feeQuery.reads.join('、') }} 当前映射：
        <code v-for="(status, name) in feeQuery.mapping_status" :key="String(name)">
          {{ name }} = {{ status.target_type }}（{{ status.effective ? '已生效' : '未生效' }}）
        </code>
      </p>
      <table class="data-table">
        <thead><tr><th>id</th><th v-for="read in feeQuery.reads" :key="read">{{ read }}</th></tr></thead>
        <tbody>
          <tr v-for="row in feeQuery.rows" :key="String(row.id)">
            <td>{{ row.id }}</td>
            <td v-for="read in feeQuery.reads" :key="read">{{ row[read] }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="verify" class="decimal-sub" data-testid="decimal-verify">
      <h4>目标表核对：{{ verify.all_fidelity_ok ? '全部保真' : '存在冲突' }}</h4>
      <table class="data-table">
        <thead>
          <tr><th>列</th><th>源类型</th><th>目标类型</th><th>类型一致</th><th>存量值</th><th>冲突</th></tr>
        </thead>
        <tbody>
          <tr v-for="column in verify.columns" :key="column.column" :data-testid="`decimal-verify-${column.column}`">
            <td>{{ column.column }}</td>
            <td>{{ column.source_type }}</td>
            <td>{{ column.declared_target_type }}</td>
            <td>{{ column.types_match ? '是' : '否' }}</td>
            <td>{{ column.value_check.total }} 条 / 阻塞 {{ column.value_check.blocked }}</td>
            <td>{{ column.conflicts.join('；') || '无' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="lastExecution" class="decimal-sub" data-testid="decimal-execution">
      <h4>执行结果</h4>
      <ul class="decimal-list">
        <li v-for="result in lastExecution.results" :key="result.column" :data-status="result.status">
          {{ result.column }}：{{ result.status }} — {{ result.message }}
        </li>
      </ul>
    </section>

    <section v-if="lastResume" class="decimal-sub" data-testid="decimal-resume-result">
      <h4>续做：只重试未生效的列</h4>
      <p class="page-desc">重试列：{{ lastResume.retried_columns.join('、') || '无' }}；已跳过：{{ lastResume.skipped_columns.map((item) => item.column).join('、') || '无' }}</p>
      <table class="data-table">
        <thead><tr><th>列</th><th>复查版本</th><th>记录类型</th><th>实际类型</th><th>在线</th></tr></thead>
        <tbody>
          <tr v-for="check in lastResume.rechecked" :key="check.column">
            <td>{{ check.column }}</td>
            <td>v{{ check.mapping_version }}</td>
            <td>{{ check.declared_type }}</td>
            <td>{{ check.actual_type }}</td>
            <td>{{ check.online ? '是' : '否' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="ledgerColumns.length" class="decimal-sub">
      <h4>列变更清单（下一次部署查原定义）</h4>
      <table class="data-table">
        <thead><tr><th>列</th><th>原始定义</th><th>当前映射</th><th>版本</th><th>最近结果</th></tr></thead>
        <tbody>
          <tr v-for="entry in ledgerColumns" :key="entry.column">
            <td>{{ entry.column }}</td>
            <td><code>{{ entry.source_definition }}</code></td>
            <td>{{ entry.mapping_type }}</td>
            <td>v{{ entry.mapping_version }}</td>
            <td>{{ entry.lastStatus }}</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  executeDecimalMigration,
  fetchDecimalDownstream,
  fetchDecimalDryRun,
  fetchDecimalLedger,
  fetchDecimalRecords,
  fetchDecimalTarget,
  fetchDecimalVerify,
  resetDecimalMigration,
  resumeDecimalMigration,
} from '@/api/backend-service'
import type {
  DecimalDownstream,
  DecimalDryRun,
  DecimalExecution,
  DecimalMigrationRecord,
  DecimalResume,
  DecimalTarget,
  DecimalVerify,
} from '@/data/decimal-types'

const dry = ref<DecimalDryRun | null>(null)
const records = ref<DecimalMigrationRecord[]>([])
const target = ref<DecimalTarget | null>(null)
const verify = ref<DecimalVerify | null>(null)
const feeQuery = ref<DecimalDownstream | null>(null)
const lastExecution = ref<DecimalExecution | null>(null)
const lastResume = ref<DecimalResume | null>(null)
const ledger = ref<Record<string, Record<string, any>>>({})
const loading = ref(false)
const errorMessage = ref('')

const blockedColumns = computed(() =>
  (dry.value?.columns ?? []).filter((column) => column.value_check.blocked > 0),
)

const ledgerColumns = computed(() => {
  const tables = Object.values(ledger.value)
  const merged = Object.assign({}, ...tables)
  return Object.values(merged).map((entry: any) => ({
    ...entry,
    lastStatus: entry.execution_results?.length
      ? `${entry.execution_results[entry.execution_results.length - 1].status}（v${entry.execution_results[entry.execution_results.length - 1].version}）`
      : '—',
  }))
})

function integerLimit(type: string): string {
  const match = /DECIMAL\((\d+)\s*,\s*(\d+)\)/i.exec(type)
  return match ? String(Number(match[1]) - Number(match[2])) : '—'
}

function scaleOf(type: string): string {
  const match = /DECIMAL\((\d+)\s*,\s*(\d+)\)/i.exec(type)
  return match ? match[2] : '—'
}

async function refreshState() {
  const [targetPayload, verifyPayload, recordsPayload, ledgerPayload] = await Promise.all([
    fetchDecimalTarget(),
    fetchDecimalVerify(),
    fetchDecimalRecords(),
    fetchDecimalLedger(),
  ])
  target.value = targetPayload
  verify.value = verifyPayload
  records.value = recordsPayload.items
  ledger.value = ledgerPayload
}

async function runDryRun() {
  await withLoading(async () => {
    dry.value = await fetchDecimalDryRun(false)
    await refreshState()
  })
}

async function execute() {
  await withLoading(async () => {
    lastExecution.value = await executeDecimalMigration()
    await runDryRun()
    if (feeQuery.value) await queryFee()
  })
}

async function resume() {
  await withLoading(async () => {
    lastResume.value = await resumeDecimalMigration()
    await runDryRun()
  })
}

async function queryFee() {
  await withLoading(async () => {
    feeQuery.value = await fetchDecimalDownstream('dwd_order_fee_view')
  })
}

async function resetAll() {
  await withLoading(async () => {
    await resetDecimalMigration()
    feeQuery.value = null
    lastExecution.value = null
    lastResume.value = null
    dry.value = await fetchDecimalDryRun(false)
    await refreshState()
  })
}

async function withLoading(task: () => Promise<void>) {
  loading.value = true
  errorMessage.value = ''
  try {
    await task()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迁移操作失败'
  } finally {
    loading.value = false
  }
}

onMounted(() => withLoading(async () => {
  dry.value = await fetchDecimalDryRun(false)
  await refreshState()
}))
</script>
