import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'
import type {
  DecimalDownstream,
  DecimalDryRun,
  DecimalExecution,
  DecimalLedger,
  DecimalMigrationRecord,
  DecimalResume,
  DecimalTarget,
  DecimalVerify,
} from '@/data/decimal-types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

async function decimalRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(`/api/decimal${path}`, init)
  if (!response.ok) {
    let detail = `接口返回 ${response.status}`
    try {
      const payload = await response.json()
      if (payload?.detail) detail = String(payload.detail)
    } catch {
      /* 保留默认提示 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export function fetchDecimalDryRun(strict: boolean): Promise<DecimalDryRun> {
  return decimalRequest('/dry-run', {
    method: 'POST',
    body: JSON.stringify({ strict_batch: strict }),
  })
}

export function executeDecimalMigration(columns?: string[]): Promise<DecimalExecution> {
  return decimalRequest('/execute', {
    method: 'POST',
    body: JSON.stringify(columns ? { columns } : {}),
  })
}

export function resumeDecimalMigration(): Promise<DecimalResume> {
  return decimalRequest('/resume', { method: 'POST', body: JSON.stringify({}) })
}

export function fetchDecimalRecords(): Promise<{ items: DecimalMigrationRecord[] }> {
  return decimalRequest('/records')
}

export function fetchDecimalDownstream(name: string): Promise<DecimalDownstream> {
  return decimalRequest(`/downstream?name=${encodeURIComponent(name)}`)
}

export function fetchDecimalTarget(): Promise<DecimalTarget> {
  return decimalRequest('/target')
}

export function fetchDecimalVerify(): Promise<DecimalVerify> {
  return decimalRequest('/verify')
}

export function fetchDecimalLedger(): Promise<DecimalLedger> {
  return decimalRequest('/column-changes')
}

export function resetDecimalMigration(): Promise<{ ok: boolean; message: string }> {
  return decimalRequest('/reset', { method: 'POST', body: '{}' })
}
