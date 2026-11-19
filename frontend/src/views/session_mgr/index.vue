<template>
  <section class="page" data-module="session_mgr">
    <header class="page-head">
      <div>
        <h2>会话管理管理</h2>
        <p class="page-desc">维护采集会话的登录凭证、超时与状态轮转；记录请求时在请求日志写入前后核对频次控制下发的令牌与当日用量。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记采集会话</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出会话管理清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th v-for="column in quotaColumns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in rows"
          :key="String(row.id)"
          :data-testid="`row-${row.id}`"
          :data-status="String(row.status)"
        >
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td v-for="column in quotaColumns" :key="column" :data-testid="`quota-${column}-${row.id}`">
            {{ quotaCell(row, column) }}
          </td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`record-${row.id}`"
              :disabled="busyId === row.id"
              @click="recordRequest(row)"
            >
              {{ busyId === row.id ? '记录中…' : '记录请求' }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + quotaColumns.length + 1" class="empty-state">暂无会话管理数据，可先登记采集会话</td>
        </tr>
      </tbody>
    </table>

    <p v-if="quotaMessage" class="page-foot" data-testid="quota-message">{{ quotaMessage }}</p>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记采集会话</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条会话管理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/session_mgr'
const columns = ["会话编号", "关联源", "登录状态", "最后活动", "请求计数", "Cookie摘要", "UserAgent", "会话时长"]
const quotaColumns = ["令牌数", "当日已用配额", "请求日志数", "数据版本"]
const actions = ["标记活跃", "标记过期", "标记失效"]
const statuses = ["草稿", "活跃", "已过期", "已失效"]
const metricNames = ["采集会话总数", "活跃会话", "已过期会话"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const quotaMessage = ref('')
const busyId = ref<number | null>(null)
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

function quotaCell(row: Row, column: string): string | number {
  const state = (row._quota_state ?? {}) as Record<string, string | number>
  const value = state[column]
  return value === undefined || value === null ? '—' : value
}

async function recordRequest(row: Row) {
  busyId.value = row.id as number
  errorMessage.value = ''
  quotaMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/record`, {
      method: 'POST',
      body: JSON.stringify({ url: `/crawl/session/${row.id}`, latency: 120, order: 'log_first' }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) throw new Error(payload.error || '请求记录失败')
    const quota = payload.quota ?? {}
    const state = quota.state ?? {}
    const parts = [
      `${payload.request_log_id} 已记录`,
      quota.allowed ? '配额扣减成功' : `配额未放行（${quota.reason ?? '待补记'}）`,
      `令牌数 ${state.tokens ?? '—'}`,
      `当日已用配额 ${state.daily_used ?? '—'}`,
      `请求日志数 ${state.request_log_count ?? '—'}`,
    ]
    if (quota.consistent === false) parts.push('前后核对未通过')
    quotaMessage.value = parts.join('，')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '请求记录失败'
  } finally {
    busyId.value = null
  }
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'session_mgr.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('会话管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '会话管理操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('采集会话列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '会话管理列表读取失败'
  }
}

onMounted(reload)
</script>
