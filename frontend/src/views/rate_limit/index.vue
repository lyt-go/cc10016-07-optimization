<template>
  <section class="page" data-module="rate_limit">
    <header class="page-head">
      <div>
        <h2>频次控制管理</h2>
        <p class="page-desc">维护请求频次、并发与队列控制策略；配额状态数据由令牌数与当日用量统一加工，刷新后仍与请求日志数对应。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记限流策略</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出频次控制清单</button>
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
          :data-quota-version="String(row._quota_version ?? '')"
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
              :data-testid="`refresh-quota-${row.id}`"
              :disabled="refreshingId === row.id"
              @click="refreshQuota(row)"
            >
              {{ refreshingId === row.id ? '刷新中…' : '刷新配额' }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + quotaColumns.length + 1" class="empty-state">暂无频次控制数据，可先登记限流策略</td>
        </tr>
      </tbody>
    </table>

    <p v-if="quotaMessage" class="page-foot" data-testid="quota-message">{{ quotaMessage }}</p>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记限流策略</h3>
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
      <span>共 {{ total }} 条频次控制记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/rate_limit'
const columns = ["策略编号", "目标源", "请求间隔", "并发上限", "日请求总量", "冷却时长", "队列上限", "优先级"]
const quotaColumns = ["令牌数", "当日已用配额", "请求日志数", "数据版本"]
const actions = ["提交校验", "标记生效", "停用策略"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["限流策略总数", "待校验策略", "已生效策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const quotaMessage = ref('')
const refreshingId = ref<number | null>(null)
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

async function refreshQuota(row: Row) {
  refreshingId.value = row.id as number
  errorMessage.value = ''
  quotaMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/refresh`, {
      method: 'POST',
      body: JSON.stringify({ expected_version: row._quota_version ?? null }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      const decision = payload.arbitration?.decision
      throw new Error(decision || payload.error || '配额状态刷新失败')
    }
    const state = payload.quota_state ?? {}
    quotaMessage.value = payload.rolled_back
      ? `配额状态已回滚（${(payload.issues ?? []).join('、')}）：令牌数 ${state['令牌数']}，当日已用配额 ${state['当日已用配额']}，请求日志数 ${state['请求日志数']}`
      : `配额状态已刷新：令牌数 ${state['令牌数']}，当日已用配额 ${state['当日已用配额']}，请求日志数 ${state['请求日志数']}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '配额状态刷新失败'
  } finally {
    refreshingId.value = null
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
    link.download = 'rate_limit.csv'
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
      throw new Error('频次控制动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '频次控制操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('限流策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '频次控制列表读取失败'
  }
}

onMounted(reload)
</script>
