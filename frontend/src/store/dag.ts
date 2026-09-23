import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import { ElNotification } from 'element-plus'
import type { DAGWorkflow, ExecutionInfo, Alert } from '@/types'

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const alerts = ref<Alert[]>([])
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')

  let ws: WebSocket | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let closedByUs = false
  // 已弹过提醒的告警 id：同一告警反复推送只提醒一次，收敛为一条
  const notified = new Set<string>()

  function notifyNewFiring(list: Alert[]) {
    for (const a of list) {
      if (a.status !== 'FIRING' || notified.has(a.id)) continue
      notified.add(a.id)
      if (a.severity === 'info') continue
      ElNotification({
        title: a.title,
        message: a.message,
        type: a.severity === 'critical' ? 'error' : 'warning',
        duration: 8000,
      })
    }
  }

  function applyState(d: any) {
    execution.value = d as ExecutionInfo
    if (Array.isArray(d.alerts)) {
      // 服务端为权威来源，整表替换，保证列表与后端同步
      alerts.value = d.alerts
      notifyNewFiring(d.alerts)
    }
  }

  function connectWS() {
    closedByUs = false
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => {
      wsConnected.value = true
      // 连接成功后服务端会立刻推送完整快照（执行状态 + 历史告警及处置结果）
    }
    ws.onmessage = (e) => {
      try { applyState(JSON.parse(e.data)) } catch { /* 忽略损坏报文 */ }
    }
    ws.onclose = () => {
      wsConnected.value = false
      ws = null
      // 自动重连，保证深夜断线/页面长开后仍能恢复告警推送
      if (!closedByUs) {
        reconnectTimer = setTimeout(connectWS, 2000)
      }
    }
    ws.onerror = () => { ws?.close() }
  }

  async function loadAlerts() {
    const { data } = await axios.get('/api/alerts')
    alerts.value = data
    return data
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try { const { data } = await axios.post('/api/workflow', { name }); workflow.value = data }
    finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post('/api/run', {
        workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value,
      })
      applyState(data)
    } finally { loading.value = false }
  }

  async function handleAlert(alert: Alert, action: 'acknowledge' | 'resolve', comment = '') {
    const operator = localStorage.getItem('alert.operator') || 'anonymous'
    const { data } = await axios.post(`/api/alerts/${alert.id}/handle`, { action, operator, comment })
    // 乐观之外以服务端返回为准；后端还会广播给所有打开的页面
    const idx = alerts.value.findIndex(a => a.id === alert.id)
    if (idx >= 0) alerts.value[idx] = data.alert
    return data.alert as Alert
  }

  function disconnectWS() {
    closedByUs = true
    if (reconnectTimer) clearTimeout(reconnectTimer)
    ws?.close(); ws = null
  }

  return {
    loading, workflow, execution, alerts, wsConnected, workers, strategy,
    connectWS, loadAlerts, createWorkflow, run, handleAlert, disconnectWS,
  }
})
