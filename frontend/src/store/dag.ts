import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo, AlertItem, WsMessage } from '@/types'

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const alerts = ref<AlertItem[]>([])
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')

  let ws: WebSocket | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let closedByUs = false

  function mergeAlerts(list: AlertItem[]) {
    // 后端为告警唯一事实来源，整表替换，保证告警列表与后端同步
    alerts.value = list
    const inExecution = (execution.value?.alerts)
    if (inExecution) execution.value = { ...execution.value!, alerts: list }
  }

  function onMessage(raw: string) {
    let d: WsMessage
    try { d = JSON.parse(raw) } catch { return }
    if (d.type === 'alerts' && Array.isArray(d.alerts)) {
      mergeAlerts(d.alerts)
    } else if (d.workflow) {
      const { type: _t, alerts: a, ...exec } = d as ExecutionInfo & { alerts?: AlertItem[] }
      execution.value = exec as ExecutionInfo
      if (Array.isArray(a)) mergeAlerts(a)
    }
  }

  function connectWS() {
    closedByUs = false
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onmessage = (e) => onMessage(e.data)
    ws.onclose = () => {
      wsConnected.value = false
      ws = null
      // 断网/后端重启后自动重连，避免深夜静默断连后再也收不到告警
      if (!closedByUs) {
        reconnectTimer = setTimeout(connectWS, 3000)
      }
    }
    ws.onerror = () => { ws?.close() }
  }

  async function fetchAlerts() {
    const { data } = await axios.get<AlertItem[]>('/api/alerts')
    mergeAlerts(data)
    return data
  }

  async function handleAlert(id: number, note: string, handledBy = '值班人') {
    const { data } = await axios.post(`/api/alerts/${id}/handle`, { note, handledBy })
    mergeAlerts(alerts.value.map(a => a.id === id ? data : a))
  }

  async function resolveAlert(id: number) {
    const { data } = await axios.post(`/api/alerts/${id}/resolve`)
    mergeAlerts(alerts.value.map(a => a.id === id ? data : a))
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try {
      const { data } = await axios.post('/api/workflow', { name })
      workflow.value = data
    } finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post<ExecutionInfo & { alerts?: AlertItem[] }>(
        '/api/run',
        { workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value },
      )
      execution.value = data
      if (Array.isArray(data.alerts)) mergeAlerts(data.alerts)
    } finally { loading.value = false }
  }

  function disconnectWS() {
    closedByUs = true
    if (reconnectTimer) clearTimeout(reconnectTimer)
    ws?.close()
    ws = null
  }

  return {
    loading, workflow, execution, alerts, wsConnected, workers, strategy,
    connectWS, fetchAlerts, handleAlert, resolveAlert,
    createWorkflow, run, disconnectWS,
  }
})
