<template>
  <div class="panel">
    <h4>
      🔔 告警
      <el-badge v-if="activeCount" :value="activeCount" class="badge" type="danger" />
      <el-select v-model="filter" size="small" class="filter" popper-class="alert-popper">
        <el-option value="all" label="全部" />
        <el-option value="active" label="待处理" />
        <el-option value="handled" label="已处理" />
        <el-option value="resolved" label="已解除" />
      </el-select>
    </h4>
    <div class="alert-list">
      <div
        v-for="a in filteredAlerts"
        :key="a.id"
        class="alert-row"
        :class="[a.status, a.severity]"
        @click="openDetail(a)"
      >
        <div class="a-main">
          <span class="a-title">
            <span class="dot" />
            {{ typeLabel(a.type) }}
            <el-tag v-if="a.count > 1" size="small" type="warning" effect="dark" class="cnt">
              ×{{ a.count }}
            </el-tag>
          </span>
          <span class="a-msg">{{ a.taskName ? a.taskName + '：' : '' }}{{ a.message }}</span>
          <span class="a-time">{{ formatTime(a.lastSeen) }}</span>
        </div>
        <el-tag size="small" :type="statusTag(a.status)" effect="plain">{{ statusLabel(a.status) }}</el-tag>
      </div>
      <div v-if="!filteredAlerts.length" class="empty">暂无告警</div>
    </div>

    <el-dialog v-model="dialogVisible" title="告警详情" width="420px" append-to-body>
      <template v-if="current">
        <el-descriptions :column="1" border size="small">
          <el-descriptions-item label="告警类型">{{ typeLabel(current.type) }}</el-descriptions-item>
          <el-descriptions-item label="级别">
            <el-tag size="small" :type="severityTag(current.severity)">{{ severityLabel(current.severity) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="环节">
            {{ current.taskName || (current.taskId || '—（整条流水线）') }}
          </el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag size="small" :type="statusTag(current.status)">{{ statusLabel(current.status) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="重复次数">{{ current.count }} 次（已收敛为一条）</el-descriptions-item>
          <el-descriptions-item label="首次发生">{{ formatTime(current.firstSeen) }}</el-descriptions-item>
          <el-descriptions-item label="最近发生">{{ formatTime(current.lastSeen) }}</el-descriptions-item>
          <el-descriptions-item label="原因说明">{{ current.message }}</el-descriptions-item>
          <el-descriptions-item v-if="current.handledAt" label="处置时间">{{ formatTime(current.handledAt) }}</el-descriptions-item>
          <el-descriptions-item v-if="current.handledBy" label="处置人">{{ current.handledBy }}</el-descriptions-item>
          <el-descriptions-item v-if="current.handleNote" label="处置备注">{{ current.handleNote }}</el-descriptions-item>
          <el-descriptions-item v-if="current.resolvedAt" label="解除时间">{{ formatTime(current.resolvedAt) }}</el-descriptions-item>
        </el-descriptions>

        <div v-if="current.status === 'active'" class="handle-box">
          <el-input
            v-model="note"
            type="textarea"
            :rows="2"
            placeholder="处置备注（可选），例如：已重启该节点/已联系值班人"
          />
        </div>
      </template>
      <template #footer>
        <span v-if="current">
          <el-button
            v-if="current.status === 'active'"
            type="primary"
            :loading="submitting"
            @click="submitHandle"
          >标记已处理</el-button>
          <el-button @click="dialogVisible = false">关闭</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { useDAGStore } from '../store/dag'
import type { AlertItem, AlertType, AlertStatus } from '../types'

const store = useDAGStore()
const filter = ref<AlertStatus | 'all'>('all')
const dialogVisible = ref(false)
const current = ref<AlertItem | null>(null)
const note = ref('')
const submitting = ref(false)

const filteredAlerts = computed(() =>
  filter.value === 'all' ? store.alerts : store.alerts.filter(a => a.status === filter.value),
)
const activeCount = computed(() => store.alerts.filter(a => a.status === 'active').length)

function openDetail(a: AlertItem) {
  // 直接使用 store 中与后端同步的数据，避免“点开后一直加载、点不动”
  current.value = a
  note.value = a.handleNote || ''
  dialogVisible.value = true
}

async function submitHandle() {
  if (!current.value) return
  submitting.value = true
  try {
    await store.handleAlert(current.value.id, note.value.trim())
    ElMessage.success('告警处置结果已同步到后端')
    dialogVisible.value = false
  } catch {
    ElMessage.error('处置失败，请重试')
  } finally {
    submitting.value = false
  }
}

function typeLabel(t: AlertType) {
  return ({
    TASK_FAILED: '节点失败',
    TASK_TIMEOUT: '节点卡死/超时',
    CIRCUIT_OPEN: '熔断阻塞',
    PIPELINE_STALL: '流水线停滞',
  } as const)[t] || t
}
function statusLabel(s: AlertStatus) {
  return ({ active: '待处理', handled: '已处理', resolved: '已解除' } as const)[s] || s
}
function statusTag(s: AlertStatus): 'danger' | 'success' | 'info' {
  return ({ active: 'danger', handled: 'success', resolved: 'info' } as const)[s] || 'info'
}
function severityLabel(s: string) { return s === 'error' ? '严重' : '警告' }
function severityTag(s: string): 'danger' | 'warning' { return s === 'error' ? 'danger' : 'warning' }

function formatTime(ts: number | null | undefined) {
  if (!ts) return '—'
  const d = new Date(ts * 1000)
  const p = (n: number) => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

onMounted(() => { store.fetchAlerts() })
</script>

<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#fbbf24;font-size:12px;margin-bottom:6px;display:flex;align-items:center;gap:6px}
.badge{margin-right:auto}
.filter{width:96px}
.alert-list{max-height:260px;overflow-y:auto;font-size:11px}
.alert-row{display:flex;justify-content:space-between;gap:6px;padding:6px;border-radius:5px;margin:3px 0;
  border:1px solid #2a2a4a;cursor:pointer;background:#14142b;transition:border-color .15s}
.alert-row:hover{border-color:#bb86fc}
.alert-row.active.error{border-left:3px solid #ef4444}
.alert-row.active.warning{border-left:3px solid #f59e0b}
.alert-row.handled{opacity:.7;border-left:3px solid #22c55e}
.alert-row.resolved{opacity:.45;border-left:3px solid #4a5568}
.a-main{display:flex;flex-direction:column;gap:2px;min-width:0}
.a-title{font-weight:700;color:#e0e0e0;display:flex;align-items:center;gap:4px}
.dot{width:7px;height:7px;border-radius:50%;background:#4a5568;display:inline-block}
.active.error .dot{background:#ef4444;box-shadow:0 0 6px #ef4444}
.active.warning .dot{background:#f59e0b;box-shadow:0 0 6px #f59e0b}
.cnt{margin-left:2px;transform:scale(.85)}
.a-msg{color:#aaa;line-height:1.4;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.a-time{color:#666;font-size:10px;font-family:monospace}
.empty{color:#4a5568;font-size:11px;text-align:center;padding:14px 0}
.handle-box{margin-top:12px}
</style>
