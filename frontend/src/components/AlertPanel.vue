<template>
  <div class="panel alert-panel">
    <div class="ap-head">
      <h4>🚨 告警中心 <span v-if="unresolvedCount" class="badge">{{ unresolvedCount }}</span></h4>
      <el-select v-model="filter" size="small" class="ap-filter">
        <el-option value="active" label="未解决" />
        <el-option value="FIRING" label="告警中" />
        <el-option value="ACKNOWLEDGED" label="已确认" />
        <el-option value="RESOLVED" label="已解决" />
        <el-option value="all" label="全部历史" />
      </el-select>
    </div>

    <div v-loading="loading" class="ap-body">
      <div
        v-for="a in filtered"
        :key="a.id"
        class="alert-row"
        :class="[a.severity, a.status.toLowerCase()]"
        @click="openDetail(a)"
      >
        <div class="ar-line1">
          <span class="ar-sev-dot"></span>
          <span class="ar-title">{{ a.title }}</span>
          <el-tag size="small" :type="sevTag(a.severity)" effect="dark" disable-transitions>{{ sevText(a.severity) }}</el-tag>
          <span v-if="a.occurrenceCount > 1" class="ar-count">×{{ a.occurrenceCount }}</span>
        </div>
        <div class="ar-line2">
          <el-tag size="small" :type="statusTag(a.status)" effect="plain" disable-transitions>{{ statusText(a.status) }}</el-tag>
          <span v-if="a.taskId" class="ar-task">{{ a.taskId }}</span>
          <span class="ar-time">{{ fmtTime(a.lastSeen) }}</span>
        </div>
      </div>
      <div v-if="!loading && !filtered.length" class="empty">
        {{ filter === 'all' || filter === 'RESOLVED' ? '暂无历史告警' : '暂无未解决告警 🎉' }}
      </div>
    </div>

    <!-- 详情抽屉：数据来自已同步的本地状态，点击立即打开，不会“点不动/像没加载完” -->
    <el-drawer v-model="detailVisible" size="360px" title="告警详情" direction="rtl">
      <template v-if="current">
        <div class="d-section" v-if="current.taskId">
          <div class="d-label">环节</div>
          <div class="d-value mono">{{ current.taskId }}</div>
        </div>
        <div class="d-section">
          <div class="d-label">类型 / 级别</div>
          <div class="d-value">
            <el-tag size="small" :type="typeTag(current.type)" disable-transitions>{{ typeText(current.type) }}</el-tag>
            <el-tag size="small" :type="sevTag(current.severity)" effect="dark" class="ml6" disable-transitions>{{ sevText(current.severity) }}</el-tag>
          </div>
        </div>
        <div class="d-section">
          <div class="d-label">状态</div>
          <div class="d-value"><el-tag size="small" :type="statusTag(current.status)" disable-transitions>{{ statusText(current.status) }}</el-tag></div>
        </div>
        <div class="d-section">
          <div class="d-label">告警内容</div>
          <div class="d-value">{{ current.message }}</div>
        </div>
        <div class="d-section">
          <div class="d-label">出现 / 最近一次</div>
          <div class="d-value mono">{{ fmtTime(current.firstSeen) }} ｜ {{ fmtTime(current.lastSeen) }}</div>
          <div class="d-sub">已收敛合并 {{ current.occurrenceCount }} 次</div>
        </div>
        <div class="d-section" v-if="current.detail && Object.keys(current.detail).length">
          <div class="d-label">原因与现场</div>
          <pre class="d-json">{{ JSON.stringify(current.detail, null, 2) }}</pre>
        </div>
        <div class="d-section" v-if="current.status === 'RESOLVED' || current.status === 'ACKNOWLEDGED'">
          <div class="d-label">处置信息</div>
          <div class="d-value mono">
            {{ current.handledBy }} 于 {{ current.handledAt ? fmtTime(current.handledAt) : '-' }} {{ statusText(current.status) }}
          </div>
          <div v-if="current.handleComment" class="d-sub">备注：{{ current.handleComment }}</div>
        </div>

        <div class="d-actions">
          <el-input
            v-model="comment"
            type="textarea"
            :rows="2"
            size="small"
            placeholder="处置备注（可选）"
            :disabled="current.status === 'RESOLVED'"
          />
          <el-button
            v-if="current.status === 'FIRING'"
            type="warning"
            size="small"
            :loading="busy"
            @click="doHandle('acknowledge')"
          >确认已知晓</el-button>
          <el-button
            v-if="current.status !== 'RESOLVED'"
            type="success"
            size="small"
            :loading="busy"
            @click="doHandle('resolve')"
          >标记已解决</el-button>
        </div>
      </template>
    </el-drawer>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useDAGStore } from '../store/dag'
import type { Alert } from '../types'

const store = useDAGStore()
const filter = ref<'active' | 'FIRING' | 'ACKNOWLEDGED' | 'RESOLVED' | 'all'>('active')
const loading = ref(false)
const detailVisible = ref(false)
const current = ref<Alert | null>(null)
const comment = ref('')
const busy = ref(false)

const filtered = computed(() => {
  const list = [...store.alerts].sort((a, b) => b.lastSeen - a.lastSeen)
  if (filter.value === 'all') return list
  if (filter.value === 'active') return list.filter(a => a.status !== 'RESOLVED')
  return list.filter(a => a.status === filter.value)
})
const unresolvedCount = computed(() => store.alerts.filter(a => a.status !== 'RESOLVED').length)

// 抽屉打开期间持续跟随 WS/后端广播更新处置结果（多人同时处理也同步）
watch(detailVisible, (open) => {
  if (open && current.value) {
    const fresh = store.alerts.find(a => a.id === current.value!.id)
    if (fresh) current.value = fresh
  }
})
watch(() => store.alerts, (list) => {
  if (current.value) {
    const fresh = list.find(a => a.id === current.value!.id)
    if (fresh && detailVisible.value) current.value = fresh
  }
}, { deep: true })

function openDetail(a: Alert) {
  current.value = a
  comment.value = a.handleComment || ''
  detailVisible.value = true
}

async function doHandle(action: 'acknowledge' | 'resolve') {
  if (!current.value) return
  busy.value = true
  try {
    const updated = await store.handleAlert(current.value, action, comment.value)
    current.value = updated
    ElMessage.success(action === 'resolve' ? '已标记解决' : '已确认')
  } catch {
    ElMessage.error('处置失败，请检查后端服务')
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  // 页面（重新）打开即拉取历史告警与处置结果；WS 快照也会做同样的事
  loading.value = true
  try { await store.loadAlerts() }
  catch { ElMessage.error('告警历史加载失败，将通过实时连接重试') }
  finally { loading.value = false }
})

function fmtTime(ts?: number | null) {
  if (!ts) return '-'
  const d = new Date(ts * 1000)
  const p = (n: number) => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}
const sevText = (s: string) => ({ critical: '严重', warning: '警告', info: '信息' } as Record<string, string>)[s] || s
const sevTag = (s: string) => ({ critical: 'danger', warning: 'warning', info: 'info' } as Record<string, any>)[s] || 'info'
const statusText = (s: string) => ({ FIRING: '告警中', ACKNOWLEDGED: '已确认', RESOLVED: '已解决' } as Record<string, string>)[s] || s
function statusTag(s: string) {
  return ({ FIRING: 'danger', ACKNOWLEDGED: 'warning', RESOLVED: 'success' } as Record<string, any>)[s] || 'info'
}
const typeText = (t: string) => ({
  TASK_STUCK: '环节卡死',
  TASK_FAILED: '环节失败',
  RETRIES_EXHAUSTED: '重试耗尽',
  CIRCUIT_OPEN: '熔断打开',
  WORKFLOW_STALLED: '流水线卡死',
  WORKFLOW_FAILED: '流水线失败',
  WORKFLOW_SUCCEEDED: '运行成功',
} as Record<string, string>)[t] || t
function typeTag(t: string) {
  return t === 'WORKFLOW_SUCCEEDED' ? 'success' : (t.startsWith('WORKFLOW') ? 'danger' : 'warning')
}
</script>

<style scoped>
.alert-panel { display: flex; flex-direction: column; max-height: 42%; }
.ap-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
.ap-head h4 { color: #f87171; font-size: 12px; }
.badge { background:#ef4444; color:#fff; border-radius:10px; padding:0 6px; font-size:10px; margin-left:4px; }
.ap-filter { width: 104px; }
.ap-body { overflow-y: auto; min-height: 60px; }
.alert-row { padding: 5px 7px; border-radius: 5px; margin: 3px 0; cursor: pointer; border: 1px solid transparent; background:#14142b; transition: border-color .15s; }
.alert-row:hover { border-color: #bb86fc66; }
.alert-row.critical.firing { border-left: 3px solid #ef4444; }
.alert-row.warning.firing { border-left: 3px solid #fbbf24; }
.alert-row.acknowledged { opacity: .75; border-left: 3px solid #fbbf24; }
.alert-row.resolved { opacity: .45; border-left: 3px solid #22c55e; }
.ar-line1 { display:flex; align-items:center; gap:5px; font-size:11px; }
.ar-sev-dot { width:7px; height:7px; border-radius:50%; background:#4a5568; flex:none; }
.critical .ar-sev-dot { background:#ef4444; box-shadow:0 0 6px #ef4444; }
.warning .ar-sev-dot { background:#fbbf24; }
.info .ar-sev-dot { background:#60a5fa; }
.ar-title { flex:1; color:#e0e0e0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.ar-count { color:#fbbf24; font-size:10px; font-weight:700; }
.ar-line2 { display:flex; align-items:center; gap:6px; margin-top:3px; font-size:10px; color:#888; }
.ar-task { font-family:monospace; }
.ar-time { margin-left:auto; }
.empty { color:#4a5568; font-size:11px; text-align:center; padding:14px 0; }
.d-section { margin-bottom: 14px; }
.d-label { font-size: 11px; color: #888; margin-bottom: 4px; }
.d-value { font-size: 13px; color: #e0e0e0; line-height: 1.5; }
.d-sub { font-size: 11px; color: #999; margin-top: 3px; }
.mono { font-family: monospace; font-size: 11px; }
.ml6 { margin-left: 6px; }
.d-json { background:#0f0f23; border:1px solid #2a2a4a; border-radius:6px; padding:8px; font-size:11px; color:#9cdcfe; overflow-x:auto; white-space:pre-wrap; word-break:break-all; }
.d-actions { display:flex; flex-direction:column; gap:8px; border-top:1px solid #2a2a4a; padding-top:12px; }
:deep(.el-drawer) { background:#1a1a2e; color:#e0e0e0; }
:deep(.el-drawer__header) { color:#e0e0e0; margin-bottom:12px; }
</style>
