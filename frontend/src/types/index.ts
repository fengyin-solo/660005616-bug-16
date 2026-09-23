export type AlertSeverity = 'critical' | 'warning' | 'info'
export type AlertStatus = 'FIRING' | 'ACKNOWLEDGED' | 'RESOLVED'
export type AlertType =
  | 'TASK_STUCK'
  | 'TASK_FAILED'
  | 'RETRIES_EXHAUSTED'
  | 'CIRCUIT_OPEN'
  | 'WORKFLOW_STALLED'
  | 'WORKFLOW_FAILED'
  | 'WORKFLOW_SUCCEEDED'

export interface Alert {
  id: string
  fingerprint: string
  type: AlertType | string
  severity: AlertSeverity | string
  taskId?: string | null
  title: string
  message: string
  detail: Record<string, any>
  status: AlertStatus
  occurrenceCount: number
  firstSeen: number
  lastSeen: number
  updatedAt: number
  handledBy?: string | null
  handledAt?: number | null
  handleComment?: string | null
}

export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }
export interface ExecutionInfo {
  workflow?: DAGWorkflow | null
  logs: ExecutionLog[]
  circuitBreakers: CircuitBreaker[]
  alerts?: Alert[]
  completed: boolean
  success?: boolean
  runId?: number
}
