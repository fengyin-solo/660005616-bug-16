export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }

export type AlertType = 'TASK_FAILED' | 'TASK_TIMEOUT' | 'CIRCUIT_OPEN' | 'PIPELINE_STALL'
export type AlertStatus = 'active' | 'handled' | 'resolved'

export interface AlertItem {
  id: number
  runId: string
  taskId: string | null
  taskName: string | null
  type: AlertType
  severity: string
  title: string
  message: string
  status: AlertStatus
  count: number
  firstSeen: number
  lastSeen: number
  resolvedAt: number | null
  handledAt: number | null
  handledBy: string | null
  handleNote: string | null
}

export interface ExecutionInfo {
  type?: string
  runId?: string
  workflow: DAGWorkflow
  logs: ExecutionLog[]
  circuitBreakers: CircuitBreaker[]
  completed: boolean
  success?: boolean
  alerts?: AlertItem[]
}

export type WsMessage = {
  type?: string
  alerts?: AlertItem[]
} & Partial<ExecutionInfo>
