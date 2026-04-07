export interface ServiceStatus {
  name: string
  url: string
  ok: boolean
  latencyMs: number
}

export interface RouteCandidate {
  carrierId: string
  costPerMin: number
  effectiveCost: number
  healthPenalty: number
  rank: number
}

export interface RouteResponse {
  dialedNumber: string
  matchedPrefix: string
  candidates: RouteCandidate[]
}

export interface CarrierStats {
  carrier_id: string
  asr: number
  attempts: number
  answered: number
  avg_duration_sec: number
  health_penalty: number
  blocklisted: boolean
}

export interface TelemetryStats {
  carriers: CarrierStats[]
  blocklist_count: number
  asr_threshold: number
}

export interface IngestionOverview {
  trie_active_buffer: string
  blocklist: string[]
  active_rates: number
  carriers: string[]
}

export interface CallRecord {
  call_id: string
  dialed_number: string
  carrier_id: string
  answered: boolean
  duration_sec: number
  cost_theoretical: number
  disconnect_reason: string
  timestamp: string
  received_at: string
}

export interface ActivitySummary {
  total_calls: number
  answered_calls: number
  failed_calls: number
  answer_rate: number
  total_cost: number
  last_call_at?: string
}

export interface ActivityFeed {
  summary: ActivitySummary
  recent_calls: CallRecord[]
}
