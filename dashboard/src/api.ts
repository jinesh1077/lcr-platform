import type {
  ActivityFeed,
  IngestionOverview,
  RouteResponse,
  ServiceStatus,
  TelemetryStats,
} from './types'

const base = (path: string) => path

async function ping(name: string, url: string): Promise<ServiceStatus> {
  const start = performance.now()
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(3000) })
    return { name, url, ok: res.ok, latencyMs: Math.round(performance.now() - start) }
  } catch {
    return { name, url, ok: false, latencyMs: Math.round(performance.now() - start) }
  }
}

export async function fetchServiceHealth(): Promise<ServiceStatus[]> {
  return Promise.all([
    ping('Ingestion', base('/ingestion/health')),
    ping('Routing', base('/routing/health')),
    ping('Telemetry', base('/telemetry/health')),
    ping('Mock Carrier', base('/mock/health')),
  ])
}

export async function fetchTelemetryStats(): Promise<TelemetryStats> {
  const res = await fetch(base('/telemetry/api/stats'))
  if (!res.ok) throw new Error('telemetry stats failed')
  return res.json()
}

export async function fetchActivity(): Promise<ActivityFeed> {
  const res = await fetch(base('/telemetry/api/activity'))
  if (!res.ok) throw new Error('activity feed failed')
  return res.json()
}

export async function fetchIngestionOverview(): Promise<IngestionOverview> {
  const res = await fetch(base('/ingestion/api/overview'))
  if (!res.ok) throw new Error('ingestion overview failed')
  return res.json()
}

export async function routeNumber(number: string): Promise<RouteResponse> {
  const res = await fetch(base('/routing/route'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ dialedNumber: number, defaultRegion: 'GB' }),
  })
  if (!res.ok) throw new Error('routing failed')
  return res.json()
}

export async function clearBlocklist(carrierId: string, apiKey: string): Promise<void> {
  const res = await fetch(base(`/ingestion/admin/blocklist/${carrierId}`), {
    method: 'DELETE',
    headers: { 'X-API-Key': apiKey },
  })
  if (!res.ok) throw new Error('clear blocklist failed')
}

export function shortId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id
}

export function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString()
  } catch {
    return iso
  }
}
