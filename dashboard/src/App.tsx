import { useCallback, useEffect, useState } from 'react'
import {
  clearBlocklist,
  fetchActivity,
  fetchIngestionOverview,
  fetchServiceHealth,
  fetchTelemetryStats,
  formatTime,
  routeNumber,
  shortId,
} from './api'
import type {
  ActivityFeed,
  IngestionOverview,
  RouteResponse,
  ServiceStatus,
  TelemetryStats,
} from './types'

const API_KEY = 'local-upload-key'

function carrierLabel(id: string): string {
  const names: Record<string, string> = {
    nexatel: 'Nexatel',
    clearpath: 'Clearpath',
    zenith: 'Zenith',
  }
  return names[id] ?? id
}

export default function App() {
  const [services, setServices] = useState<ServiceStatus[]>([])
  const [telemetry, setTelemetry] = useState<TelemetryStats | null>(null)
  const [activity, setActivity] = useState<ActivityFeed | null>(null)
  const [overview, setOverview] = useState<IngestionOverview | null>(null)
  const [routeResult, setRouteResult] = useState<RouteResponse | null>(null)
  const [dialed, setDialed] = useState('44207123456')
  const [routeError, setRouteError] = useState('')
  const [routing, setRouting] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [lastRefresh, setLastRefresh] = useState<string | null>(null)
  const [notice, setNotice] = useState('')

  const refresh = useCallback(async (manual = false) => {
    setRefreshing(true)
    if (manual) setNotice('')

    try { setServices(await fetchServiceHealth()) } catch { setServices([]) }
    try { setActivity(await fetchActivity()) } catch { setActivity(null) }
    try { setTelemetry(await fetchTelemetryStats()) } catch { setTelemetry(null) }
    try { setOverview(await fetchIngestionOverview()) } catch { setOverview(null) }

    setLastRefresh(new Date().toLocaleTimeString())
    if (manual) setNotice('Updated')
    setRefreshing(false)
  }, [])

  useEffect(() => {
    refresh()
    const id = setInterval(() => refresh(), 8000)
    return () => clearInterval(id)
  }, [refresh])

  async function handleRoute(e: React.FormEvent) {
    e.preventDefault()
    setRouting(true)
    setRouteError('')
    setRouteResult(null)
    try {
      setRouteResult(await routeNumber(dialed))
    } catch {
      setRouteError('Routing request failed')
    } finally {
      setRouting(false)
    }
  }

  async function handleUnblock(carrierId: string) {
    try {
      await clearBlocklist(carrierId, API_KEY)
      setNotice(`Unblocked ${carrierLabel(carrierId)}`)
      refresh(true)
    } catch {
      setNotice(`Could not unblock ${carrierLabel(carrierId)}`)
    }
  }

  const summary = activity?.summary
  const hasCalls = (summary?.total_calls ?? 0) > 0
  const upCount = services.filter((s) => s.ok).length

  return (
    <div className="app">
      <header>
        <div>
          <h1>LCR Console</h1>
          <p className="tagline">Routing, quality, and CDR activity</p>
        </div>
        <button type="button" className="refresh-btn" onClick={() => refresh(true)} disabled={refreshing}>
          {refreshing ? 'Updating…' : 'Refresh'}
        </button>
      </header>

      {notice && <div className="notice">{notice}</div>}
      {lastRefresh && <p className="meta">{upCount}/4 services up · checked {lastRefresh}</p>}

      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-num">{summary?.total_calls ?? 0}</div>
          <div className="stat-lbl">CDRs received</div>
        </div>
        <div className="stat-card">
          <div className="stat-num">{hasCalls ? `${Math.round(summary!.answer_rate * 100)}%` : '—'}</div>
          <div className="stat-lbl">Answer rate</div>
        </div>
        <div className="stat-card">
          <div className="stat-num">{overview?.active_rates ?? '—'}</div>
          <div className="stat-lbl">Loaded routes</div>
        </div>
        <div className="stat-card">
          <div className="stat-num">{telemetry?.blocklist_count ?? 0}</div>
          <div className="stat-lbl">Blocked</div>
        </div>
      </div>

      <section className="card">
        <h2>Route lookup</h2>
        <form className="route-form" onSubmit={handleRoute}>
          <input value={dialed} onChange={(e) => setDialed(e.target.value)} placeholder="44207123456" />
          <button type="submit" disabled={routing}>{routing ? '…' : 'Route'}</button>
        </form>
        {routeError && <p className="error">{routeError}</p>}
        {routeResult && (
          <div className="route-result">
            <p>Prefix <strong>{routeResult.matchedPrefix}</strong></p>
            {routeResult.candidates.map((c) => (
              <div key={c.rank} className="winner">
                <span>{carrierLabel(c.carrierId)} · ${c.effectiveCost.toFixed(4)}/min</span>
                <span className="rank">#{c.rank}</span>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="card">
        <h2>Recent CDRs {hasCalls && <span className="muted">({summary!.total_calls})</span>}</h2>
        {!hasCalls ? (
          <p className="empty">No CDRs yet. Run <code>make simulate</code> to push test traffic.</p>
        ) : (
          <>
            <p className="flow">simulator → routing → carrier → kafka → telemetry → clickhouse</p>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Number</th>
                    <th>Carrier</th>
                    <th>Outcome</th>
                    <th>Duration</th>
                    <th>Cost</th>
                  </tr>
                </thead>
                <tbody>
                  {activity!.recent_calls.map((c) => (
                    <tr key={c.call_id}>
                      <td className="mono">{formatTime(c.received_at)}</td>
                      <td className="mono">{c.dialed_number}</td>
                      <td>{carrierLabel(c.carrier_id)}</td>
                      <td><span className={`tag ${c.answered ? 'good' : 'bad'}`}>{c.answered ? 'Answered' : 'Failed'}</span></td>
                      <td>{c.answered ? `${c.duration_sec}s` : '—'}</td>
                      <td className="mono">${c.cost_theoretical.toFixed(4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="meta">Last 25 · {shortId(activity!.recent_calls[0]?.call_id ?? '')}</p>
          </>
        )}
      </section>

      <section className="card">
        <h2>Carriers</h2>
        {!telemetry?.carriers?.length ? (
          <p className="empty">No stats until CDRs arrive.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Carrier</th>
                <th>Calls</th>
                <th>ASR</th>
                <th>State</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {telemetry.carriers.map((c) => (
                <tr key={c.carrier_id}>
                  <td>{carrierLabel(c.carrier_id)}</td>
                  <td>{c.attempts}</td>
                  <td>{c.attempts > 0 ? `${(c.asr * 100).toFixed(0)}%` : '—'}</td>
                  <td>
                    <span className={`tag ${c.blocklisted ? 'bad' : 'good'}`}>
                      {c.blocklisted ? 'Blocked' : 'Active'}
                    </span>
                  </td>
                  <td>
                    {c.blocklisted && (
                      <button type="button" className="link-btn" onClick={() => handleUnblock(c.carrier_id)}>
                        Unblock
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="card compact">
        <h2>Services</h2>
        <div className="status-pills">
          {(services.length ? services : [
            { name: 'Ingestion', ok: false },
            { name: 'Routing', ok: false },
            { name: 'Telemetry', ok: false },
            { name: 'Mock carrier', ok: false },
          ]).map((s) => (
            <span key={s.name} className={`pill ${s.ok ? 'ok' : 'down'}`}>
              {s.ok ? '●' : '○'} {s.name}
            </span>
          ))}
        </div>
      </section>
    </div>
  )
}
