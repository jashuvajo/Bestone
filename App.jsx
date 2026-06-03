import React, { useEffect, useMemo, useRef, useState } from "react";
import { motion } from "framer-motion";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { createChart } from "lightweight-charts";

const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws/dashboard";
const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

const moduleLabels = [
  "Execution HUD",
  "Heatmap Terminal",
  "Orderflow Analytics",
  "AI Matrix",
  "Greeks & IV",
  "Strategy Router",
  "Upstox Portfolio",
  "Risk Engine",
  "Telemetry",
  "AI Analytics",
  "Trade Journal",
  "Session Intelligence",
  "Backtesting",
  "Settings",
];

const panelClass = "rounded-xl border border-slate-800 bg-slate-900/70 backdrop-blur-md p-3";
const tiny = "text-[10px] uppercase tracking-wide text-slate-400";

const defaultPayload = {
  timestamp: 0,
  session: "CLOSED",
  runtime: {
    safe_mode: true,
    auto_trading_enabled: false,
    broker_state: "DISCONNECTED",
    broker_reason: "Awaiting feed",
    websocket_alive: false,
    stale_feed: true,
    api_latency_ms: 0,
    websocket_latency_ms: 0,
  },
  risk_config: {},
  portfolio: { funds: {}, positions: [], holdings: [], orders: [] },
  performance: {
    realized_pnl: 0,
    unrealized_pnl: 0,
    rejection_rate: 0,
    fill_drift: 0,
    execution_latency_ms: 0,
    exposure_pct: 0,
  },
  signals: { NIFTY: {}, SENSEX: {} },
  session_intelligence: { NIFTY: {}, SENSEX: {}, global: {} },
  snapshots: {},
  active_trades: [],
  recent_trades: [],
  heatmaps: {},
  backtesting: { total_records: 0, recent_replay: [] },
};

function Stat({ label, value, danger = false }) {
  return (
    <div className="rounded-md bg-slate-950/70 px-2 py-1">
      <div className={tiny}>{label}</div>
      <div className={`text-sm font-semibold ${danger ? "text-rose-400" : "text-cyan-300"}`}>{value}</div>
    </div>
  );
}

function useLwChart(containerRef, history) {
  useEffect(() => {
    if (!containerRef.current) return;
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 240,
      layout: { background: { color: "#020617" }, textColor: "#94a3b8" },
      rightPriceScale: { borderColor: "#334155" },
      timeScale: { borderColor: "#334155", timeVisible: true, secondsVisible: true },
      grid: { vertLines: { color: "#0f172a" }, horzLines: { color: "#0f172a" } },
    });
    const series = chart.addAreaSeries({
      lineColor: "#06b6d4",
      topColor: "rgba(6,182,212,0.25)",
      bottomColor: "rgba(6,182,212,0.0)",
      lineWidth: 2,
    });
    series.setData(history);
    const resize = () => {
      if (!containerRef.current) return;
      chart.applyOptions({ width: containerRef.current.clientWidth });
    };
    window.addEventListener("resize", resize);
    return () => {
      window.removeEventListener("resize", resize);
      chart.remove();
    };
  }, [containerRef, history]);
}

export default function App() {
  const [payload, setPayload] = useState(defaultPayload);
  const [selectedSymbol, setSelectedSymbol] = useState("NIFTY");
  const [connected, setConnected] = useState(false);
  const [priceSeries, setPriceSeries] = useState([]);
  const [aiHistory, setAiHistory] = useState([]);
  const [riskEdit, setRiskEdit] = useState({
    trading_capital: 250000,
    max_exposure_pct: 0.35,
    ai_threshold: 65,
    aggression_level: 1,
  });
  const [settingsEdit, setSettingsEdit] = useState({
    max_latency_ms: 800,
    stale_data_seconds: 4,
    slippage_kill_switch_points: 1.5,
    max_symbol_concentration_pct: 0.7,
  });
  const [replayCursor, setReplayCursor] = useState(0);
  const chartRef = useRef(null);

  const snap = payload.snapshots?.[selectedSymbol] || {};
  const signal = payload.signals?.[selectedSymbol] || {};
  const heat = payload.heatmaps?.[selectedSymbol] || {};
  const perf = payload.performance || {};
  const runtime = payload.runtime || {};
  const sessionIntel = payload.session_intelligence?.[selectedSymbol] || {};

  useLwChart(chartRef, priceSeries);

  useEffect(() => {
    let ws;
    let alive = true;
    const connect = () => {
      ws = new WebSocket(WS_URL);
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (alive) setTimeout(connect, 1500);
      };
      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        setPayload(data);

        const now = Math.floor(data.timestamp || Date.now() / 1000);
        const px = data.snapshots?.[selectedSymbol]?.spot_ltp;
        if (typeof px === "number" && Number.isFinite(px)) {
          setPriceSeries((old) => [...old.slice(-450), { time: now, value: px }]);
        }

        const aiConf = data.signals?.[selectedSymbol]?.ai?.ai_confidence || 0;
        const tqs = data.signals?.[selectedSymbol]?.tqs || 0;
        setAiHistory((old) => [...old.slice(-300), { t: now, ai: aiConf, tqs }]);
      };
    };
    connect();
    return () => {
      alive = false;
      if (ws) ws.close();
    };
  }, [selectedSymbol]);

  const sessionBadge = useMemo(() => {
    const m = {
      PREMARKET: "bg-indigo-700/50 text-indigo-200",
      LIVE: "bg-emerald-700/50 text-emerald-200",
      POSTMARKET: "bg-amber-700/50 text-amber-200",
      CLOSED: "bg-slate-700/60 text-slate-200",
    };
    return m[payload.session] || m.CLOSED;
  }, [payload.session]);

  const micro = signal.microstructure || {};
  const feature = signal.features || {};
  const quality = {
    rejection: Number(perf.rejection_rate || 0) * 100,
    drift: Number(perf.fill_drift || 0),
    latency: Number(perf.execution_latency_ms || 0),
  };

  const heatRows = useMemo(() => {
    if (!heat?.strikes) return [];
    return heat.strikes.slice(-30).map((strike, idx) => ({
      strike,
      bid: Number(heat.liquidity_walls_bid?.[idx] || 0),
      ask: Number(heat.liquidity_walls_ask?.[idx] || 0),
      gamma: Number(heat.gamma_walls?.[idx] || 0),
      delta: Number(heat.delta_heat?.[idx] || 0),
    }));
  }, [heat]);

  const orderflowSeries = useMemo(() => {
    return [
      { name: "Momentum", v: (feature.momentum_score || 0) * 100 },
      { name: "Delta Vel", v: (feature.delta_velocity || 0) * 100000 },
      { name: "Agg Delta", v: (feature.aggressive_delta || 0) * 100 },
      { name: "Volume Acc", v: (feature.volume_acceleration || 0) * 100 },
      { name: "Spread Q", v: (feature.spread_quality || 0) * 100 },
      { name: "Gamma", v: (feature.gamma_bias || 0) * 100 },
    ];
  }, [feature]);

  const aiMatrixSeries = useMemo(() => {
    const ai = signal.ai || {};
    return [
      { k: "AI", v: ai.ai_confidence || 0 },
      { k: "Model", v: ai.model_confidence || 0 },
      { k: "Bayes", v: ai.bayesian_confidence || 0 },
      { k: "TQS", v: signal.tqs || 0 },
    ];
  }, [signal]);

  const greekSeries = useMemo(() => {
    const deltas = heat.delta_heat || [];
    const gammas = heat.gamma_walls || [];
    const avgDelta = deltas.length ? deltas.reduce((a, b) => a + b, 0) / deltas.length : 0;
    const avgGamma = gammas.length ? gammas.reduce((a, b) => a + b, 0) / gammas.length : 0;
    return [
      { k: "IV", v: Number(snap.iv || 0) },
      { k: "PCR", v: Number(snap.pcr || 0) * 100 },
      { k: "Delta", v: avgDelta * 10 },
      { k: "Gamma", v: avgGamma / 1000 },
    ];
  }, [heat, snap]);

  const routeChecks = [
    { k: "Momentum", pass: (feature.momentum_score || 0) > 0.12 },
    { k: "Delta spike", pass: (feature.delta_velocity || 0) > 0.0006 },
    { k: "Volume expansion", pass: (feature.volume_acceleration || 0) > 0.05 },
    { k: "Spread tight", pass: (feature.spread_quality || 0) > 0.38 },
    { k: "VWAP align", pass: (feature.vwap_alignment || 0) > 0 },
    { k: "Gamma align", pass: (feature.gamma_bias || 0) > -0.2 },
    { k: "AI threshold", pass: (signal.ai?.ai_confidence || 0) >= Number(payload.risk_config?.ai_threshold || 65) },
  ];

  const backtestRows = payload.backtesting?.recent_replay || [];
  const replayPoint = backtestRows[replayCursor] || {};

  const postConfig = async (body) => {
    await fetch(`${API_URL}/api/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-[1900px] p-3">
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          className="mb-3 flex items-center justify-between rounded-xl border border-slate-800 bg-slate-900/80 px-4 py-3"
        >
          <div className="flex items-center gap-3">
            <div className="h-3 w-3 rounded-full bg-cyan-400 shadow-[0_0_18px_#22d3ee]" />
            <div>
              <div className="text-xl font-bold tracking-wide">PRO SCALPER</div>
              <div className="text-xs text-slate-400">Institutional AI Options Scalping Terminal · NIFTY / SENSEX</div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button className={`rounded px-3 py-1 text-xs ${selectedSymbol === "NIFTY" ? "bg-cyan-600" : "bg-slate-800"}`} onClick={() => setSelectedSymbol("NIFTY")}>NIFTY</button>
            <button className={`rounded px-3 py-1 text-xs ${selectedSymbol === "SENSEX" ? "bg-cyan-600" : "bg-slate-800"}`} onClick={() => setSelectedSymbol("SENSEX")}>SENSEX</button>
            <span className={`rounded px-2 py-1 text-xs ${sessionBadge}`}>{payload.session}</span>
            <span className={`rounded px-2 py-1 text-xs ${connected ? "bg-emerald-700/60" : "bg-rose-700/60"}`}>WS {connected ? "UP" : "DOWN"}</span>
            <span className={`rounded px-2 py-1 text-xs ${runtime.safe_mode ? "bg-rose-700/80" : "bg-emerald-700/70"}`}>{runtime.safe_mode ? "SAFE MODE" : "LIVE EXECUTION"}</span>
          </div>
        </motion.div>

        {runtime.safe_mode && (
          <div className="mb-3 rounded-lg border border-rose-700 bg-rose-900/30 px-3 py-2 text-sm text-rose-200">
            Broker disconnected or protections triggered. Auto trading is blocked. Reason: <span className="font-semibold">{runtime.broker_reason}</span>
          </div>
        )}

        <div className="grid grid-cols-12 gap-3">
          <aside className="col-span-2 space-y-2">
            <div className={panelClass}>
              <div className={tiny}>Modules</div>
              <div className="mt-2 space-y-1 text-xs">
                {moduleLabels.map((m) => (
                  <div key={m} className="rounded bg-slate-950/60 px-2 py-1 text-slate-300">{m}</div>
                ))}
              </div>
            </div>
            <div className={panelClass}>
              <div className={tiny}>Execution Controls</div>
              <div className="mt-2 flex flex-col gap-2 text-xs">
                <button className="rounded bg-emerald-700/70 px-2 py-1" onClick={() => fetch(`${API_URL}/api/trading/true`, { method: "POST" })}>Enable Auto</button>
                <button className="rounded bg-rose-700/70 px-2 py-1" onClick={() => fetch(`${API_URL}/api/trading/false`, { method: "POST" })}>Disable Auto</button>
                <button
                  className="rounded bg-cyan-700/70 px-2 py-1"
                  onClick={() =>
                    fetch(`${API_URL}/api/order/manual`, {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({ symbol: selectedSymbol, quantity_lots: 1, force: false }),
                    })
                  }
                >
                  Manual Buy 1 Lot
                </button>
              </div>
            </div>
          </aside>

          <main className="col-span-10 grid grid-cols-10 gap-3">
            <section className="col-span-6 space-y-3">
              <div className={panelClass}>
                <div className="mb-2 flex items-center justify-between">
                  <div className={tiny}>Execution HUD</div>
                  <div className="text-xs text-slate-400">Regime: {signal.regime || "NA"}</div>
                </div>
                <div className="grid grid-cols-5 gap-2">
                  <Stat label="Spot LTP" value={Number(snap.spot_ltp || 0).toFixed(2)} />
                  <Stat label="ATM Strike" value={Number(snap.atm_strike || 0).toFixed(2)} />
                  <Stat label="Call LTP" value={Number(snap.call_ltp || 0).toFixed(2)} />
                  <Stat label="TQS" value={Number(signal.tqs || 0).toFixed(2)} danger={Number(signal.tqs || 0) < Number(payload.risk_config?.ai_threshold || 65)} />
                  <Stat label="AI Confidence" value={Number(signal.ai?.ai_confidence || 0).toFixed(1)} />
                  <Stat label="Realized PnL" value={Number(perf.realized_pnl || 0).toFixed(2)} danger={Number(perf.realized_pnl || 0) < 0} />
                  <Stat label="Unrealized PnL" value={Number(perf.unrealized_pnl || 0).toFixed(2)} danger={Number(perf.unrealized_pnl || 0) < 0} />
                  <Stat label="Exposure %" value={`${(Number(perf.exposure_pct || 0) * 100).toFixed(1)}%`} />
                  <Stat label="Delta Velocity" value={Number(feature.delta_velocity || 0).toExponential(2)} />
                  <Stat label="Spread Quality" value={`${(Number(feature.spread_quality || 0) * 100).toFixed(1)}%`} />
                  <Stat label="Volatility" value={Number(signal.realized_volatility || 0).toFixed(4)} />
                  <Stat label="Cumulative Delta" value={Number(signal.cumulative_delta || 0).toFixed(1)} />
                  <Stat label="Active Trades" value={payload.active_trades?.length || 0} />
                  <Stat label="Trailing State" value={payload.active_trades?.[0]?.trailing_state || "NA"} />
                  <Stat label="Broker Health" value={runtime.broker_state || "NA"} danger={runtime.broker_state !== "CONNECTED"} />
                  <Stat label="Exec Latency ms" value={quality.latency.toFixed(1)} />
                  <Stat label="Order Quality" value={`rej:${quality.rejection.toFixed(1)}%`} danger={quality.rejection > 30} />
                </div>
                <div className="mt-3" ref={chartRef} />
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div className={panelClass}>
                  <div className={tiny}>Orderflow Analytics</div>
                  <div className="mt-2 h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={orderflowSeries}>
                        <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" />
                        <XAxis dataKey="name" stroke="#94a3b8" fontSize={10} />
                        <YAxis stroke="#94a3b8" fontSize={10} />
                        <Tooltip />
                        <Bar dataKey="v" fill="#22d3ee" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="mt-2 grid grid-cols-2 gap-1 text-xs">
                    <div>Bid absorption: {(micro.bid_absorption || 0).toFixed(2)}</div>
                    <div>Ask absorption: {(micro.ask_absorption || 0).toFixed(2)}</div>
                    <div>Sweep velocity: {(micro.sweep_velocity || 0).toFixed(2)}</div>
                    <div>Liquidity sweep conf: {(micro.liquidity_sweep_confirmation || 0).toFixed(0)}</div>
                  </div>
                </div>
                <div className={panelClass}>
                  <div className={tiny}>AI Matrix</div>
                  <div className="mt-2 h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={aiMatrixSeries}>
                        <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" />
                        <XAxis dataKey="k" stroke="#94a3b8" fontSize={10} />
                        <YAxis stroke="#94a3b8" fontSize={10} />
                        <Tooltip />
                        <Line dataKey="v" stroke="#a855f7" strokeWidth={2} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="mt-2 grid grid-cols-2 gap-1 text-xs">
                    <div>Expected Move: {(signal.ai?.expected_move || 0).toFixed(2)}</div>
                    <div>Expected Slip: {(signal.ai?.expected_slippage || 0).toFixed(2)}</div>
                    <div>Vol Expansion: {(signal.ai?.expected_volatility_expansion || 0).toFixed(2)}</div>
                    <div>PCR: {Number(snap.pcr || 0).toFixed(3)}</div>
                  </div>
                </div>
              </div>
            </section>

            <section className="col-span-4 space-y-3">
              <div className={panelClass}>
                <div className={tiny}>Upstox Portfolio</div>
                <div className="mt-2 grid grid-cols-2 gap-2 text-xs">
                  <Stat label="Available Capital" value={Number(payload.portfolio?.funds?.equity?.available_margin || 0).toFixed(2)} />
                  <Stat label="Used Margin" value={Number(payload.portfolio?.funds?.equity?.used_margin || 0).toFixed(2)} />
                  <Stat label="Realized PnL" value={Number(perf.realized_pnl || 0).toFixed(2)} danger={Number(perf.realized_pnl || 0) < 0} />
                  <Stat label="Unrealized PnL" value={Number(perf.unrealized_pnl || 0).toFixed(2)} danger={Number(perf.unrealized_pnl || 0) < 0} />
                  <Stat label="Positions" value={payload.portfolio?.positions?.length || 0} />
                  <Stat label="Orders" value={payload.portfolio?.orders?.length || 0} />
                  <Stat label="Broker Status" value={runtime.broker_state || "NA"} danger={runtime.broker_state !== "CONNECTED"} />
                  <Stat label="Exposure %" value={`${(Number(perf.exposure_pct || 0) * 100).toFixed(1)}%`} />
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Risk Engine</div>
                <div className="mt-2 space-y-2 text-xs">
                  <label className="flex items-center justify-between"><span>Trading Capital</span><input className="w-32 rounded bg-slate-950 px-2 py-1" type="number" value={riskEdit.trading_capital} onChange={(e) => setRiskEdit((x) => ({ ...x, trading_capital: Number(e.target.value) }))} /></label>
                  <label className="flex items-center justify-between"><span>Max Exposure %</span><input className="w-32 rounded bg-slate-950 px-2 py-1" type="number" step="0.01" value={riskEdit.max_exposure_pct} onChange={(e) => setRiskEdit((x) => ({ ...x, max_exposure_pct: Number(e.target.value) }))} /></label>
                  <label className="flex items-center justify-between"><span>AI Threshold</span><input className="w-32 rounded bg-slate-950 px-2 py-1" type="number" value={riskEdit.ai_threshold} onChange={(e) => setRiskEdit((x) => ({ ...x, ai_threshold: Number(e.target.value) }))} /></label>
                  <label className="flex items-center justify-between"><span>Aggression</span><input className="w-32 rounded bg-slate-950 px-2 py-1" type="number" step="0.1" value={riskEdit.aggression_level} onChange={(e) => setRiskEdit((x) => ({ ...x, aggression_level: Number(e.target.value) }))} /></label>
                  <button className="w-full rounded bg-cyan-700/70 py-1" onClick={() => postConfig(riskEdit)}>Save Risk Config</button>
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Telemetry</div>
                <div className="mt-2 h-36">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={[{ k: "API", v: runtime.api_latency_ms || 0 }, { k: "WS", v: runtime.websocket_latency_ms || 0 }, { k: "Exec", v: perf.execution_latency_ms || 0 }] }>
                      <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" />
                      <XAxis dataKey="k" stroke="#94a3b8" fontSize={10} />
                      <YAxis stroke="#94a3b8" fontSize={10} />
                      <Tooltip />
                      <Area dataKey="v" stroke="#22c55e" fill="#14532d" />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-1 text-xs">
                  <div>Broker: {runtime.broker_state}</div>
                  <div>WS Alive: {String(runtime.websocket_alive)}</div>
                  <div>Stale Feed: {String(runtime.stale_feed)}</div>
                  <div>Fill Drift: {Number(perf.fill_drift || 0).toFixed(2)}</div>
                </div>
              </div>
            </section>

            <section className="col-span-10 grid grid-cols-5 gap-3">
              <div className={panelClass}>
                <div className={tiny}>Heatmap Terminal</div>
                <div className="mt-2 max-h-60 overflow-auto text-xs">
                  <table className="w-full border-collapse">
                    <thead><tr className="text-slate-400"><th className="text-left">Strike</th><th className="text-right">Bid</th><th className="text-right">Ask</th><th className="text-right">Gamma</th><th className="text-right">Delta</th></tr></thead>
                    <tbody>
                      {heatRows.map((r) => (
                        <tr key={r.strike} className="border-t border-slate-800">
                          <td>{r.strike}</td><td className="text-right text-emerald-300">{r.bid.toFixed(0)}</td><td className="text-right text-rose-300">{r.ask.toFixed(0)}</td><td className="text-right text-violet-300">{r.gamma.toFixed(0)}</td><td className={`text-right ${r.delta > 0 ? "text-cyan-300" : "text-orange-300"}`}>{r.delta.toFixed(2)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="mt-2 text-xs text-slate-300">Support: {heat.support_resistance?.support || "NA"} | Resistance: {heat.support_resistance?.resistance || "NA"}</div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Greeks & IV</div>
                <div className="mt-2 h-44">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={greekSeries}>
                      <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" />
                      <XAxis dataKey="k" stroke="#94a3b8" fontSize={10} />
                      <YAxis stroke="#94a3b8" fontSize={10} />
                      <Tooltip />
                      <Bar dataKey="v" fill="#60a5fa" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="mt-2 text-xs">Gamma wall: {snap.gamma_wall || "NA"}</div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Strategy Router</div>
                <div className="mt-2 space-y-1 text-xs">
                  {routeChecks.map((r) => (
                    <div key={r.k} className={`flex justify-between rounded px-2 py-1 ${r.pass ? "bg-emerald-900/30 text-emerald-300" : "bg-rose-900/30 text-rose-300"}`}>
                      <span>{r.k}</span><span>{r.pass ? "PASS" : "BLOCK"}</span>
                    </div>
                  ))}
                </div>
                <div className="mt-2 text-xs">Cross-corr: {Number(perf.cross_symbol_correlation || 0).toFixed(3)}</div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>AI Analytics</div>
                <div className="mt-2 h-44">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={aiHistory}>
                      <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" />
                      <XAxis dataKey="t" stroke="#94a3b8" fontSize={10} />
                      <YAxis stroke="#94a3b8" fontSize={10} />
                      <Tooltip />
                      <Line dataKey="ai" stroke="#22d3ee" dot={false} />
                      <Line dataKey="tqs" stroke="#f59e0b" dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Trade Journal</div>
                <div className="mt-2 max-h-56 overflow-auto space-y-1 text-xs">
                  {(payload.recent_trades || []).slice().reverse().slice(0, 18).map((t) => (
                    <div key={t.trade_id} className="rounded bg-slate-950/70 p-2">
                      <div className="flex justify-between"><span>{t.symbol}</span><span className={Number(t.pnl || 0) >= 0 ? "text-emerald-300" : "text-rose-300"}>{Number(t.pnl || 0).toFixed(2)}</span></div>
                      <div className="text-slate-400">{Number(t.entry_price || 0).toFixed(2)} → {Number(t.exit_price || 0).toFixed(2)} | {t.exit_reason}</div>
                    </div>
                  ))}
                </div>
              </div>
            </section>

            <section className="col-span-10 grid grid-cols-4 gap-3">
              <div className={panelClass}>
                <div className={tiny}>Session Intelligence</div>
                <div className="mt-2 space-y-1 text-xs">
                  <div>Overnight Sentiment: {Number(sessionIntel.overnight_sentiment || 0).toFixed(2)}</div>
                  <div>Gap Probability: {Number(sessionIntel.gap_probability || 0).toFixed(2)}%</div>
                  <div>Gap vs Prev Close: {Number(sessionIntel.gap_pct_vs_prev_close || 0).toFixed(2)}%</div>
                  <div>Expected Drive: {Number(sessionIntel.expected_opening_drive || 0).toFixed(2)}</div>
                  <div>GIFT Change: {Number(payload.session_intelligence?.global?.gift?.change_pct || 0).toFixed(2)}%</div>
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Backtesting</div>
                <div className="mt-2 space-y-2 text-xs">
                  <div>Stored ticks: {payload.backtesting?.total_records || 0}</div>
                  <div className="flex items-center gap-2">
                    <button className="rounded bg-slate-800 px-2 py-1" onClick={() => setReplayCursor((v) => Math.max(v - 1, 0))}>Prev</button>
                    <button className="rounded bg-slate-800 px-2 py-1" onClick={() => setReplayCursor((v) => Math.min(v + 1, Math.max(backtestRows.length - 1, 0)))}>Next</button>
                    <span>Cursor: {replayCursor}</span>
                  </div>
                  <div className="rounded bg-slate-950/70 p-2">
                    <div>Symbol: {replayPoint.symbol || "NA"}</div>
                    <div>Spot: {Number(replayPoint.spot_ltp || 0).toFixed(2)}</div>
                    <div>Call: {Number(replayPoint.call_ltp || 0).toFixed(2)}</div>
                    <div>TQS: {Number(replayPoint.tqs || 0).toFixed(2)}</div>
                  </div>
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Settings</div>
                <div className="mt-2 space-y-2 text-xs">
                  <label className="flex items-center justify-between"><span>Max Latency ms</span><input className="w-24 rounded bg-slate-950 px-2 py-1" type="number" value={settingsEdit.max_latency_ms} onChange={(e) => setSettingsEdit((x) => ({ ...x, max_latency_ms: Number(e.target.value) }))} /></label>
                  <label className="flex items-center justify-between"><span>Stale Feed sec</span><input className="w-24 rounded bg-slate-950 px-2 py-1" type="number" value={settingsEdit.stale_data_seconds} onChange={(e) => setSettingsEdit((x) => ({ ...x, stale_data_seconds: Number(e.target.value) }))} /></label>
                  <label className="flex items-center justify-between"><span>Slip Kill</span><input className="w-24 rounded bg-slate-950 px-2 py-1" type="number" step="0.1" value={settingsEdit.slippage_kill_switch_points} onChange={(e) => setSettingsEdit((x) => ({ ...x, slippage_kill_switch_points: Number(e.target.value) }))} /></label>
                  <label className="flex items-center justify-between"><span>Sym Conc %</span><input className="w-24 rounded bg-slate-950 px-2 py-1" type="number" step="0.01" value={settingsEdit.max_symbol_concentration_pct} onChange={(e) => setSettingsEdit((x) => ({ ...x, max_symbol_concentration_pct: Number(e.target.value) }))} /></label>
                  <button className="w-full rounded bg-violet-700/70 py-1" onClick={() => postConfig(settingsEdit)}>Save Settings</button>
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Session PnL Profile</div>
                <div className="mt-2 h-44">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={[{ k: "R-PnL", v: perf.realized_pnl || 0 }, { k: "U-PnL", v: perf.unrealized_pnl || 0 }, { k: "Wins", v: perf.wins || 0 }, { k: "Loss", v: perf.losses || 0 }]}>
                      <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" />
                      <XAxis dataKey="k" stroke="#94a3b8" fontSize={10} />
                      <YAxis stroke="#94a3b8" fontSize={10} />
                      <Tooltip />
                      <Bar dataKey="v" fill="#f59e0b" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="mt-2 text-xs">Drawdown: {(Number(perf.daily_drawdown_pct || 0) * 100).toFixed(2)}%</div>
              </div>
            </section>
          </main>
        </div>
      </div>
    </div>
  );
}
