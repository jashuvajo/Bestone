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
  performance: { realized_pnl: 0, unrealized_pnl: 0, rejection_rate: 0, fill_drift: 0, execution_latency_ms: 0 },
  signals: { NIFTY: {}, SENSEX: {} },
  snapshots: {},
  active_trades: [],
  recent_trades: [],
  heatmaps: {},
};

function Stat({ label, value, danger = false }) {
  return (
    <div className="rounded-md bg-slate-950/70 px-2 py-1">
      <div className={tiny}>{label}</div>
      <div className={`text-sm font-semibold ${danger ? "text-rose-400" : "text-cyan-300"}`}>{value}</div>
    </div>
  );
}

function useChart(containerRef, history) {
  useEffect(() => {
    if (!containerRef.current) return;
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 220,
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
  const [priceSeries, setPriceSeries] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState("NIFTY");
  const [riskEdit, setRiskEdit] = useState({
    trading_capital: 250000,
    max_exposure_pct: 0.35,
    ai_threshold: 65,
    aggression_level: 1,
  });
  const [connected, setConnected] = useState(false);
  const chartRef = useRef(null);

  const snap = payload.snapshots[selectedSymbol] || {};
  const signal = payload.signals[selectedSymbol] || {};
  const perf = payload.performance || {};
  const heat = payload.heatmaps[selectedSymbol] || {};

  useChart(chartRef, priceSeries);

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
        const now = Math.floor((data.timestamp || Date.now() / 1000));
        const px = data.snapshots?.[selectedSymbol]?.spot_ltp;
        if (typeof px === "number" && Number.isFinite(px)) {
          setPriceSeries((old) => [...old.slice(-450), { time: now, value: px }]);
        }
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

  const riskSave = async () => {
    await fetch(`${API_URL}/api/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(riskEdit),
    });
  };

  const toggleAuto = async (enabled) => {
    await fetch(`${API_URL}/api/trading/${enabled ? "true" : "false"}`, { method: "POST" });
  };

  const runManual = async () => {
    await fetch(`${API_URL}/api/order/manual`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol: selectedSymbol, quantity_lots: 1, force: false }),
    });
  };

  const heatRows = useMemo(() => {
    if (!heat?.strikes || !heat?.liquidity_walls_bid) return [];
    return heat.strikes.slice(-30).map((strike, idx) => ({
      strike,
      bid: Number(heat.liquidity_walls_bid[idx] || 0),
      ask: Number(heat.liquidity_walls_ask?.[idx] || 0),
      gamma: Number(heat.gamma_walls?.[idx] || 0),
      delta: Number(heat.delta_heat?.[idx] || 0),
    }));
  }, [heat]);

  const orderflowSeries = useMemo(() => {
    const f = signal.features || {};
    return [
      { name: "Momentum", v: (f.momentum_score || 0) * 100 },
      { name: "Delta Vel", v: (f.delta_velocity || 0) * 100000 },
      { name: "Volume Acc", v: (f.volume_acceleration || 0) * 100 },
      { name: "Spread Q", v: (f.spread_quality || 0) * 100 },
      { name: "Gamma", v: (f.gamma_bias || 0) * 100 },
      { name: "VWAP", v: (f.vwap_alignment || 0) * 100 },
    ];
  }, [signal]);

  const aiSeries = useMemo(() => {
    const ai = signal.ai || {};
    return [
      { k: "AI", v: ai.ai_confidence || 0 },
      { k: "Model", v: ai.model_confidence || 0 },
      { k: "Bayes", v: ai.bayesian_confidence || 0 },
      { k: "TQS", v: signal.tqs || 0 },
    ];
  }, [signal]);

  const micro = signal.microstructure || {};
  const runtime = payload.runtime || {};

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-[1800px] p-3">
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
            <button
              className={`rounded px-3 py-1 text-xs ${selectedSymbol === "NIFTY" ? "bg-cyan-600" : "bg-slate-800"}`}
              onClick={() => setSelectedSymbol("NIFTY")}
            >
              NIFTY
            </button>
            <button
              className={`rounded px-3 py-1 text-xs ${selectedSymbol === "SENSEX" ? "bg-cyan-600" : "bg-slate-800"}`}
              onClick={() => setSelectedSymbol("SENSEX")}
            >
              SENSEX
            </button>
            <span className={`rounded px-2 py-1 text-xs ${sessionBadge}`}>{payload.session}</span>
            <span className={`rounded px-2 py-1 text-xs ${connected ? "bg-emerald-700/60" : "bg-rose-700/60"}`}>
              WS {connected ? "UP" : "DOWN"}
            </span>
            <span className={`rounded px-2 py-1 text-xs ${runtime.safe_mode ? "bg-rose-700/80" : "bg-emerald-700/70"}`}>
              {runtime.safe_mode ? "SAFE MODE" : "LIVE EXECUTION"}
            </span>
          </div>
        </motion.div>

        {runtime.safe_mode && (
          <div className="mb-3 rounded-lg border border-rose-700 bg-rose-900/30 px-3 py-2 text-sm text-rose-200">
            Broker disconnected or protection triggered. Auto trading is blocked until broker health recovers. Reason:{" "}
            <span className="font-semibold">{runtime.broker_reason}</span>
          </div>
        )}

        <div className="grid grid-cols-12 gap-3">
          <aside className="col-span-2 space-y-2">
            <div className={panelClass}>
              <div className={tiny}>Modules</div>
              <div className="mt-2 space-y-1 text-xs">
                {moduleLabels.map((m) => (
                  <div key={m} className="rounded bg-slate-950/60 px-2 py-1 text-slate-300">
                    {m}
                  </div>
                ))}
              </div>
            </div>

            <div className={panelClass}>
              <div className={tiny}>Execution Controls</div>
              <div className="mt-2 flex flex-col gap-2 text-xs">
                <button className="rounded bg-emerald-700/70 px-2 py-1" onClick={() => toggleAuto(true)}>
                  Enable Auto
                </button>
                <button className="rounded bg-rose-700/70 px-2 py-1" onClick={() => toggleAuto(false)}>
                  Disable Auto
                </button>
                <button className="rounded bg-cyan-700/70 px-2 py-1" onClick={runManual}>
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
                <div className="grid grid-cols-4 gap-2">
                  <Stat label="Spot LTP" value={Number(snap.spot_ltp || 0).toFixed(2)} />
                  <Stat label="ATM Strike" value={Number(snap.atm_strike || 0).toFixed(2)} />
                  <Stat label="Call LTP" value={Number(snap.call_ltp || 0).toFixed(2)} />
                  <Stat label="TQS" value={Number(signal.tqs || 0).toFixed(2)} danger={Number(signal.tqs || 0) < Number(payload.risk_config?.ai_threshold || 65)} />
                  <Stat label="AI Confidence" value={Number(signal.ai?.ai_confidence || 0).toFixed(1)} />
                  <Stat label="Spread Quality" value={Number((signal.features?.spread_quality || 0) * 100).toFixed(1)} />
                  <Stat label="Delta Velocity" value={Number(signal.features?.delta_velocity || 0).toExponential(2)} />
                  <Stat label="Exec Latency ms" value={Number(perf.execution_latency_ms || 0).toFixed(1)} />
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
                    <div>Spoofing flag: {(micro.spoofing || 0).toFixed(0)}</div>
                  </div>
                </div>

                <div className={panelClass}>
                  <div className={tiny}>AI Matrix</div>
                  <div className="mt-2 h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={aiSeries}>
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
                  <Stat label="Active Orders" value={payload.portfolio?.orders?.length || 0} />
                  <Stat label="Positions" value={payload.portfolio?.positions?.length || 0} />
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Risk Engine</div>
                <div className="mt-2 space-y-2 text-xs">
                  <label className="flex items-center justify-between">
                    <span>Trading Capital</span>
                    <input
                      className="w-32 rounded bg-slate-950 px-2 py-1"
                      value={riskEdit.trading_capital}
                      onChange={(e) => setRiskEdit((x) => ({ ...x, trading_capital: Number(e.target.value) }))}
                      type="number"
                    />
                  </label>
                  <label className="flex items-center justify-between">
                    <span>Max Exposure %</span>
                    <input
                      className="w-32 rounded bg-slate-950 px-2 py-1"
                      value={riskEdit.max_exposure_pct}
                      onChange={(e) => setRiskEdit((x) => ({ ...x, max_exposure_pct: Number(e.target.value) }))}
                      type="number"
                      step="0.01"
                    />
                  </label>
                  <label className="flex items-center justify-between">
                    <span>AI Threshold</span>
                    <input
                      className="w-32 rounded bg-slate-950 px-2 py-1"
                      value={riskEdit.ai_threshold}
                      onChange={(e) => setRiskEdit((x) => ({ ...x, ai_threshold: Number(e.target.value) }))}
                      type="number"
                    />
                  </label>
                  <label className="flex items-center justify-between">
                    <span>Aggression</span>
                    <input
                      className="w-32 rounded bg-slate-950 px-2 py-1"
                      value={riskEdit.aggression_level}
                      onChange={(e) => setRiskEdit((x) => ({ ...x, aggression_level: Number(e.target.value) }))}
                      type="number"
                      step="0.1"
                    />
                  </label>
                  <button className="w-full rounded bg-cyan-700/70 py-1" onClick={riskSave}>
                    Save Risk Config
                  </button>
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Telemetry</div>
                <div className="mt-2 h-36">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart
                      data={[
                        { k: "API", v: runtime.api_latency_ms || 0 },
                        { k: "WS", v: runtime.websocket_latency_ms || 0 },
                        { k: "Exec", v: perf.execution_latency_ms || 0 },
                      ]}
                    >
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
                  <div>Rejection Rate: {(Number(perf.rejection_rate || 0) * 100).toFixed(1)}%</div>
                </div>
              </div>
            </section>

            <section className="col-span-10 grid grid-cols-3 gap-3">
              <div className={panelClass}>
                <div className={tiny}>Heatmap Terminal</div>
                <div className="mt-2 max-h-60 overflow-auto text-xs">
                  <table className="w-full border-collapse">
                    <thead>
                      <tr className="text-slate-400">
                        <th className="text-left">Strike</th>
                        <th className="text-right">Bid Wall</th>
                        <th className="text-right">Ask Wall</th>
                        <th className="text-right">Gamma</th>
                        <th className="text-right">Delta Heat</th>
                      </tr>
                    </thead>
                    <tbody>
                      {heatRows.map((r) => (
                        <tr key={r.strike} className="border-t border-slate-800">
                          <td>{r.strike}</td>
                          <td className="text-right text-emerald-300">{r.bid.toFixed(0)}</td>
                          <td className="text-right text-rose-300">{r.ask.toFixed(0)}</td>
                          <td className="text-right text-violet-300">{r.gamma.toFixed(0)}</td>
                          <td className={`text-right ${r.delta > 0 ? "text-cyan-300" : "text-orange-300"}`}>{r.delta.toFixed(2)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Trade Journal</div>
                <div className="mt-2 max-h-60 overflow-auto space-y-1 text-xs">
                  {(payload.recent_trades || []).slice().reverse().slice(0, 25).map((t) => (
                    <div key={t.trade_id} className="rounded bg-slate-950/70 p-2">
                      <div className="flex justify-between">
                        <span>{t.symbol}</span>
                        <span className={Number(t.pnl || 0) >= 0 ? "text-emerald-300" : "text-rose-300"}>
                          {Number(t.pnl || 0).toFixed(2)}
                        </span>
                      </div>
                      <div className="text-slate-400">
                        {Number(t.entry_price || 0).toFixed(2)} → {Number(t.exit_price || 0).toFixed(2)} | {t.exit_reason}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className={panelClass}>
                <div className={tiny}>Session Intelligence</div>
                <div className="mt-2 h-56">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={[
                        { k: "PnL", v: perf.realized_pnl || 0 },
                        { k: "U-PnL", v: perf.unrealized_pnl || 0 },
                        { k: "Wins", v: perf.wins || 0 },
                        { k: "Losses", v: perf.losses || 0 },
                      ]}
                    >
                      <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" />
                      <XAxis dataKey="k" stroke="#94a3b8" fontSize={10} />
                      <YAxis stroke="#94a3b8" fontSize={10} />
                      <Tooltip />
                      <Bar dataKey="v" fill="#f59e0b" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-1 text-xs">
                  <div>Drawdown: {(Number(perf.daily_drawdown_pct || 0) * 100).toFixed(2)}%</div>
                  <div>Cooldown Until: {perf.cooldown_until ? new Date(perf.cooldown_until * 1000).toLocaleTimeString() : "NA"}</div>
                  <div>Active Trades: {(payload.active_trades || []).length}</div>
                  <div>Mode: {runtime.auto_trading_enabled ? "AUTO" : "MANUAL"}</div>
                </div>
              </div>
            </section>
          </main>
        </div>
      </div>
    </div>
  );
}
