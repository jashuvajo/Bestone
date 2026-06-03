import asyncio
import gzip
import json
import logging
import os
import sqlite3
import statistics
import time
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import requests
import uvicorn
import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    import xgboost as xgb
except Exception:  # pragma: no cover
    xgb = None

try:
    import lightgbm as lgb
except Exception:  # pragma: no cover
    lgb = None

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

try:
    import redis
except Exception:  # pragma: no cover
    redis = None


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("pro-scalper")

INDIA_TZ_OFFSET_SECONDS = 5 * 3600 + 1800
API_BASE = "https://api.upstox.com"
ORDER_BASE = os.getenv("UPSTOX_ORDER_BASE", "https://api-hft.upstox.com")
POLL_INTERVAL_SECONDS = 1.0


class BrokerState(str, Enum):
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    DEGRADED = "DEGRADED"


class MarketRegime(str, Enum):
    TRENDING = "TRENDING"
    CHOPPY = "CHOPPY"
    VOLATILE = "VOLATILE"
    LOW_LIQUIDITY = "LOW_LIQUIDITY"
    BREAKOUT = "BREAKOUT"
    MEAN_REVERTING = "MEAN_REVERTING"
    EXPANDING_VOLATILITY = "EXPANDING_VOLATILITY"


class SessionState(str, Enum):
    PREMARKET = "PREMARKET"
    LIVE = "LIVE"
    POSTMARKET = "POSTMARKET"
    CLOSED = "CLOSED"


@dataclass
class RuntimeFlags:
    safe_mode: bool = True
    auto_trading_enabled: bool = False
    broker_state: BrokerState = BrokerState.DISCONNECTED
    broker_reason: str = "Startup pending"
    websocket_alive: bool = False
    rest_api_alive: bool = False
    api_latency_ms: float = 0.0
    websocket_latency_ms: float = 0.0
    stale_feed: bool = True
    last_ws_message_ts: float = 0.0
    last_analysis_ts: float = 0.0
    last_rest_success_ts: float = 0.0
    last_error: str = ""


@dataclass
class RiskConfig:
    trading_capital: float = 250000.0
    capital_allocation_pct: float = 0.12
    max_exposure_pct: float = 0.35
    max_daily_drawdown_pct: float = 0.05
    loss_cooldown_minutes: int = 10
    slippage_kill_switch_points: float = 1.5
    stale_data_seconds: int = 4
    max_latency_ms: int = 800
    ai_threshold: float = 65.0
    aggression_level: float = 1.0
    max_symbol_concentration_pct: float = 0.7


@dataclass
class MarketSnapshot:
    symbol: str
    spot_key: str
    spot_ltp: Optional[float] = None
    atm_strike: Optional[float] = None
    expiry_date: Optional[str] = None
    call_instrument_key: Optional[str] = None
    put_instrument_key: Optional[str] = None
    call_ltp: Optional[float] = None
    put_ltp: Optional[float] = None
    call_bid: Optional[float] = None
    call_ask: Optional[float] = None
    call_bid_qty: Optional[float] = None
    call_ask_qty: Optional[float] = None
    total_call_oi: float = 0.0
    total_put_oi: float = 0.0
    pcr: float = 0.0
    iv: float = 0.0
    gamma_wall: Optional[float] = None
    liquidity_wall_bid: Optional[float] = None
    liquidity_wall_ask: Optional[float] = None
    option_chain_raw: List[Dict[str, Any]] = field(default_factory=list)
    quote_raw: Dict[str, Any] = field(default_factory=dict)
    updated_at: float = 0.0


@dataclass
class TradeRecord:
    trade_id: str
    symbol: str
    instrument_token: str
    quantity: int
    side: str
    entry_price: float
    entry_ts: float
    max_price: float
    stop_loss: float
    target: float
    tqs: float
    ai_confidence: float
    regime: str
    trailing_state: str = "INIT"
    partial_exit_done: bool = False
    exit_price: Optional[float] = None
    exit_ts: Optional[float] = None
    exit_reason: Optional[str] = None
    order_id: Optional[str] = None
    pnl: float = 0.0


class ConfigPayload(BaseModel):
    trading_capital: Optional[float] = None
    capital_allocation_pct: Optional[float] = None
    max_exposure_pct: Optional[float] = None
    max_daily_drawdown_pct: Optional[float] = None
    loss_cooldown_minutes: Optional[int] = None
    slippage_kill_switch_points: Optional[float] = None
    stale_data_seconds: Optional[int] = None
    max_latency_ms: Optional[int] = None
    ai_threshold: Optional[float] = None
    aggression_level: Optional[float] = None
    max_symbol_concentration_pct: Optional[float] = None
    auto_trading_enabled: Optional[bool] = None


class OrderRequestPayload(BaseModel):
    symbol: str = Field(pattern="^(NIFTY|SENSEX)$")
    quantity_lots: int = Field(gt=0)
    order_type: str = Field(default="LIMIT", pattern="^(LIMIT|MARKET)$")
    force: bool = False


class UpstoxClient:
    def __init__(self) -> None:
        self.api_key = os.getenv("UPSTOX_API_KEY", "").strip()
        self.api_secret = os.getenv("UPSTOX_API_SECRET", "").strip()
        self.redirect_uri = os.getenv("UPSTOX_REDIRECT_URI", "").strip()
        self.auth_code = os.getenv("UPSTOX_AUTH_CODE", "").strip()
        self.refresh_token = os.getenv("UPSTOX_REFRESH_TOKEN", "").strip()
        self.access_token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
        self.extended_token = os.getenv("UPSTOX_EXTENDED_TOKEN", "").strip()
        self.http = requests.Session()
        self.http.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self.last_auth_refresh = 0.0

    def _auth_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    def _exchange_token(self, payload: Dict[str, str]) -> Dict[str, Any]:
        response = self.http.post(
            f"{API_BASE}/v2/login/authorization/token",
            data=payload,
            headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            timeout=8,
        )
        response.raise_for_status()
        result = response.json()
        token = result.get("access_token")
        if not token:
            raise RuntimeError(f"Upstox token exchange failed: {result}")
        self.access_token = token
        self.extended_token = result.get("extended_token", "")
        self.refresh_token = result.get("refresh_token", self.refresh_token)
        self.last_auth_refresh = time.time()
        return result

    def authenticate(self, force: bool = False) -> None:
        if self.access_token and not force:
            logger.info("Using current UPSTOX_ACCESS_TOKEN.")
            return
        runtime_token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
        if runtime_token:
            self.access_token = runtime_token
            self.last_auth_refresh = time.time()
            logger.info("Loaded runtime UPSTOX_ACCESS_TOKEN for session recovery.")
            return
        if self.refresh_token and self.api_key and self.api_secret:
            try:
                self._exchange_token(
                    {
                        "refresh_token": self.refresh_token,
                        "client_id": self.api_key,
                        "client_secret": self.api_secret,
                        "grant_type": "refresh_token",
                    }
                )
                logger.info("Upstox token refreshed using refresh_token grant.")
                return
            except Exception as exc:
                logger.warning("Refresh-token flow unavailable/failed: %s", exc)
        if all([self.api_key, self.api_secret, self.redirect_uri, self.auth_code]):
            self._exchange_token(
                {
                    "code": self.auth_code,
                    "client_id": self.api_key,
                    "client_secret": self.api_secret,
                    "redirect_uri": self.redirect_uri,
                    "grant_type": "authorization_code",
                }
            )
            logger.info("Upstox OAuth token acquired successfully.")
            return
        raise RuntimeError(
            "Missing recoverable Upstox auth material. Provide UPSTOX_ACCESS_TOKEN or full OAuth credentials."
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        form_payload: Optional[Dict[str, Any]] = None,
        base_url: str = API_BASE,
        timeout: float = 6,
        retry_auth: bool = True,
    ) -> Dict[str, Any]:
        url = f"{base_url}{path}"
        headers = self._auth_headers()
        if form_payload is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        response = self.http.request(
            method,
            url,
            headers=headers,
            params=params,
            json=payload,
            data=form_payload,
            timeout=timeout,
        )
        if response.status_code == 401 and retry_auth:
            self.authenticate(force=True)
            headers = self._auth_headers()
            if form_payload is not None:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
            response = self.http.request(
                method,
                url,
                headers=headers,
                params=params,
                json=payload,
                data=form_payload,
                timeout=timeout,
            )
        response.raise_for_status()
        text = response.text.strip()
        if not text:
            return {}
        return response.json()

    def validate_session(self) -> Tuple[bool, Dict[str, Any], float]:
        started = time.time()
        try:
            result = self._request("GET", "/v2/user/profile", timeout=6, retry_auth=True)
            return True, result, (time.time() - started) * 1000.0
        except Exception as exc:
            return False, {"error": str(exc)}, (time.time() - started) * 1000.0

    def get_ws_authorized_uri(self) -> str:
        result = self._request("GET", "/v3/feed/market-data-feed/authorize", timeout=6, retry_auth=True)
        data = result.get("data", {})
        ws_uri = data.get("authorized_redirect_uri")
        if not ws_uri:
            raise RuntimeError(f"Missing websocket redirect URI in response: {result}")
        return ws_uri

    def get_funds_margin(self) -> Dict[str, Any]:
        return self._request("GET", "/v2/user/get-funds-and-margin", timeout=6)

    def get_positions(self) -> Dict[str, Any]:
        return self._request("GET", "/v2/portfolio/short-term-positions", timeout=6)

    def get_holdings(self) -> Dict[str, Any]:
        return self._request("GET", "/v2/portfolio/long-term-holdings", timeout=6)

    def get_orderbook(self) -> Dict[str, Any]:
        return self._request("GET", "/v2/order/retrieve-all", timeout=6)

    def get_option_contracts(self, spot_key: str) -> Dict[str, Any]:
        return self._request("GET", "/v2/option/contract", params={"instrument_key": spot_key}, timeout=6)

    def get_option_chain(self, spot_key: str, expiry_date: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            "/v2/option/chain",
            params={"instrument_key": spot_key, "expiry_date": expiry_date},
            timeout=8,
        )

    def get_market_quotes(self, instrument_keys: List[str]) -> Dict[str, Any]:
        keys = [k for k in instrument_keys if k]
        if not keys:
            return {"status": "success", "data": {}}
        return self._request(
            "GET",
            "/v2/market-quote/quotes",
            params={"instrument_key": ",".join(keys)},
            timeout=6,
        )

    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/v3/order/place",
            base_url=ORDER_BASE,
            payload=payload,
            timeout=6,
        )


class DatabaseStore:
    def __init__(self) -> None:
        self.db_url = os.getenv("DATABASE_URL", "").strip()
        self.sqlite_path = os.getenv("SQLITE_PATH", "pro_scalper.db")
        self._sqlite = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        self._sqlite.execute("PRAGMA journal_mode=WAL;")
        self._sqlite.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()
        self.pg_conn = None
        if self.db_url and self.db_url.startswith("postgres") and psycopg2 is not None:
            try:
                self.pg_conn = psycopg2.connect(self.db_url)
                self.pg_conn.autocommit = True
                logger.info("Connected to PostgreSQL persistence.")
            except Exception as exc:
                logger.warning("PostgreSQL connection failed, falling back to SQLite: %s", exc)

    def _init_schema(self) -> None:
        cursor = self._sqlite.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ticks (
                ts REAL,
                symbol TEXT,
                spot_ltp REAL,
                call_ltp REAL,
                put_ltp REAL,
                spread REAL,
                delta_velocity REAL,
                volume_acceleration REAL,
                iv REAL,
                pcr REAL,
                regime TEXT,
                ai_confidence REAL,
                tqs REAL,
                latency_ms REAL,
                safe_mode INTEGER,
                raw_json TEXT
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                ts_open REAL,
                ts_close REAL,
                symbol TEXT,
                instrument_token TEXT,
                quantity INTEGER,
                side TEXT,
                entry_price REAL,
                exit_price REAL,
                pnl REAL,
                tqs REAL,
                ai_confidence REAL,
                exit_reason TEXT,
                regime TEXT,
                slippage REAL,
                execution_latency_ms REAL,
                raw_json TEXT
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry (
                ts REAL,
                broker_state TEXT,
                safe_mode INTEGER,
                websocket_alive INTEGER,
                api_latency_ms REAL,
                websocket_latency_ms REAL,
                rejection_rate REAL,
                fill_drift REAL,
                drawdown_pct REAL,
                raw_json TEXT
            );
            """
        )
        self._sqlite.commit()

    def write_tick(self, row: Dict[str, Any]) -> None:
        self._sqlite.execute(
            """
            INSERT INTO ticks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("ts"),
                row.get("symbol"),
                row.get("spot_ltp"),
                row.get("call_ltp"),
                row.get("put_ltp"),
                row.get("spread"),
                row.get("delta_velocity"),
                row.get("volume_acceleration"),
                row.get("iv"),
                row.get("pcr"),
                row.get("regime"),
                row.get("ai_confidence"),
                row.get("tqs"),
                row.get("latency_ms"),
                1 if row.get("safe_mode") else 0,
                json.dumps(row.get("raw", {}), default=str),
            ),
        )
        self._sqlite.commit()

    def write_trade(self, trade: TradeRecord, slippage: float, execution_latency_ms: float) -> None:
        self._sqlite.execute(
            """
            INSERT OR REPLACE INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade.trade_id,
                trade.entry_ts,
                trade.exit_ts,
                trade.symbol,
                trade.instrument_token,
                trade.quantity,
                trade.side,
                trade.entry_price,
                trade.exit_price,
                trade.pnl,
                trade.tqs,
                trade.ai_confidence,
                trade.exit_reason,
                trade.regime,
                slippage,
                execution_latency_ms,
                json.dumps(trade.__dict__, default=str),
            ),
        )
        self._sqlite.commit()

    def write_telemetry(self, payload: Dict[str, Any]) -> None:
        self._sqlite.execute(
            """
            INSERT INTO telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.get("ts"),
                payload.get("broker_state"),
                1 if payload.get("safe_mode") else 0,
                1 if payload.get("websocket_alive") else 0,
                payload.get("api_latency_ms"),
                payload.get("websocket_latency_ms"),
                payload.get("rejection_rate"),
                payload.get("fill_drift"),
                payload.get("drawdown_pct"),
                json.dumps(payload, default=str),
            ),
        )
        self._sqlite.commit()

    def recent_ticks(self, limit: int = 300) -> List[Dict[str, Any]]:
        cursor = self._sqlite.execute(
            """
            SELECT ts, symbol, spot_ltp, call_ltp, tqs, ai_confidence, regime
            FROM ticks ORDER BY ts DESC LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        return [
            {
                "ts": row[0],
                "symbol": row[1],
                "spot_ltp": row[2],
                "call_ltp": row[3],
                "tqs": row[4],
                "ai_confidence": row[5],
                "regime": row[6],
            }
            for row in rows
        ]


class ModelEngine:
    def __init__(self) -> None:
        self.samples: Deque[Dict[str, Any]] = deque(maxlen=5000)
        self.outcomes: Deque[int] = deque(maxlen=5000)
        self.feature_names = [
            "momentum_score",
            "delta_velocity",
            "aggressive_delta",
            "cumulative_delta_strength",
            "volume_acceleration",
            "spread_quality",
            "gamma_bias",
            "iv_expansion",
            "realized_volatility",
            "vwap_alignment",
            "option_chain_bias",
            "market_profile_alignment",
            "regime_support",
            "breakout_velocity",
            "orderbook_imbalance",
        ]
        self.xgb_model = None
        self.lgb_model = None
        self.last_train_ts = 0.0

    def append_observation(self, features: Dict[str, float], label: Optional[int] = None) -> None:
        self.samples.append({k: float(features.get(k, 0.0)) for k in self.feature_names})
        if label is not None:
            self.outcomes.append(int(label))

    def maybe_train(self) -> None:
        now = time.time()
        if now - self.last_train_ts < 120:
            return
        if len(self.samples) < 150 or len(self.outcomes) < 100:
            return
        feature_rows = list(self.samples)[-len(self.outcomes) :]
        x_df = pd.DataFrame(feature_rows)
        y = np.array(list(self.outcomes), dtype=np.float32)
        try:
            if xgb is not None:
                self.xgb_model = xgb.XGBClassifier(
                    n_estimators=120,
                    max_depth=4,
                    learning_rate=0.08,
                    subsample=0.9,
                    colsample_bytree=0.9,
                    objective="binary:logistic",
                    eval_metric="logloss",
                )
                self.xgb_model.fit(x_df, y)
            if lgb is not None:
                self.lgb_model = lgb.LGBMClassifier(
                    n_estimators=150,
                    learning_rate=0.06,
                    num_leaves=31,
                    subsample=0.9,
                    colsample_bytree=0.9,
                )
                self.lgb_model.fit(x_df, y)
            self.last_train_ts = now
            logger.info("AI models trained with %d samples.", len(y))
        except Exception as exc:
            logger.warning("Model training skipped due to error: %s", exc)

    def score(
        self,
        features: Dict[str, float],
        historical_wins: int,
        historical_losses: int,
        weighted_score: float,
    ) -> Dict[str, float]:
        x_df = pd.DataFrame([{k: float(features.get(k, 0.0)) for k in self.feature_names}])
        model_probs: List[float] = []
        try:
            if self.xgb_model is not None:
                model_probs.append(float(self.xgb_model.predict_proba(x_df)[0][1]))
            if self.lgb_model is not None:
                model_probs.append(float(self.lgb_model.predict_proba(x_df)[0][1]))
        except Exception as exc:
            logger.warning("Model score fallback due to error: %s", exc)
        model_conf = float(np.mean(model_probs) * 100.0) if model_probs else weighted_score
        alpha = historical_wins + 1
        beta = historical_losses + 1
        bayesian_conf = float(alpha / (alpha + beta) * 100.0)
        final_confidence = float(np.clip(0.55 * model_conf + 0.45 * bayesian_conf, 0.0, 100.0))
        expected_move = max(
            0.0,
            features.get("breakout_velocity", 0.0) * 1.2 + features.get("iv_expansion", 0.0) * 2.0,
        )
        expected_slippage = max(0.0, (1.0 - features.get("spread_quality", 0.0)) * 1.4)
        expected_vol_expansion = max(0.0, features.get("iv_expansion", 0.0) * 100.0)
        return {
            "ai_confidence": final_confidence,
            "model_confidence": model_conf,
            "bayesian_confidence": bayesian_conf,
            "expected_move": expected_move,
            "expected_slippage": expected_slippage,
            "expected_volatility_expansion": expected_vol_expansion,
        }


class ProScalperEngine:
    def __init__(self) -> None:
        self.client = UpstoxClient()
        self.db = DatabaseStore()
        self.model = ModelEngine()
        self.runtime = RuntimeFlags()
        self.risk = RiskConfig()
        self.broadcast_clients: Set[WebSocket] = set()
        self.redis_client = None
        if redis is not None and os.getenv("REDIS_URL"):
            try:
                self.redis_client = redis.from_url(os.environ["REDIS_URL"])
            except Exception as exc:
                logger.warning("Redis init failed, using memory bus: %s", exc)
        self.symbol_map = {
            "NIFTY": {"spot_key": "NSE_INDEX|Nifty 50", "lot_size": 65},
            "SENSEX": {"spot_key": "BSE_INDEX|SENSEX", "lot_size": 20},
        }
        self.gift_instrument_key = os.getenv("GIFT_NIFTY_INSTRUMENT_KEY", "").strip()
        self.snapshots: Dict[str, MarketSnapshot] = {
            symbol: MarketSnapshot(symbol=symbol, spot_key=v["spot_key"]) for symbol, v in self.symbol_map.items()
        }
        self.series: Dict[str, Dict[str, Deque[float]]] = defaultdict(
            lambda: {
                "spot": deque(maxlen=500),
                "call": deque(maxlen=500),
                "volume_proxy": deque(maxlen=500),
                "imbalance": deque(maxlen=500),
                "cum_delta": deque(maxlen=500),
                "spread": deque(maxlen=500),
            }
        )
        self.active_trades: Dict[str, TradeRecord] = {}
        self.closed_trades: Deque[TradeRecord] = deque(maxlen=500)
        self.performance = {
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "wins": 0,
            "losses": 0,
            "daily_peak_equity": self.risk.trading_capital,
            "daily_drawdown_pct": 0.0,
            "rejection_count": 0,
            "fill_count": 0,
            "slippage_history": deque(maxlen=200),
            "execution_latency_history": deque(maxlen=200),
            "loss_streak": 0,
            "cooldown_until": 0.0,
        }
        self.portfolio_cache: Dict[str, Any] = {
            "funds": {},
            "positions": [],
            "holdings": [],
            "orders": [],
            "updated_at": 0.0,
        }
        self.heatmaps: Dict[str, Dict[str, Any]] = {"NIFTY": {}, "SENSEX": {}}
        self.last_signal: Dict[str, Any] = {"NIFTY": {}, "SENSEX": {}}
        self.session_intelligence: Dict[str, Dict[str, Any]] = {"NIFTY": {}, "SENSEX": {}, "global": {}}
        self.backtest_cursor = 0
        self.expiry_refresh_ts: Dict[str, float] = {"NIFTY": 0.0, "SENSEX": 0.0}
        self.broker_diagnostics: Dict[str, Any] = {"last_profile": {}, "last_portfolio_error": "", "last_analysis_error": ""}

    def set_safe_mode(self, enabled: bool, reason: str) -> None:
        self.runtime.safe_mode = enabled
        if enabled:
            self.runtime.auto_trading_enabled = False
        self.runtime.broker_reason = reason
        logger.warning("SAFE MODE: %s | reason=%s", enabled, reason)

    def _indian_now(self) -> datetime:
        return datetime.utcfromtimestamp(time.time() + INDIA_TZ_OFFSET_SECONDS)

    def session_state(self) -> SessionState:
        now = self._indian_now()
        t = now.time()
        pre_start = datetime.strptime("09:00:00", "%H:%M:%S").time()
        live_start = datetime.strptime("09:15:00", "%H:%M:%S").time()
        live_end = datetime.strptime("15:30:00", "%H:%M:%S").time()
        post_end = datetime.strptime("16:00:00", "%H:%M:%S").time()
        if pre_start <= t < live_start:
            return SessionState.PREMARKET
        if live_start <= t <= live_end:
            return SessionState.LIVE
        if live_end < t <= post_end:
            return SessionState.POSTMARKET
        return SessionState.CLOSED

    async def startup(self) -> None:
        strict_startup_auth = os.getenv("STRICT_STARTUP_AUTH", "false").lower() == "true"
        if strict_startup_auth:
            self.client.authenticate()
            await self._refresh_broker_health()
        else:
            try:
                self.client.authenticate()
                await self._refresh_broker_health()
            except Exception as exc:
                self.runtime.broker_state = BrokerState.DISCONNECTED
                self.runtime.websocket_alive = False
                self.runtime.rest_api_alive = False
                self.runtime.last_error = str(exc)
                self.set_safe_mode(True, f"Startup auth unavailable: {exc}")
                logger.warning("Starting in SAFE MODE without broker session: %s", exc)
        asyncio.create_task(self._websocket_watchdog_loop())
        asyncio.create_task(self._portfolio_refresh_loop())
        asyncio.create_task(self._market_analysis_loop())
        asyncio.create_task(self._execution_loop())
        asyncio.create_task(self._broadcast_loop())
        asyncio.create_task(self._telemetry_loop())
        asyncio.create_task(self._session_refresh_loop())

    async def _session_refresh_loop(self) -> None:
        interval = max(60, int(os.getenv("UPSTOX_SESSION_REFRESH_SECONDS", "600")))
        while True:
            try:
                await asyncio.sleep(interval)
                await asyncio.to_thread(self.client.authenticate, True)
                await self._refresh_broker_health()
            except Exception as exc:
                self.runtime.broker_state = BrokerState.DISCONNECTED
                self.runtime.rest_api_alive = False
                self.runtime.last_error = str(exc)
                self.set_safe_mode(True, f"Session refresh failed: {exc}")

    async def _refresh_broker_health(self) -> None:
        ok, profile, latency_ms = await asyncio.to_thread(self.client.validate_session)
        self.runtime.api_latency_ms = latency_ms
        if not ok:
            self.runtime.rest_api_alive = False
            self.runtime.broker_state = BrokerState.DISCONNECTED
            self.runtime.websocket_alive = False
            self.runtime.last_error = str(profile.get("error", "Broker validation failed"))
            self.set_safe_mode(True, f"Broker validation failed: {profile.get('error')}")
            return

        self.runtime.rest_api_alive = True
        self.runtime.last_rest_success_ts = time.time()
        self.runtime.last_error = ""
        self.broker_diagnostics["last_profile"] = profile.get("data", {})

        if latency_ms > self.risk.max_latency_ms:
            self.runtime.broker_state = BrokerState.DEGRADED
            self.set_safe_mode(True, f"High API latency detected: {latency_ms:.1f} ms")
        elif not self.runtime.websocket_alive:
            self.runtime.broker_state = BrokerState.DEGRADED
            self.set_safe_mode(True, "REST connected, waiting for websocket market stream")
        else:
            self.runtime.broker_state = BrokerState.CONNECTED
            self.runtime.safe_mode = False
            self.runtime.broker_reason = "Healthy"
            if not self.runtime.auto_trading_enabled:
                self.runtime.auto_trading_enabled = True
        logger.info("Broker profile validated: %s", profile.get("data", {}).get("user_id", "unknown"))

    def _decode_ws_payload(self, message: Any) -> Optional[str]:
        if isinstance(message, str):
            return message
        if not isinstance(message, (bytes, bytearray)):
            return None
        blob = bytes(message)
        decoders = [
            lambda b: b.decode("utf-8"),
            lambda b: gzip.decompress(b).decode("utf-8"),
            lambda b: zlib.decompress(b).decode("utf-8"),
        ]
        for decoder in decoders:
            try:
                return decoder(blob)
            except Exception:
                continue
        return None

    async def _websocket_watchdog_loop(self) -> None:
        while True:
            try:
                if self.runtime.broker_state == BrokerState.DISCONNECTED:
                    await self._refresh_broker_health()
                    await asyncio.sleep(2)
                    continue
                ws_uri = await asyncio.to_thread(self.client.get_ws_authorized_uri)
                started = time.time()
                async with websockets.connect(ws_uri, ping_interval=10, ping_timeout=10, max_size=2**24) as ws:
                    self.runtime.websocket_alive = True
                    self.runtime.last_ws_message_ts = time.time()
                    if self.runtime.rest_api_alive and self.runtime.api_latency_ms <= self.risk.max_latency_ms:
                        self.runtime.broker_state = BrokerState.CONNECTED
                        self.runtime.safe_mode = False
                        self.runtime.broker_reason = "Healthy"
                    logger.info("Connected to Upstox MarketDataStreamerV3 endpoint.")
                    spot_keys = [v["spot_key"] for v in self.symbol_map.values()]
                    subscribe_payload = {
                        "guid": f"pro-scalper-{int(time.time())}",
                        "method": "sub",
                        "data": {"mode": "full", "instrumentKeys": spot_keys},
                    }
                    await ws.send(json.dumps(subscribe_payload))
                    while True:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=20)
                        self.runtime.last_ws_message_ts = time.time()
                        self.runtime.websocket_latency_ms = (time.time() - started) * 1000.0
                        decoded = self._decode_ws_payload(raw_msg)
                        if decoded:
                            self._consume_ws_message(decoded)
                        started = time.time()
            except Exception as exc:
                self.runtime.websocket_alive = False
                self.runtime.last_error = str(exc)
                self.set_safe_mode(True, f"Market websocket disconnected: {exc}")
                self.runtime.broker_state = BrokerState.DEGRADED if self.runtime.rest_api_alive else BrokerState.DISCONNECTED
                await asyncio.sleep(3)

    def _consume_ws_message(self, msg: str) -> None:
        try:
            data = json.loads(msg)
        except Exception:
            return
        feeds = data.get("feeds", {}) if isinstance(data, dict) else {}
        for symbol, mapping in self.symbol_map.items():
            spot_key = mapping["spot_key"]
            feed = feeds.get(spot_key, {})
            ltpc = feed.get("ltpc", {})
            ltp = ltpc.get("ltp")
            if ltp is None:
                continue
            snapshot = self.snapshots[symbol]
            snapshot.spot_ltp = float(ltp)
            snapshot.updated_at = time.time()

    async def _portfolio_refresh_loop(self) -> None:
        while True:
            try:
                funds, positions, holdings, orders = await asyncio.gather(
                    asyncio.to_thread(self.client.get_funds_margin),
                    asyncio.to_thread(self.client.get_positions),
                    asyncio.to_thread(self.client.get_holdings),
                    asyncio.to_thread(self.client.get_orderbook),
                )
                self.portfolio_cache = {
                    "funds": funds.get("data", {}),
                    "positions": positions.get("data", []),
                    "holdings": holdings.get("data", []),
                    "orders": orders.get("data", []),
                    "updated_at": time.time(),
                }
                self.runtime.rest_api_alive = True
                self.runtime.last_rest_success_ts = time.time()
                self.broker_diagnostics["last_portfolio_error"] = ""
                await asyncio.sleep(3)
            except Exception as exc:
                err = str(exc)
                logger.warning("Portfolio refresh failed: %s", err)
                self.broker_diagnostics["last_portfolio_error"] = err
                self.runtime.last_error = err
                if "401" in err or "Unauthorized" in err:
                    self.runtime.rest_api_alive = False
                    self.runtime.broker_state = BrokerState.DISCONNECTED
                else:
                    self.runtime.broker_state = BrokerState.DEGRADED if self.runtime.rest_api_alive else BrokerState.DISCONNECTED
                self.set_safe_mode(True, f"Portfolio endpoint failure: {err}")
                await asyncio.sleep(2)

    def _nearest_expiry(self, contracts_data: List[Dict[str, Any]]) -> Optional[str]:
        if not contracts_data:
            return None
        today = date.today()
        expiries: List[date] = []
        for item in contracts_data:
            raw = item.get("expiry") or item.get("expiry_date")
            if not raw:
                continue
            try:
                expiries.append(datetime.strptime(raw, "%Y-%m-%d").date())
            except Exception:
                continue
        valid = sorted(x for x in expiries if x >= today)
        return valid[0].isoformat() if valid else None

    def _should_refresh_expiry(self, symbol: str, snapshot: MarketSnapshot) -> bool:
        now = time.time()
        if not snapshot.expiry_date:
            return True
        last_refresh = self.expiry_refresh_ts.get(symbol, 0.0)
        if now - last_refresh > 300:
            return True
        try:
            expiry_dt = datetime.strptime(snapshot.expiry_date, "%Y-%m-%d").date()
            return expiry_dt < date.today()
        except Exception:
            return True

    def _extract_quote(self, quotes: Dict[str, Any], instrument_key: str) -> Dict[str, Any]:
        return quotes.get("data", {}).get(instrument_key, {})

    def _quote_ltp(self, quote: Dict[str, Any]) -> Optional[float]:
        if not isinstance(quote, dict):
            return None
        ltpc = quote.get("ltpc", {})
        ltp = ltpc.get("ltp")
        if ltp is not None:
            return float(ltp)
        ohlc = quote.get("ohlc", {})
        close = ohlc.get("close")
        return float(close) if close is not None else None

    def _extract_bid_ask(
        self,
        quote: Dict[str, Any],
    ) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        depth = quote.get("depth") or {}
        bids = depth.get("buy", []) if isinstance(depth, dict) else []
        asks = depth.get("sell", []) if isinstance(depth, dict) else []
        bid_px = float(bids[0].get("price")) if bids else None
        ask_px = float(asks[0].get("price")) if asks else None
        bid_qty = float(bids[0].get("quantity")) if bids else None
        ask_qty = float(asks[0].get("quantity")) if asks else None
        return bid_px, ask_px, bid_qty, ask_qty

    def _build_heatmap(self, symbol: str, chain: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not chain:
            return {}
        strikes: List[float] = []
        bid_liquidity: List[float] = []
        ask_liquidity: List[float] = []
        gamma_values: List[float] = []
        delta_values: List[float] = []
        volume_values: List[float] = []
        call_oi_values: List[float] = []
        put_oi_values: List[float] = []
        ladder_rows: List[Dict[str, Any]] = []

        for row in chain:
            strike = float(row.get("strike_price", 0.0))
            call_data = row.get("call_options", {})
            put_data = row.get("put_options", {})
            call_md = call_data.get("market_data", {})
            put_md = put_data.get("market_data", {})
            call_g = call_data.get("option_greeks", {})
            put_g = put_data.get("option_greeks", {})

            call_oi = float(call_md.get("oi", 0.0))
            put_oi = float(put_md.get("oi", 0.0))
            call_bid = float(call_md.get("bid_qty", 0.0))
            put_bid = float(put_md.get("bid_qty", 0.0))
            call_ask = float(call_md.get("ask_qty", 0.0))
            put_ask = float(put_md.get("ask_qty", 0.0))
            gamma = abs(float(call_g.get("gamma", 0.0))) * call_oi + abs(float(put_g.get("gamma", 0.0))) * put_oi
            delta_heat = float(call_g.get("delta", 0.0)) - float(put_g.get("delta", 0.0))
            volume = float(call_md.get("volume", 0.0)) + float(put_md.get("volume", 0.0))

            strikes.append(strike)
            bid_liquidity.append(call_bid + put_bid)
            ask_liquidity.append(call_ask + put_ask)
            gamma_values.append(gamma)
            delta_values.append(delta_heat)
            volume_values.append(volume)
            call_oi_values.append(call_oi)
            put_oi_values.append(put_oi)
            ladder_rows.append(
                {
                    "strike": strike,
                    "bid_qty": call_bid + put_bid,
                    "ask_qty": call_ask + put_ask,
                    "call_bid_qty": call_bid,
                    "put_bid_qty": put_bid,
                    "call_ask_qty": call_ask,
                    "put_ask_qty": put_ask,
                }
            )

        total_liq = np.array(bid_liquidity) + np.array(ask_liquidity)
        vol_arr = np.array(volume_values)
        delta_arr = np.abs(np.array(delta_values))
        call_oi_arr = np.array(call_oi_values)
        put_oi_arr = np.array(put_oi_values)

        liq_void_threshold = float(np.quantile(total_liq, 0.2)) if len(total_liq) else 0.0
        sweep_threshold = float(np.quantile(vol_arr, 0.8)) if len(vol_arr) else 0.0
        delta_threshold = float(np.quantile(delta_arr, 0.75)) if len(delta_arr) else 0.0
        oi_cluster_threshold = float(np.quantile(call_oi_arr + put_oi_arr, 0.85)) if len(call_oi_arr) else 0.0

        liquidity_voids = [strikes[i] for i, val in enumerate(total_liq) if val <= liq_void_threshold]
        sweep_zones = [
            strikes[i]
            for i, _ in enumerate(strikes)
            if vol_arr[i] >= sweep_threshold and delta_arr[i] >= delta_threshold
        ]
        stop_loss_clusters = [
            strikes[i] for i in range(len(strikes)) if (call_oi_arr[i] + put_oi_arr[i]) >= oi_cluster_threshold
        ]

        support_strike = strikes[int(np.argmax(put_oi_arr))] if len(strikes) else None
        resistance_strike = strikes[int(np.argmax(call_oi_arr))] if len(strikes) else None

        heatmap = {
            "symbol": symbol,
            "strikes": strikes,
            "liquidity_walls_bid": bid_liquidity,
            "liquidity_walls_ask": ask_liquidity,
            "gamma_walls": gamma_values,
            "delta_heat": delta_values,
            "volume_concentration": volume_values,
            "stop_loss_clusters": stop_loss_clusters,
            "sweep_zones": sweep_zones,
            "liquidity_voids": liquidity_voids,
            "support_resistance": {"support": support_strike, "resistance": resistance_strike},
            "bid_ask_ladders": ladder_rows,
            "updated_at": time.time(),
        }
        self.heatmaps[symbol] = heatmap
        return heatmap

    def _compute_regime(self, symbol: str) -> MarketRegime:
        s = self.series[symbol]
        if len(s["spot"]) < 30:
            return MarketRegime.LOW_LIQUIDITY
        spot = list(s["spot"])
        returns = np.diff(spot) / np.maximum(np.array(spot[:-1]), 1e-9)
        vol = float(np.std(returns))
        trend = float((spot[-1] - spot[0]) / max(abs(spot[0]), 1.0))
        spread_mean = statistics.mean(s["spread"]) if s["spread"] else 999.0
        if vol > 0.0035 and abs(trend) > 0.001:
            return MarketRegime.EXPANDING_VOLATILITY
        if spread_mean > 1.2:
            return MarketRegime.LOW_LIQUIDITY
        if abs(trend) > 0.0018 and vol < 0.003:
            return MarketRegime.TRENDING
        if vol > 0.004:
            return MarketRegime.VOLATILE
        if abs(trend) < 0.0004:
            return MarketRegime.CHOPPY
        if trend > 0.001 and vol > 0.0025:
            return MarketRegime.BREAKOUT
        return MarketRegime.MEAN_REVERTING

    def _vwap(self, prices: Deque[float], volumes: Deque[float]) -> float:
        if not prices or not volumes:
            return 0.0
        min_len = min(len(prices), len(volumes))
        p = np.array(list(prices)[-min_len:])
        v = np.array(list(volumes)[-min_len:])
        if float(v.sum()) <= 0:
            return float(p[-1])
        return float(np.dot(p, v) / v.sum())

    def _realized_volatility(self, symbol: str, window: int = 30) -> float:
        spot = list(self.series[symbol]["spot"])[-window:]
        if len(spot) < 5:
            return 0.0
        arr = np.array(spot, dtype=np.float64)
        returns = np.diff(arr) / np.maximum(arr[:-1], 1e-9)
        return float(np.std(returns) * np.sqrt(max(len(returns), 1)))

    def _microstructure(self, symbol: str, snapshot: MarketSnapshot) -> Dict[str, float]:
        s = self.series[symbol]
        spread = 0.0
        if snapshot.call_bid is not None and snapshot.call_ask is not None:
            spread = max(0.0, snapshot.call_ask - snapshot.call_bid)
        imbalance = 0.0
        if snapshot.call_bid_qty is not None and snapshot.call_ask_qty is not None:
            denom = snapshot.call_bid_qty + snapshot.call_ask_qty
            imbalance = ((snapshot.call_bid_qty - snapshot.call_ask_qty) / denom) if denom > 0 else 0.0
        recent_imbalance = list(s["imbalance"])[-20:]
        recent_spread = list(s["spread"])[-20:]
        bid_absorption = float(np.mean([1 for x in recent_imbalance if x > 0.1]) / max(len(recent_imbalance), 1))
        ask_absorption = float(np.mean([1 for x in recent_imbalance if x < -0.1]) / max(len(recent_imbalance), 1))
        spoofing = 1.0 if recent_imbalance and abs(recent_imbalance[-1] - imbalance) > 0.45 else 0.0
        iceberg = 1.0 if recent_spread and statistics.mean(recent_spread) < 0.8 and abs(imbalance) > 0.4 else 0.0
        liquidity_vacuum = 1.0 if spread > 1.6 else 0.0
        sweep_velocity = float(max(0.0, np.mean(np.diff(list(s["call"])[-8:])) if len(s["call"]) > 8 else 0.0))
        hidden_liquidity = float(max(0.0, bid_absorption - spoofing))
        stacked_bids = float(max(0.0, imbalance))
        stacked_offers = float(max(0.0, -imbalance))
        aggressive_mo_imbalance = float(imbalance * max(1.0, sweep_velocity))
        liquidity_sweep_confirmation = 1.0 if sweep_velocity > 0 and aggressive_mo_imbalance > 0 and spread < 1.2 else 0.0
        return {
            "spread": spread,
            "imbalance": imbalance,
            "bid_absorption": bid_absorption,
            "ask_absorption": ask_absorption,
            "spoofing": spoofing,
            "iceberg": iceberg,
            "liquidity_vacuum": liquidity_vacuum,
            "sweep_velocity": sweep_velocity,
            "hidden_liquidity": hidden_liquidity,
            "stacked_bids": stacked_bids,
            "stacked_offers": stacked_offers,
            "aggressive_mo_imbalance": aggressive_mo_imbalance,
            "liquidity_sweep_confirmation": liquidity_sweep_confirmation,
        }

    def _score_trade_quality(
        self,
        symbol: str,
        snapshot: MarketSnapshot,
        regime: MarketRegime,
        micro: Dict[str, float],
    ) -> Dict[str, Any]:
        s = self.series[symbol]
        if len(s["call"]) < 8:
            return {"tqs": 0.0, "weighted_score": 0.0, "features": {}, "ai": {}}
        call = np.array(s["call"], dtype=np.float64)
        spot = np.array(s["spot"], dtype=np.float64)
        momentum_score = float(np.clip((call[-1] - call[-6]) / 5.0, -1.0, 1.0))
        aggressive_delta = float(np.clip(micro["aggressive_mo_imbalance"], -1.0, 1.0))
        delta_velocity = float(np.clip((spot[-1] - spot[-4]) / max(spot[-4], 1e-9), -0.02, 0.02))
        volume_acceleration = (
            float(
                np.clip(
                    (statistics.mean(list(s["volume_proxy"])[-5:]) - statistics.mean(list(s["volume_proxy"])[-20:-5]))
                    / max(statistics.mean(list(s["volume_proxy"])[-20:-5]), 1.0),
                    -1.0,
                    2.0,
                )
            )
            if len(s["volume_proxy"]) > 20
            else 0.0
        )
        cumulative_delta_series = list(s["cum_delta"])
        cumulative_delta_strength = float(
            np.clip((cumulative_delta_series[-1] - cumulative_delta_series[-8]) / 5000.0, -1.0, 1.0)
            if len(cumulative_delta_series) >= 8
            else 0.0
        )
        spread_quality = float(np.clip(1.0 - (micro["spread"] / 2.0), 0.0, 1.0))
        gamma_bias = float(np.clip((snapshot.gamma_wall or 0.0) / 100000.0, -1.0, 1.0))
        iv_expansion = float(np.clip(snapshot.iv / 100.0, 0.0, 2.0))
        realized_volatility = float(np.clip(self._realized_volatility(symbol) * 100.0, 0.0, 5.0))
        vwap_val = self._vwap(s["call"], s["volume_proxy"])
        vwap_alignment = 1.0 if call[-1] > vwap_val else -0.6
        option_chain_bias = float(
            np.clip(
                (snapshot.total_call_oi - snapshot.total_put_oi)
                / max(snapshot.total_call_oi + snapshot.total_put_oi, 1.0),
                -1.0,
                1.0,
            )
        )
        market_profile_alignment = float(
            np.clip((spot[-1] - np.mean(spot[-30:])) / max(np.mean(spot[-30:]), 1.0), -0.01, 0.01)
        )
        regime_support = (
            1.0 if regime in [MarketRegime.TRENDING, MarketRegime.BREAKOUT, MarketRegime.EXPANDING_VOLATILITY] else -0.8
        )
        breakout_velocity = float(np.clip((call[-1] - call[-3]) / max(call[-3], 1e-9), -0.05, 0.05))
        orderbook_imbalance = micro["imbalance"]
        features = {
            "momentum_score": momentum_score,
            "delta_velocity": delta_velocity,
            "aggressive_delta": aggressive_delta,
            "cumulative_delta_strength": cumulative_delta_strength,
            "volume_acceleration": volume_acceleration,
            "spread_quality": spread_quality,
            "gamma_bias": gamma_bias,
            "iv_expansion": iv_expansion,
            "realized_volatility": realized_volatility,
            "vwap_alignment": vwap_alignment,
            "option_chain_bias": option_chain_bias,
            "market_profile_alignment": market_profile_alignment,
            "regime_support": regime_support,
            "breakout_velocity": breakout_velocity,
            "orderbook_imbalance": orderbook_imbalance,
        }
        weighted_score = float(
            100
            * (
                0.14 * max(0.0, momentum_score)
                + 0.08 * max(0.0, aggressive_delta)
                + 0.08 * max(0.0, cumulative_delta_strength)
                + 0.1 * max(0.0, delta_velocity * 40)
                + 0.11 * max(0.0, volume_acceleration)
                + 0.1 * spread_quality
                + 0.07 * max(0.0, gamma_bias)
                + 0.07 * max(0.0, iv_expansion / 2)
                + 0.05 * max(0.0, realized_volatility / 5)
                + 0.08 * max(0.0, vwap_alignment)
                + 0.08 * max(0.0, option_chain_bias)
                + 0.06 * max(0.0, market_profile_alignment * 100)
                + 0.06 * max(0.0, regime_support)
            )
        )
        ai = self.model.score(features, self.performance["wins"], self.performance["losses"], weighted_score)
        tqs = float(np.clip(0.65 * weighted_score + 0.35 * ai["ai_confidence"], 0.0, 100.0))
        return {"tqs": tqs, "weighted_score": weighted_score, "features": features, "ai": ai}

    def _quote_close(self, snapshot: MarketSnapshot) -> Optional[float]:
        spot_quote = snapshot.quote_raw.get(snapshot.spot_key, {}) if snapshot.quote_raw else {}
        ohlc = spot_quote.get("ohlc", {}) if isinstance(spot_quote, dict) else {}
        close = ohlc.get("close")
        return float(close) if close is not None else None

    def _build_session_intelligence(self, symbol: str, regime: MarketRegime) -> Dict[str, Any]:
        session = self.session_state()
        snapshot = self.snapshots[symbol]
        signal = self.last_signal.get(symbol, {})
        prev_close = self._quote_close(snapshot)
        spot = snapshot.spot_ltp or 0.0
        gap_pct = ((spot - prev_close) / prev_close) * 100.0 if prev_close else 0.0
        pcr_bias = float(np.clip((1.2 - snapshot.pcr), -1.0, 1.0))
        overnight_sentiment = float(np.clip(gap_pct * 8 + pcr_bias * 20 + (signal.get("tqs", 0.0) - 50) * 0.3, -100, 100))
        gap_probability = float(np.clip(abs(gap_pct) * 12 + snapshot.iv * 0.4 + abs(pcr_bias) * 20, 0, 100))
        expected_drive = float(np.clip((signal.get("features", {}).get("breakout_velocity", 0.0) * 1500), -100, 100))
        support = self.heatmaps.get(symbol, {}).get("support_resistance", {}).get("support")
        resistance = self.heatmaps.get(symbol, {}).get("support_resistance", {}).get("resistance")
        heat = self.heatmaps.get(symbol, {})
        strikes = heat.get("strikes", [])
        atm = snapshot.atm_strike or (strikes[len(strikes) // 2] if strikes else None)
        tomorrow_watchlist = []
        for strike in strikes[:]:
            if atm is None:
                break
            distance = abs(strike - atm)
            if distance <= (50 if symbol == "NIFTY" else 200):
                liq_idx = strikes.index(strike)
                liq_score = float((heat.get("liquidity_walls_bid", [0])[liq_idx] if liq_idx < len(heat.get("liquidity_walls_bid", [])) else 0))
                gamma_score = float((heat.get("gamma_walls", [0])[liq_idx] if liq_idx < len(heat.get("gamma_walls", [])) else 0))
                tomorrow_watchlist.append({"strike": strike, "distance_from_atm": distance, "liquidity_score": liq_score, "gamma_score": gamma_score})
        tomorrow_watchlist = sorted(tomorrow_watchlist, key=lambda x: (x["distance_from_atm"], -x["liquidity_score"], -x["gamma_score"]))[:6]

        result = {
            "symbol": symbol,
            "session": session.value,
            "overnight_sentiment": overnight_sentiment,
            "gap_probability": gap_probability,
            "gap_pct_vs_prev_close": gap_pct,
            "option_chain_positioning": {
                "pcr": snapshot.pcr,
                "total_call_oi": snapshot.total_call_oi,
                "total_put_oi": snapshot.total_put_oi,
                "iv": snapshot.iv,
            },
            "support_resistance": {"support": support, "resistance": resistance},
            "expected_opening_drive": expected_drive,
            "tomorrow_watchlist": tomorrow_watchlist,
            "regime": regime.value,
            "post_market_review": {
                "realized_pnl": self.performance["realized_pnl"],
                "wins": self.performance["wins"],
                "losses": self.performance["losses"],
                "execution_quality": self._execution_quality(),
            },
            "closed_market_replay_available": True,
            "updated_at": time.time(),
        }
        self.session_intelligence[symbol] = result
        return result

    async def _gift_influence(self) -> Dict[str, Any]:
        if not self.gift_instrument_key:
            return {"gift_key_configured": False}
        try:
            quotes = await asyncio.to_thread(self.client.get_market_quotes, [self.gift_instrument_key])
            quote = quotes.get("data", {}).get(self.gift_instrument_key, {})
            ltpc = quote.get("ltpc", {})
            ohlc = quote.get("ohlc", {})
            ltp = float(ltpc.get("ltp", 0.0))
            close = float(ohlc.get("close", 0.0))
            change_pct = ((ltp - close) / close) * 100.0 if close else 0.0
            return {
                "gift_key_configured": True,
                "instrument_key": self.gift_instrument_key,
                "ltp": ltp,
                "prev_close": close,
                "change_pct": change_pct,
            }
        except Exception as exc:
            return {"gift_key_configured": True, "error": str(exc)}

    async def _market_analysis_loop(self) -> None:
        while True:
            started = time.time()
            try:
                session = self.session_state()
                if self.runtime.broker_state == BrokerState.DISCONNECTED:
                    await self._refresh_broker_health()
                if not self.runtime.rest_api_alive:
                    await asyncio.sleep(1)
                    continue
                for symbol, cfg in self.symbol_map.items():
                    snapshot = self.snapshots[symbol]
                    if self._should_refresh_expiry(symbol, snapshot):
                        contracts = await asyncio.to_thread(self.client.get_option_contracts, cfg["spot_key"])
                        snapshot.expiry_date = self._nearest_expiry(contracts.get("data", []))
                        self.expiry_refresh_ts[symbol] = time.time()
                        env_expiry = os.getenv(f"{symbol}_EXPIRY", "").strip()
                        if env_expiry:
                            snapshot.expiry_date = env_expiry
                    if not snapshot.expiry_date:
                        raise RuntimeError(f"{symbol} expiry unavailable from option contracts")

                    try:
                        chain_resp = await asyncio.to_thread(self.client.get_option_chain, cfg["spot_key"], snapshot.expiry_date)
                    except Exception:
                        snapshot.expiry_date = None
                        self.expiry_refresh_ts[symbol] = 0.0
                        raise
                    chain = chain_resp.get("data", [])
                    if not chain:
                        raise RuntimeError(f"No option chain rows for {symbol}")
                    snapshot.option_chain_raw = chain
                    spot_price = float(chain[0].get("underlying_spot_price") or snapshot.spot_ltp or 0.0)
                    snapshot.spot_ltp = spot_price
                    atm_row = min(chain, key=lambda r: abs(float(r.get("strike_price", 0.0)) - spot_price))
                    snapshot.atm_strike = float(atm_row.get("strike_price"))
                    call_data = atm_row.get("call_options", {})
                    put_data = atm_row.get("put_options", {})
                    snapshot.call_instrument_key = call_data.get("instrument_key")
                    snapshot.put_instrument_key = put_data.get("instrument_key")
                    quote_keys = [cfg["spot_key"], snapshot.call_instrument_key or "", snapshot.put_instrument_key or ""]
                    quotes = await asyncio.to_thread(self.client.get_market_quotes, quote_keys)
                    snapshot.quote_raw = quotes.get("data", {})
                    spot_quote = self._extract_quote(quotes, cfg["spot_key"])
                    spot_quote_ltp = self._quote_ltp(spot_quote)
                    if spot_quote_ltp is not None:
                        snapshot.spot_ltp = float(spot_quote_ltp)
                    call_quote = self._extract_quote(quotes, snapshot.call_instrument_key or "")
                    put_quote = self._extract_quote(quotes, snapshot.put_instrument_key or "")
                    snapshot.call_ltp = float(self._quote_ltp(call_quote) or snapshot.call_ltp or 0.0)
                    snapshot.put_ltp = float(self._quote_ltp(put_quote) or snapshot.put_ltp or 0.0)
                    b, a, bq, aq = self._extract_bid_ask(call_quote)
                    snapshot.call_bid = b
                    snapshot.call_ask = a
                    snapshot.call_bid_qty = bq
                    snapshot.call_ask_qty = aq
                    snapshot.total_call_oi = float(sum(float(x.get("call_options", {}).get("market_data", {}).get("oi", 0.0)) for x in chain))
                    snapshot.total_put_oi = float(sum(float(x.get("put_options", {}).get("market_data", {}).get("oi", 0.0)) for x in chain))
                    snapshot.pcr = snapshot.total_put_oi / snapshot.total_call_oi if snapshot.total_call_oi > 0 else 0.0
                    snapshot.iv = float(
                        statistics.mean(
                            [
                                float(x.get("call_options", {}).get("option_greeks", {}).get("iv", 0.0))
                                for x in chain[: min(len(chain), 15)]
                            ]
                        )
                    )
                    heatmap = self._build_heatmap(symbol, chain)
                    if heatmap.get("gamma_walls"):
                        idx_gamma = int(np.argmax(heatmap["gamma_walls"]))
                        idx_bid = int(np.argmax(heatmap["liquidity_walls_bid"]))
                        idx_ask = int(np.argmax(heatmap["liquidity_walls_ask"]))
                        snapshot.gamma_wall = heatmap["strikes"][idx_gamma]
                        snapshot.liquidity_wall_bid = heatmap["strikes"][idx_bid]
                        snapshot.liquidity_wall_ask = heatmap["strikes"][idx_ask]
                    snapshot.updated_at = time.time()

                    s = self.series[symbol]
                    s["spot"].append(snapshot.spot_ltp or 0.0)
                    s["call"].append(snapshot.call_ltp or 0.0)
                    volume_proxy = float(
                        atm_row.get("call_options", {}).get("market_data", {}).get("volume", 0.0)
                        + atm_row.get("put_options", {}).get("market_data", {}).get("volume", 0.0)
                    )
                    s["volume_proxy"].append(volume_proxy)
                    spread = max(0.0, (snapshot.call_ask or 0.0) - (snapshot.call_bid or 0.0))
                    s["spread"].append(spread)
                    imbalance = 0.0
                    if snapshot.call_bid_qty and snapshot.call_ask_qty:
                        den = snapshot.call_bid_qty + snapshot.call_ask_qty
                        imbalance = (snapshot.call_bid_qty - snapshot.call_ask_qty) / den if den > 0 else 0.0
                    s["imbalance"].append(float(imbalance))
                    call_bid_qty = snapshot.call_bid_qty or 0.0
                    call_ask_qty = snapshot.call_ask_qty or 0.0
                    delta_step = float((call_bid_qty - call_ask_qty) * max(1.0, volume_proxy / 1000.0))
                    prev_cum_delta = s["cum_delta"][-1] if s["cum_delta"] else 0.0
                    s["cum_delta"].append(prev_cum_delta + delta_step)

                    regime = self._compute_regime(symbol)
                    micro = self._microstructure(symbol, snapshot)
                    scored = self._score_trade_quality(symbol, snapshot, regime, micro)
                    session_intel = self._build_session_intelligence(symbol, regime)
                    self.last_signal[symbol] = {
                        "symbol": symbol,
                        "session": session.value,
                        "regime": regime.value,
                        "tqs": scored.get("tqs", 0.0),
                        "weighted_score": scored.get("weighted_score", 0.0),
                        "ai": scored.get("ai", {}),
                        "features": scored.get("features", {}),
                        "cumulative_delta": s["cum_delta"][-1] if s["cum_delta"] else 0.0,
                        "realized_volatility": self._realized_volatility(symbol),
                        "microstructure": micro,
                        "heatmap": self.heatmaps[symbol],
                        "session_intelligence": session_intel,
                        "timestamp": time.time(),
                    }
                    self.model.append_observation(scored.get("features", {}))
                    self.model.maybe_train()
                    self.db.write_tick(
                        {
                            "ts": time.time(),
                            "symbol": symbol,
                            "spot_ltp": snapshot.spot_ltp,
                            "call_ltp": snapshot.call_ltp,
                            "put_ltp": snapshot.put_ltp,
                            "spread": micro["spread"],
                            "delta_velocity": scored.get("features", {}).get("delta_velocity", 0.0),
                            "volume_acceleration": scored.get("features", {}).get("volume_acceleration", 0.0),
                            "iv": snapshot.iv,
                            "pcr": snapshot.pcr,
                            "regime": regime.value,
                            "ai_confidence": scored.get("ai", {}).get("ai_confidence", 0.0),
                            "tqs": scored.get("tqs", 0.0),
                            "latency_ms": self.runtime.api_latency_ms,
                            "safe_mode": self.runtime.safe_mode,
                            "raw": {
                                "snapshot": snapshot.__dict__,
                                "signal": self.last_signal[symbol],
                                "session": session.value,
                            },
                        }
                    )

                self.session_intelligence["global"] = {
                    "session": session.value,
                    "gift": await self._gift_influence(),
                    "timestamp": time.time(),
                }
                self.runtime.last_analysis_ts = time.time()
                latest_snapshot_ts = max((x.updated_at for x in self.snapshots.values()), default=0.0)
                self.runtime.stale_feed = (time.time() - latest_snapshot_ts) > self.risk.stale_data_seconds
            except Exception as exc:
                err = str(exc)
                logger.error("Market analysis loop error: %s", err)
                self.broker_diagnostics["last_analysis_error"] = err
                self.runtime.last_error = err
                if "401" in err or "Unauthorized" in err:
                    self.runtime.rest_api_alive = False
                    self.runtime.broker_state = BrokerState.DISCONNECTED
                else:
                    self.runtime.broker_state = BrokerState.DEGRADED if self.runtime.rest_api_alive else BrokerState.DISCONNECTED
                self.set_safe_mode(True, f"Analysis loop failed: {err}")
            elapsed = time.time() - started
            await asyncio.sleep(max(0.0, POLL_INTERVAL_SECONDS - elapsed))

    def _portfolio_exposure_pct(self) -> float:
        exposure = sum(trade.entry_price * trade.quantity for trade in self.active_trades.values())
        cap = max(self.risk.trading_capital, 1.0)
        return float(np.clip(exposure / cap, 0.0, 2.0))

    def _symbol_exposure_pct(self, symbol: str) -> float:
        exposure = sum(trade.entry_price * trade.quantity for trade in self.active_trades.values() if trade.symbol == symbol)
        cap = max(self.risk.trading_capital, 1.0)
        return float(np.clip(exposure / cap, 0.0, 2.0))

    def _cross_symbol_correlation(self) -> float:
        n_spot = list(self.series["NIFTY"]["spot"])
        s_spot = list(self.series["SENSEX"]["spot"])
        min_len = min(len(n_spot), len(s_spot), 80)
        if min_len < 20:
            return 0.0
        n = np.array(n_spot[-min_len:])
        s = np.array(s_spot[-min_len:])
        n_ret = np.diff(n) / np.maximum(n[:-1], 1e-9)
        s_ret = np.diff(s) / np.maximum(s[:-1], 1e-9)
        corr = np.corrcoef(n_ret, s_ret)[0, 1]
        return float(corr) if not np.isnan(corr) else 0.0

    def _allowed_to_trade(self) -> Tuple[bool, str]:
        if self.runtime.safe_mode:
            return False, "SAFE_MODE_ACTIVE"
        if self.session_state() != SessionState.LIVE:
            return False, "MARKET_NOT_LIVE"
        if self.runtime.broker_state != BrokerState.CONNECTED:
            return False, "BROKER_NOT_HEALTHY"
        if not self.runtime.websocket_alive:
            return False, "WEBSOCKET_UNAVAILABLE"
        if self.runtime.api_latency_ms > self.risk.max_latency_ms:
            return False, "LATENCY_THRESHOLD_BREACHED"
        if self.runtime.stale_feed:
            return False, "STALE_FEED"
        if self._portfolio_exposure_pct() >= self.risk.max_exposure_pct:
            return False, "MAX_EXPOSURE_REACHED"
        if time.time() < self.performance["cooldown_until"]:
            return False, "LOSS_COOLDOWN_ACTIVE"
        if self.performance["daily_drawdown_pct"] >= self.risk.max_daily_drawdown_pct:
            return False, "MAX_DRAWDOWN_REACHED"
        if len(self.active_trades) >= 2 and self._cross_symbol_correlation() > 0.95:
            return False, "CORRELATION_PROTECTION_TRIGGERED"
        return True, "OK"

    def _trade_qty(self, symbol: str, premium: float) -> int:
        lot_size = self.symbol_map[symbol]["lot_size"]
        alloc = self.risk.trading_capital * self.risk.capital_allocation_pct * self.risk.aggression_level
        cost_per_lot = max(1.0, premium * lot_size)
        lots = max(1, int(alloc // cost_per_lot))
        return lots * lot_size

    async def _place_entry(self, symbol: str, force: bool = False, quantity_override: Optional[int] = None) -> Dict[str, Any]:
        snapshot = self.snapshots[symbol]
        signal = self.last_signal.get(symbol, {})
        conditions = [
            signal.get("features", {}).get("momentum_score", 0.0) > 0.12,
            signal.get("features", {}).get("delta_velocity", 0.0) > 0.0006,
            signal.get("features", {}).get("volume_acceleration", 0.0) > 0.05,
            signal.get("features", {}).get("spread_quality", 0.0) > 0.38,
            signal.get("features", {}).get("vwap_alignment", 0.0) > 0.0,
            signal.get("features", {}).get("gamma_bias", 0.0) > -0.2,
            signal.get("ai", {}).get("ai_confidence", 0.0) >= self.risk.ai_threshold,
            signal.get("tqs", 0.0) >= self.risk.ai_threshold,
            signal.get("microstructure", {}).get("liquidity_sweep_confirmation", 0.0) >= 1.0,
            signal.get("regime") in {
                MarketRegime.TRENDING.value,
                MarketRegime.BREAKOUT.value,
                MarketRegime.EXPANDING_VOLATILITY.value,
            },
        ]
        can_trade, reason = self._allowed_to_trade()
        if not force and (not can_trade or not all(conditions)):
            return {"ok": False, "reason": reason if not can_trade else "ENTRY_CONDITIONS_NOT_MET"}
        if not snapshot.call_instrument_key or not snapshot.call_ltp:
            return {"ok": False, "reason": "CALL_INSTRUMENT_UNAVAILABLE"}
        if self._symbol_exposure_pct(symbol) > self.risk.max_symbol_concentration_pct:
            return {"ok": False, "reason": "SYMBOL_CONCENTRATION_LIMIT"}

        qty = quantity_override or self._trade_qty(symbol, snapshot.call_ltp)
        if qty <= 0:
            return {"ok": False, "reason": "QUANTITY_COMPUTED_ZERO"}
        spread = max(0.0, (snapshot.call_ask or snapshot.call_ltp) - (snapshot.call_bid or snapshot.call_ltp))
        explosive = signal.get("features", {}).get("breakout_velocity", 0.0) > 0.008
        order_type = "MARKET" if explosive else "LIMIT"
        if spread > 1.8:
            return {"ok": False, "reason": "SPREAD_TOO_WIDE"}
        if signal.get("ai", {}).get("expected_slippage", 0.0) > self.risk.slippage_kill_switch_points:
            return {"ok": False, "reason": "SLIPPAGE_GUARD_TRIGGERED"}

        limit_px = float(snapshot.call_ask or snapshot.call_ltp)
        if order_type == "LIMIT":
            limit_px = float(limit_px + max(0.05, spread * 0.25))
        payload = {
            "quantity": int(qty),
            "product": "I",
            "validity": "IOC",
            "price": round(limit_px, 2) if order_type == "LIMIT" else 0,
            "tag": "PRO_SCALPER",
            "instrument_token": snapshot.call_instrument_key,
            "order_type": order_type,
            "transaction_type": "BUY",
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False,
            "slice": True,
        }
        t0 = time.time()
        try:
            response = await asyncio.to_thread(self.client.place_order, payload)
            latency_ms = (time.time() - t0) * 1000.0
            self.performance["fill_count"] += 1
            self.performance["execution_latency_history"].append(latency_ms)
            entry_px = float(snapshot.call_ltp)
            trade_id = f"{symbol}-{int(time.time() * 1000)}"
            trade = TradeRecord(
                trade_id=trade_id,
                symbol=symbol,
                instrument_token=snapshot.call_instrument_key,
                quantity=int(qty),
                side="BUY",
                entry_price=entry_px,
                entry_ts=time.time(),
                max_price=entry_px,
                stop_loss=max(0.05, entry_px - 3.0),
                target=entry_px + 5.0,
                tqs=float(signal.get("tqs", 0.0)),
                ai_confidence=float(signal.get("ai", {}).get("ai_confidence", 0.0)),
                regime=signal.get("regime", "UNKNOWN"),
                order_id=str(response.get("data", {}).get("order_id", "")),
            )
            self.active_trades[trade_id] = trade
            return {"ok": True, "trade": trade.__dict__, "response": response}
        except Exception as exc:
            self.performance["rejection_count"] += 1
            return {"ok": False, "reason": f"ORDER_REJECTED: {exc}"}

    async def _place_exit(self, trade: TradeRecord, reason: str, exit_quantity: Optional[int] = None) -> None:
        snapshot = self.snapshots[trade.symbol]
        qty_to_exit = int(min(trade.quantity, exit_quantity or trade.quantity))
        if qty_to_exit <= 0:
            return
        spread = max(0.0, (snapshot.call_ask or snapshot.call_ltp or 0.0) - (snapshot.call_bid or snapshot.call_ltp or 0.0))
        order_type = "MARKET" if spread > 1.0 else "LIMIT"
        px = float(snapshot.call_bid or snapshot.call_ltp or trade.entry_price)
        payload = {
            "quantity": qty_to_exit,
            "product": "I",
            "validity": "IOC",
            "price": round(px, 2) if order_type == "LIMIT" else 0,
            "tag": "PRO_SCALPER_EXIT",
            "instrument_token": trade.instrument_token,
            "order_type": order_type,
            "transaction_type": "SELL",
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False,
            "slice": True,
        }
        t0 = time.time()
        try:
            await asyncio.to_thread(self.client.place_order, payload)
            latency_ms = (time.time() - t0) * 1000.0
            exit_pnl = (px - trade.entry_price) * qty_to_exit
            self.performance["realized_pnl"] += exit_pnl
            partial = qty_to_exit < trade.quantity
            slippage = max(0.0, abs(px - (snapshot.call_bid or snapshot.call_ltp or 0.0)))
            self.performance["slippage_history"].append(slippage)

            if partial:
                trade.quantity -= qty_to_exit
                trade.partial_exit_done = True
                trade.trailing_state = "PARTIAL_PROFIT_LOCK"
                trade.stop_loss = max(trade.stop_loss, trade.entry_price + 0.5)
                partial_record = TradeRecord(
                    trade_id=f"{trade.trade_id}-partial-{int(time.time() * 1000)}",
                    symbol=trade.symbol,
                    instrument_token=trade.instrument_token,
                    quantity=qty_to_exit,
                    side=trade.side,
                    entry_price=trade.entry_price,
                    entry_ts=trade.entry_ts,
                    max_price=trade.max_price,
                    stop_loss=trade.stop_loss,
                    target=trade.target,
                    tqs=trade.tqs,
                    ai_confidence=trade.ai_confidence,
                    regime=trade.regime,
                    trailing_state="PARTIAL_EXIT",
                    partial_exit_done=True,
                    exit_price=px,
                    exit_ts=time.time(),
                    exit_reason=reason,
                    order_id=trade.order_id,
                    pnl=exit_pnl,
                )
                self.db.write_trade(partial_record, slippage, latency_ms)
                self.closed_trades.append(partial_record)
                return

            trade.exit_price = px
            trade.exit_ts = time.time()
            trade.exit_reason = reason
            trade.pnl = exit_pnl
            if trade.pnl > 0:
                self.performance["wins"] += 1
                self.performance["loss_streak"] = 0
                self.model.append_observation(self.last_signal.get(trade.symbol, {}).get("features", {}), label=1)
            else:
                self.performance["losses"] += 1
                self.performance["loss_streak"] += 1
                self.model.append_observation(self.last_signal.get(trade.symbol, {}).get("features", {}), label=0)
                if self.performance["loss_streak"] >= 2:
                    self.performance["cooldown_until"] = time.time() + self.risk.loss_cooldown_minutes * 60
            self.performance["daily_peak_equity"] = max(
                self.performance["daily_peak_equity"],
                self.risk.trading_capital + self.performance["realized_pnl"],
            )
            equity_now = self.risk.trading_capital + self.performance["realized_pnl"]
            peak = max(self.performance["daily_peak_equity"], 1.0)
            self.performance["daily_drawdown_pct"] = max(0.0, (peak - equity_now) / peak)
            self.db.write_trade(trade, slippage, latency_ms)
            self.closed_trades.append(trade)
            self.active_trades.pop(trade.trade_id, None)
        except Exception as exc:
            logger.error("Exit order failed for %s: %s", trade.trade_id, exc)
            self.performance["rejection_count"] += 1

    async def _manage_trailing(self) -> None:
        for trade in list(self.active_trades.values()):
            snapshot = self.snapshots[trade.symbol]
            ltp = snapshot.call_ltp
            if not ltp:
                continue
            trade.max_price = max(trade.max_price, ltp)
            hist = self.series[trade.symbol]["call"]
            atr = 0.8
            if len(hist) >= 14:
                tr = np.abs(np.diff(np.array(list(hist)[-14:])))
                atr = float(np.mean(tr))
            gain = ltp - trade.entry_price
            if gain >= 2.0 and trade.trailing_state == "INIT":
                trade.stop_loss = max(trade.stop_loss, trade.entry_price + 0.2)
                trade.trailing_state = "BREAKEVEN"
            if gain >= 5.0:
                dynamic_buffer = max(0.6, atr * 1.2)
                trade.stop_loss = max(trade.stop_loss, trade.max_price - dynamic_buffer)
                trade.trailing_state = "MOMENTUM_TRAIL"
            if gain >= 5.0 and not trade.partial_exit_done and trade.quantity > self.symbol_map[trade.symbol]["lot_size"]:
                lot_size = self.symbol_map[trade.symbol]["lot_size"]
                half_lots = max(1, (trade.quantity // lot_size) // 2)
                partial_qty = half_lots * lot_size
                await self._place_exit(trade, "PARTIAL_PROFIT_LOCK", exit_quantity=partial_qty)
            if gain >= 7.0 and self.last_signal.get(trade.symbol, {}).get("features", {}).get("volume_acceleration", 0.0) > 0.2:
                trade.target = max(trade.target, trade.entry_price + 9.0)
                trade.trailing_state = "TARGET_EXTENDED"

            if ltp <= trade.stop_loss:
                await self._place_exit(trade, "TRAIL_STOP_HIT")
                continue
            if ltp >= trade.target and trade.trailing_state != "TARGET_EXTENDED":
                await self._place_exit(trade, "TARGET_ACHIEVED")
                continue
            if self.runtime.safe_mode:
                await self._place_exit(trade, "SAFE_MODE_EXIT")
                continue
            if time.time() - trade.entry_ts > 180:
                await self._place_exit(trade, "TIME_STOP")

    async def _execution_loop(self) -> None:
        while True:
            try:
                await self._manage_trailing()
                if self.runtime.safe_mode:
                    await asyncio.sleep(1)
                    continue
                for symbol in ["NIFTY", "SENSEX"]:
                    open_for_symbol = [t for t in self.active_trades.values() if t.symbol == symbol]
                    if len(open_for_symbol) >= 2:
                        continue
                    signal = self.last_signal.get(symbol, {})
                    if not signal:
                        continue
                    if signal.get("tqs", 0.0) < self.risk.ai_threshold:
                        continue
                    await self._place_entry(symbol)
            except Exception as exc:
                logger.error("Execution loop failure: %s", exc)
                self.set_safe_mode(True, f"Execution failure: {exc}")
            await asyncio.sleep(1)

    def _live_unrealized(self) -> float:
        return sum(
            ((self.snapshots[trade.symbol].call_ltp or trade.entry_price) - trade.entry_price) * trade.quantity
            for trade in self.active_trades.values()
        )

    def _execution_quality(self) -> Dict[str, float]:
        rejection_rate = self.performance["rejection_count"] / max(
            self.performance["rejection_count"] + self.performance["fill_count"], 1
        )
        fill_drift = float(np.mean(self.performance["slippage_history"])) if self.performance["slippage_history"] else 0.0
        exec_latency = (
            float(np.mean(self.performance["execution_latency_history"]))
            if self.performance["execution_latency_history"]
            else 0.0
        )
        return {
            "rejection_rate": rejection_rate,
            "fill_drift": fill_drift,
            "execution_latency_ms": exec_latency,
        }

    async def _telemetry_loop(self) -> None:
        while True:
            try:
                quality = self._execution_quality()
                ws_age = time.time() - self.runtime.last_ws_message_ts if self.runtime.last_ws_message_ts else 999.0
                if self.runtime.websocket_alive and ws_age > (self.risk.stale_data_seconds + 2):
                    self.set_safe_mode(True, "Market websocket stale")
                if quality["rejection_rate"] > 0.4 or quality["fill_drift"] > self.risk.slippage_kill_switch_points:
                    self.set_safe_mode(True, "Execution quality deteriorated")
                payload = {
                    "ts": time.time(),
                    "broker_state": self.runtime.broker_state.value,
                    "safe_mode": self.runtime.safe_mode,
                    "websocket_alive": self.runtime.websocket_alive,
                    "api_latency_ms": self.runtime.api_latency_ms,
                    "websocket_latency_ms": self.runtime.websocket_latency_ms,
                    "rejection_rate": quality["rejection_rate"],
                    "fill_drift": quality["fill_drift"],
                    "drawdown_pct": self.performance["daily_drawdown_pct"],
                }
                self.db.write_telemetry(payload)
            except Exception as exc:
                logger.warning("Telemetry loop issue: %s", exc)
            await asyncio.sleep(2)

    def _backtesting_snapshot(self) -> Dict[str, Any]:
        recent = self.db.recent_ticks(limit=200)
        return {
            "total_records": len(recent),
            "recent_replay": recent[:80],
            "cursor": self.backtest_cursor,
        }

    def _dashboard_payload(self) -> Dict[str, Any]:
        unrealized = self._live_unrealized()
        self.performance["unrealized_pnl"] = unrealized
        quality = self._execution_quality()
        return {
            "timestamp": time.time(),
            "session": self.session_state().value,
            "runtime": {
                "safe_mode": self.runtime.safe_mode,
                "auto_trading_enabled": self.runtime.auto_trading_enabled and not self.runtime.safe_mode,
                "broker_state": self.runtime.broker_state.value,
                "broker_reason": self.runtime.broker_reason,
                "websocket_alive": self.runtime.websocket_alive,
                "rest_api_alive": self.runtime.rest_api_alive,
                "stale_feed": self.runtime.stale_feed,
                "api_latency_ms": self.runtime.api_latency_ms,
                "websocket_latency_ms": self.runtime.websocket_latency_ms,
                "last_rest_success_ts": self.runtime.last_rest_success_ts,
                "last_error": self.runtime.last_error,
            },
            "risk_config": self.risk.__dict__,
            "portfolio": self.portfolio_cache,
            "performance": {
                **self.performance,
                "rejection_rate": quality["rejection_rate"],
                "fill_drift": quality["fill_drift"],
                "execution_latency_ms": quality["execution_latency_ms"],
                "exposure_pct": self._portfolio_exposure_pct(),
                "cross_symbol_correlation": self._cross_symbol_correlation(),
            },
            "signals": self.last_signal,
            "session_intelligence": self.session_intelligence,
            "snapshots": {k: v.__dict__ for k, v in self.snapshots.items()},
            "active_trades": [x.__dict__ for x in self.active_trades.values()],
            "recent_trades": [x.__dict__ for x in list(self.closed_trades)[-40:]],
            "heatmaps": self.heatmaps,
            "backtesting": self._backtesting_snapshot(),
            "broker_diagnostics": self.broker_diagnostics,
        }

    async def _broadcast_loop(self) -> None:
        while True:
            payload = self._dashboard_payload()
            dead_clients: List[WebSocket] = []
            for ws in self.broadcast_clients:
                try:
                    await ws.send_json(payload)
                except Exception:
                    dead_clients.append(ws)
            for ws in dead_clients:
                self.broadcast_clients.discard(ws)
            if self.redis_client:
                try:
                    self.redis_client.publish("pro-scalper-feed", json.dumps(payload))
                except Exception:
                    pass
            await asyncio.sleep(1)

    async def register_ws(self, ws: WebSocket) -> None:
        await ws.accept()
        self.broadcast_clients.add(ws)
        await ws.send_json(self._dashboard_payload())

    def update_config(self, payload: ConfigPayload) -> Dict[str, Any]:
        data = payload.model_dump(exclude_none=True)
        if not data:
            return self.risk.__dict__
        for key, value in data.items():
            if key == "auto_trading_enabled":
                if self.runtime.safe_mode and value:
                    continue
                self.runtime.auto_trading_enabled = bool(value)
                continue
            if hasattr(self.risk, key):
                setattr(self.risk, key, value)
        return self.risk.__dict__

    async def manual_order(self, payload: OrderRequestPayload) -> Dict[str, Any]:
        symbol = payload.symbol
        if payload.order_type == "MARKET":
            self.last_signal[symbol].setdefault("features", {})["breakout_velocity"] = 0.02
        qty = payload.quantity_lots * self.symbol_map[symbol]["lot_size"]
        return await self._place_entry(symbol, force=payload.force, quantity_override=qty)


engine = ProScalperEngine()
app = FastAPI(title="PRO SCALPER", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup() -> None:
    await engine.startup()


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    payload = engine._dashboard_payload()
    return {
        "status": "ok",
        "safe_mode": payload["runtime"]["safe_mode"],
        "broker_state": payload["runtime"]["broker_state"],
        "reason": payload["runtime"]["broker_reason"],
        "api_latency_ms": payload["runtime"]["api_latency_ms"],
        "session": payload["session"],
    }


@app.get("/api/dashboard")
async def dashboard() -> Dict[str, Any]:
    return engine._dashboard_payload()


@app.post("/api/config")
async def update_config(payload: ConfigPayload) -> Dict[str, Any]:
    return {"config": engine.update_config(payload), "runtime": engine.runtime.__dict__}


@app.post("/api/order/manual")
async def manual_order(payload: OrderRequestPayload) -> Dict[str, Any]:
    result = await engine.manual_order(payload)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result


@app.post("/api/trading/{enabled}")
async def set_trading(enabled: bool) -> Dict[str, Any]:
    if enabled and engine.runtime.safe_mode:
        raise HTTPException(status_code=400, detail="Cannot enable trading while SAFE MODE is active.")
    engine.runtime.auto_trading_enabled = enabled
    return {"auto_trading_enabled": engine.runtime.auto_trading_enabled}


@app.get("/api/broker/status")
async def broker_status() -> Dict[str, Any]:
    payload = engine._dashboard_payload()
    return {
        "runtime": payload.get("runtime", {}),
        "portfolio_last_updated": payload.get("portfolio", {}).get("updated_at"),
        "funds_snapshot": payload.get("portfolio", {}).get("funds", {}),
        "broker_diagnostics": payload.get("broker_diagnostics", {}),
    }


@app.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket) -> None:
    await engine.register_ws(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        engine.broadcast_clients.discard(ws)
    except Exception:
        engine.broadcast_clients.discard(ws)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
