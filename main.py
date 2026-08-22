
"""
AI Spot Trader — Paper Trading V3

مشروع مستقل عن بوت الإشارات:
- تداول وهمي فقط.
- رأس مال 1000 USDT.
- صفقة واحدة.
- 4 دفعات × 250 USDT.
- التعزيز فقط بعد ارتداد حقيقي مؤكد.
- خروج عند +10 USDT صافي بعد الرسوم.
- بعد التعزيز: يستمر لهدف +10$ ما دام السيناريو سليمًا؛ وإلا Rescue عند صافي موجب بعد الرسوم.
- إشعارات تيليجرام للدخول والتعزيز والبيع.
- قاعدة بيانات SQLite كاملة.
- استبعاد العملات الكبيرة.
- استهداف العملات المدرجة منذ 2021.
- منع تلقائي للعملات تحت Monitoring أو Delisting مع قائمة حظر يدوية إضافية.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Dict, List, Optional, Tuple, Set

import requests


# =========================
# الإعدادات
# =========================

BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/app/data/paper_trader.sqlite3")

PAPER_BALANCE = float(os.getenv("PAPER_BALANCE", "1000"))
TRANCHE_SIZE = float(os.getenv("TRANCHE_SIZE", "250"))
MAX_TRANCHES = int(os.getenv("MAX_TRANCHES", "4"))
TARGET_NET_PROFIT = float(os.getenv("TARGET_NET_PROFIT", "10"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.001"))
STRATEGY_VERSION = os.getenv("STRATEGY_VERSION", "V3")
RESCUE_NET_BUFFER = float(os.getenv("RESCUE_NET_BUFFER", "0.50"))

# دخول مبكر / منع مطاردة العملات بعد الانطلاق
ENTRY_RSI_MIN = float(os.getenv("ENTRY_RSI_MIN", "46"))
ENTRY_RSI_MAX = float(os.getenv("ENTRY_RSI_MAX", "63"))
MAX_ENTRY_EXTENSION_ATR = float(os.getenv("MAX_ENTRY_EXTENSION_ATR", "0.85"))
MAX_ENTRY_CHANGE_1H_PCT = float(os.getenv("MAX_ENTRY_CHANGE_1H_PCT", "5.0"))
MAX_ENTRY_CHANGE_15M_PCT = float(os.getenv("MAX_ENTRY_CHANGE_15M_PCT", "2.2"))
BREAKOUT_NEAR_PCT = float(os.getenv("BREAKOUT_NEAR_PCT", "3.0"))
BREAKOUT_MAX_ABOVE_PCT = float(os.getenv("BREAKOUT_MAX_ABOVE_PCT", "0.8"))
MIN_VOLUME_BUILD = float(os.getenv("MIN_VOLUME_BUILD", "1.00"))

# أمان السوق: BTC + BTC Dominance
BTC_MIN_TREND_SCORE = float(os.getenv("BTC_MIN_TREND_SCORE", "55"))
BTC_MAX_DOMINANCE = float(os.getenv("BTC_MAX_DOMINANCE", "62.0"))
BTC_MAX_DOMINANCE_RISE_1H = float(os.getenv("BTC_MAX_DOMINANCE_RISE_1H", "0.30"))
COINGECKO_GLOBAL_URL = os.getenv("COINGECKO_GLOBAL_URL", "https://api.coingecko.com/api/v3/global")
MARKET_CONTEXT_CACHE_SECONDS = int(os.getenv("MARKET_CONTEXT_CACHE_SECONDS", "300"))

# التعزيز الذكي ووضع الإنقاذ
SMART_AVERAGE_MIN_REBOUND = float(os.getenv("SMART_AVERAGE_MIN_REBOUND", "80"))
SMART_AVERAGE_MIN_COIN_SCORE = float(os.getenv("SMART_AVERAGE_MIN_COIN_SCORE", "60"))
RESCUE_COIN_SCORE = float(os.getenv("RESCUE_COIN_SCORE", "48"))
RESCUE_TREND_15M = float(os.getenv("RESCUE_TREND_15M", "43"))
RESCUE_TREND_1H = float(os.getenv("RESCUE_TREND_1H", "43"))

# حماية Monitoring / Delisting من Binance
RISK_CACHE_SECONDS = int(os.getenv("RISK_CACHE_SECONDS", "300"))
# هذه المسارات غير موثقة حاليًا ضمن Binance Spot API العام وتعيد HTTP 400 بدون اعتماد مناسب.
# نتركها اختيارية بدل إغراق اللوق بالأخطاء، مع استمرار الحظر اليدوي وفحص حالة TRADING من exchangeInfo.
BINANCE_RISK_ENDPOINTS_ENABLED = os.getenv("BINANCE_RISK_ENDPOINTS_ENABLED", "0") == "1"

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "30"))
MIN_QUOTE_VOLUME_24H = float(os.getenv("MIN_QUOTE_VOLUME_24H", "500000"))
MAX_SYMBOLS_PER_SCAN = int(os.getenv("MAX_SYMBOLS_PER_SCAN", "200"))
MIN_LISTING_YEAR = int(os.getenv("MIN_LISTING_YEAR", "2021"))
MIN_ENTRY_SCORE = float(os.getenv("MIN_ENTRY_SCORE", "72"))
MIN_REBOUND_SCORE = float(os.getenv("MIN_REBOUND_SCORE", "68"))
AVERAGE_COOLDOWN_MINUTES = int(os.getenv("AVERAGE_COOLDOWN_MINUTES", "45"))
POST_EXIT_COOLDOWN_MINUTES = int(os.getenv("POST_EXIT_COOLDOWN_MINUTES", "60"))
MIN_AVERAGE_DROP_PCT = float(os.getenv("MIN_AVERAGE_DROP_PCT", "2.0"))
MIN_AVERAGE_REBOUND_SCORE = float(os.getenv("MIN_AVERAGE_REBOUND_SCORE", "74"))
MIN_AVERAGE_COIN_SCORE = float(os.getenv("MIN_AVERAGE_COIN_SCORE", "55"))
COMMAND_POLL_SECONDS = float(os.getenv("COMMAND_POLL_SECONDS", "2"))
TELEGRAM_COMMANDS_ENABLED = os.getenv("TELEGRAM_COMMANDS_ENABLED", "1") == "1"
TELEGRAM_OFFSET_FILE = os.getenv("TELEGRAM_OFFSET_FILE", "/app/data/paper_trader_telegram_offset.json")

# إنشاء مجلد التخزين الدائم بعد تعريف جميع المسارات
for _persistent_path in (DATABASE_PATH, TELEGRAM_OFFSET_FILE):
    _persistent_dir = os.path.dirname(_persistent_path)
    if _persistent_dir:
        os.makedirs(_persistent_dir, exist_ok=True)

# التعلم الذاتي من نتائج Paper Trading
LEARNING_ENABLED = os.getenv("LEARNING_ENABLED", "1") == "1"
LEARNING_MIN_TRADES = int(os.getenv("LEARNING_MIN_TRADES", "8"))
LEARNING_WINDOW = int(os.getenv("LEARNING_WINDOW", "500"))
LEARNING_MAX_ADJUST = float(os.getenv("LEARNING_MAX_ADJUST", "6"))

# فلتر السوق المرن
MIN_MARKET_SCORE = float(os.getenv("MIN_MARKET_SCORE", "50"))
EXCEPTIONAL_MARKET_FLOOR = float(os.getenv("EXCEPTIONAL_MARKET_FLOOR", "48"))
EXCEPTIONAL_COIN_SCORE = float(os.getenv("EXCEPTIONAL_COIN_SCORE", "90"))

# حماية Binance وتقليل الضغط على API
BINANCE_MAX_RETRIES = int(os.getenv("BINANCE_MAX_RETRIES", "5"))
BINANCE_BACKOFF_BASE = float(os.getenv("BINANCE_BACKOFF_BASE", "1.5"))
MAX_ANALYZE_PER_CYCLE = int(os.getenv("MAX_ANALYZE_PER_CYCLE", "60"))
KLINE_CACHE_5M_SECONDS = int(os.getenv("KLINE_CACHE_5M_SECONDS", "45"))
KLINE_CACHE_15M_SECONDS = int(os.getenv("KLINE_CACHE_15M_SECONDS", "90"))
KLINE_CACHE_1H_SECONDS = int(os.getenv("KLINE_CACHE_1H_SECONDS", "240"))
EXCHANGE_INFO_CACHE_SECONDS = int(os.getenv("EXCHANGE_INFO_CACHE_SECONDS", "300"))
TICKER_24H_CACHE_SECONDS = int(os.getenv("TICKER_24H_CACHE_SECONDS", "30"))


STABLE_BASES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "USDS"}
EXCLUDED_MAJORS = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT",
    "SOLUSDT", "ADAUSDT", "DOGEUSDT",
}


# =========================
# قاعدة البيانات
# =========================

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    symbol TEXT NOT NULL,
    market_score REAL NOT NULL,
    coin_score REAL NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    total_cost REAL NOT NULL DEFAULT 0,
    total_qty REAL NOT NULL DEFAULT 0,
    avg_price REAL NOT NULL DEFAULT 0,
    tranches INTEGER NOT NULL DEFAULT 0,
    realized_pnl REAL,
    exit_reason TEXT,
    max_unrealized_pnl REAL NOT NULL DEFAULT 0,
    min_unrealized_pnl REAL NOT NULL DEFAULT 0,
    last_buy_at TEXT,
    last_buy_candle_time INTEGER NOT NULL DEFAULT 0,
    strategy_version TEXT NOT NULL DEFAULT 'V2',
    rescue_mode INTEGER NOT NULL DEFAULT 0,
    rescue_reason TEXT
);

CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    ts TEXT NOT NULL,
    side TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    quote_amount REAL NOT NULL,
    fee REAL NOT NULL,
    reason TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    event_key TEXT UNIQUE NOT NULL,
    sent INTEGER NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS excluded_symbols (
    symbol TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS learning_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    trade_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    pnl REAL NOT NULL,
    won INTEGER NOT NULL,
    market_score REAL NOT NULL,
    coin_score REAL NOT NULL,
    rsi_15m REAL NOT NULL,
    volume_5m REAL NOT NULL,
    volume_15m REAL NOT NULL,
    trend_5m REAL NOT NULL,
    trend_15m REAL NOT NULL,
    trend_1h REAL NOT NULL,
    structure_score REAL NOT NULL,
    extension_atr REAL NOT NULL,
    strategy_version TEXT NOT NULL DEFAULT 'V2',
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    btc_price REAL NOT NULL,
    btc_trend_score REAL NOT NULL,
    btc_change_1h REAL NOT NULL,
    btc_dominance REAL,
    btc_dominance_change_1h REAL,
    market_safe INTEGER NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    trade_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    price REAL,
    pnl REAL,
    payload TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, path: str):
        self.path = path
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(trades)")}
            if "last_buy_at" not in columns:
                conn.execute("ALTER TABLE trades ADD COLUMN last_buy_at TEXT")
            if "last_buy_candle_time" not in columns:
                conn.execute(
                    "ALTER TABLE trades ADD COLUMN last_buy_candle_time INTEGER NOT NULL DEFAULT 0"
                )
            if "strategy_version" not in columns:
                conn.execute("ALTER TABLE trades ADD COLUMN strategy_version TEXT NOT NULL DEFAULT 'V2'")
            if "rescue_mode" not in columns:
                conn.execute("ALTER TABLE trades ADD COLUMN rescue_mode INTEGER NOT NULL DEFAULT 0")
            if "rescue_reason" not in columns:
                conn.execute("ALTER TABLE trades ADD COLUMN rescue_reason TEXT")
            learning_columns = {row["name"] for row in conn.execute("PRAGMA table_info(learning_snapshots)")}
            if "strategy_version" not in learning_columns:
                conn.execute("ALTER TABLE learning_snapshots ADD COLUMN strategy_version TEXT NOT NULL DEFAULT 'V2'")

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def add_analysis(
        self,
        symbol: str,
        market_score: float,
        coin_score: float,
        decision: str,
        reason: str,
        payload: Dict,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO analyses
                (ts,symbol,market_score,coin_score,decision,reason,payload)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    utc_now(),
                    symbol,
                    market_score,
                    coin_score,
                    decision,
                    reason,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def get_open_trade(self):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM trades WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
            ).fetchone()

    def create_trade(self, symbol: str) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT INTO trades(symbol,status,opened_at,strategy_version) VALUES(?,'OPEN',?,?)",
                (symbol, utc_now(), STRATEGY_VERSION),
            )
            return int(cur.lastrowid)

    def add_fill(
        self,
        trade_id: int,
        side: str,
        symbol: str,
        price: float,
        qty: float,
        quote_amount: float,
        fee: float,
        reason: str,
        payload: Dict,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO fills
                (trade_id,ts,side,symbol,price,qty,quote_amount,fee,reason,payload)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    trade_id,
                    utc_now(),
                    side,
                    symbol,
                    price,
                    qty,
                    quote_amount,
                    fee,
                    reason,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def refresh_trade(self, trade_id: int) -> None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN side='BUY' THEN quote_amount ELSE 0 END),0) AS cost,
                    COALESCE(SUM(CASE WHEN side='BUY' THEN qty ELSE -qty END),0) AS qty,
                    COALESCE(SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END),0) AS tranches
                FROM fills WHERE trade_id=?
                """,
                (trade_id,),
            ).fetchone()
            total_cost = float(row["cost"])
            total_qty = float(row["qty"])
            avg_price = total_cost / total_qty if total_qty > 0 else 0.0
            conn.execute(
                """
                UPDATE trades
                SET total_cost=?,total_qty=?,avg_price=?,tranches=?
                WHERE id=?
                """,
                (total_cost, total_qty, avg_price, int(row["tranches"]), trade_id),
            )

    def close_trade(self, trade_id: int, pnl: float, reason: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE trades
                SET status='CLOSED',closed_at=?,realized_pnl=?,exit_reason=?
                WHERE id=?
                """,
                (utc_now(), pnl, reason, trade_id),
            )

    def update_excursions(self, trade_id: int, pnl: float) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE trades
                SET max_unrealized_pnl=MAX(max_unrealized_pnl,?),
                    min_unrealized_pnl=MIN(min_unrealized_pnl,?)
                WHERE id=?
                """,
                (pnl, pnl, trade_id),
            )

    def is_excluded(self, symbol: str) -> Optional[str]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT reason FROM excluded_symbols WHERE symbol=?",
                (symbol,),
            ).fetchone()
            return str(row["reason"]) if row else None

    def set_excluded(
        self,
        symbol: str,
        reason: str,
        source: str = "manual",
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO excluded_symbols(symbol,reason,source,updated_at)
                VALUES (?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    reason=excluded.reason,
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                (symbol.upper(), reason, source, utc_now()),
            )

    def remove_excluded(self, symbol: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM excluded_symbols WHERE symbol=?",
                (symbol.upper(),),
            )

    def list_excluded(self):
        with self.connect() as conn:
            return conn.execute(
                "SELECT symbol,reason,source,updated_at FROM excluded_symbols ORDER BY symbol"
            ).fetchall()

    def set_runtime(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO runtime_settings(key,value,updated_at)
                VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key, value, utc_now()),
            )

    def get_runtime(self, key: str, default: str = "") -> str:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value FROM runtime_settings WHERE key=?",
                (key,),
            ).fetchone()
            return str(row["value"]) if row else default

    def mark_buy_state(self, trade_id: int, candle_time: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE trades
                SET last_buy_at=?, last_buy_candle_time=?
                WHERE id=?
                """,
                (utc_now(), int(candle_time), trade_id),
            )

    def set_rescue_mode(self, trade_id: int, reason: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE trades SET rescue_mode=1,rescue_reason=? WHERE id=?",
                (reason, trade_id),
            )

    def add_trade_event(
        self,
        trade_id: int,
        symbol: str,
        event_type: str,
        price: Optional[float],
        pnl: Optional[float],
        payload: Dict,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO trade_events(ts,trade_id,symbol,strategy_version,event_type,price,pnl,payload)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    utc_now(), trade_id, symbol, STRATEGY_VERSION, event_type,
                    price, pnl, json.dumps(payload, ensure_ascii=False),
                ),
            )

    def add_market_snapshot(self, context: Dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO market_snapshots(
                    ts,strategy_version,btc_price,btc_trend_score,btc_change_1h,
                    btc_dominance,btc_dominance_change_1h,market_safe,payload
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    utc_now(), STRATEGY_VERSION,
                    float(context.get("btc_price", 0) or 0),
                    float(context.get("btc_trend_score", 0) or 0),
                    float(context.get("btc_change_1h", 0) or 0),
                    context.get("btc_dominance"),
                    context.get("btc_dominance_change_1h"),
                    int(bool(context.get("market_safe"))),
                    json.dumps(context, ensure_ascii=False),
                ),
            )

    def dominance_about_an_hour_ago(self) -> Optional[float]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT btc_dominance FROM market_snapshots
                WHERE btc_dominance IS NOT NULL
                  AND julianday(ts) <= julianday('now','-45 minutes')
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
            return float(row["btc_dominance"]) if row and row["btc_dominance"] is not None else None

    def trade_stats(self):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END),0) AS wins,
                    COALESCE(SUM(CASE WHEN realized_pnl = 0 THEN 1 ELSE 0 END),0) AS breakeven,
                    COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END),0) AS losses,
                    COALESCE(SUM(realized_pnl),0) AS net_pnl,
                    COALESCE(AVG(realized_pnl),0) AS avg_pnl
                FROM trades WHERE status='CLOSED'
                """
            ).fetchone()

    def notification_seen(self, event_key: str) -> bool:
        with self.connect() as conn:
            return (
                conn.execute(
                    "SELECT 1 FROM notifications WHERE event_key=? AND sent=1",
                    (event_key,),
                ).fetchone()
                is not None
            )

    def save_notification(self, event_key: str, payload: Dict, sent: bool) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO notifications(ts,event_key,sent,payload)
                VALUES (?,?,?,?)
                ON CONFLICT(event_key) DO UPDATE SET
                    ts=excluded.ts, sent=excluded.sent, payload=excluded.payload
                """,
                (
                    utc_now(),
                    event_key,
                    int(sent),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def get_first_buy_payload(self, trade_id: int) -> Dict:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT payload FROM fills
                WHERE trade_id=? AND side='BUY'
                ORDER BY id ASC LIMIT 1
                """,
                (trade_id,),
            ).fetchone()
            if not row:
                return {}
            try:
                return json.loads(row["payload"])
            except Exception:
                return {}

    def add_learning_snapshot(
        self,
        trade_id: int,
        symbol: str,
        pnl: float,
        market_score: float,
        coin_score: float,
        payload: Dict,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO learning_snapshots(
                    ts,trade_id,symbol,pnl,won,market_score,coin_score,
                    rsi_15m,volume_5m,volume_15m,trend_5m,trend_15m,trend_1h,
                    structure_score,extension_atr,strategy_version,payload
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    utc_now(),
                    trade_id,
                    symbol,
                    pnl,
                    1 if pnl > 0 else 0,
                    market_score,
                    coin_score,
                    float(payload.get("rsi_15m", 0) or 0),
                    float(payload.get("volume_5m", 0) or 0),
                    float(payload.get("volume_15m", 0) or 0),
                    float(payload.get("trend_5m", 0) or 0),
                    float(payload.get("trend_15m", 0) or 0),
                    float(payload.get("trend_1h", 0) or 0),
                    float(payload.get("structure_score", 0) or 0),
                    float(payload.get("extension_atr", 0) or 0),
                    str(payload.get("strategy_version", STRATEGY_VERSION)),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def recent_learning(self, limit: int, strategy_version: Optional[str] = None):
        with self.connect() as conn:
            if strategy_version:
                return conn.execute(
                    """
                    SELECT * FROM learning_snapshots
                    WHERE strategy_version=?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (strategy_version, limit),
                ).fetchall()
            return conn.execute(
                """
                SELECT * FROM learning_snapshots
                ORDER BY id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()


# =========================
# Binance Public API
# =========================

class BinancePublic:
    def __init__(self):
        self.session = requests.Session()
        self._cache: Dict[str, tuple] = {}

    def _cache_get(self, key: str, ttl: int):
        item = self._cache.get(key)
        if not item:
            return None
        saved_at, value = item
        if time.time() - saved_at <= ttl:
            return value
        self._cache.pop(key, None)
        return None

    def _cache_set(self, key: str, value):
        self._cache[key] = (time.time(), value)
        return value

    def get(self, path: str, params: Optional[Dict] = None):
        last_error = None

        for attempt in range(BINANCE_MAX_RETRIES):
            try:
                response = self.session.get(
                    BINANCE_BASE.rstrip("/") + path,
                    params=params,
                    timeout=20,
                )

                if response.status_code in {418, 429}:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        wait_seconds = float(retry_after) if retry_after else 0.0
                    except (TypeError, ValueError):
                        wait_seconds = 0.0

                    if wait_seconds <= 0:
                        wait_seconds = BINANCE_BACKOFF_BASE * (2 ** attempt)

                    print(
                        f"Binance rate limit {response.status_code}; "
                        f"backoff {wait_seconds:.1f}s",
                        flush=True,
                    )
                    time.sleep(min(wait_seconds, 60))
                    last_error = RuntimeError(
                        f"Binance rate limit {response.status_code}"
                    )
                    continue

                if 500 <= response.status_code < 600:
                    wait_seconds = BINANCE_BACKOFF_BASE * (2 ** attempt)
                    time.sleep(min(wait_seconds, 30))
                    last_error = RuntimeError(
                        f"Binance server error {response.status_code}"
                    )
                    continue

                response.raise_for_status()
                return response.json()

            except requests.RequestException as exc:
                last_error = exc
                if attempt >= BINANCE_MAX_RETRIES - 1:
                    break
                wait_seconds = BINANCE_BACKOFF_BASE * (2 ** attempt)
                time.sleep(min(wait_seconds, 30))

        raise RuntimeError(f"تعذر الاتصال بـ Binance بعد عدة محاولات: {last_error}")

    def get_absolute(self, url: str, params: Optional[Dict] = None):
        last_error = None
        for attempt in range(BINANCE_MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=20)
                if response.status_code in {418, 429} or 500 <= response.status_code < 600:
                    wait_seconds = BINANCE_BACKOFF_BASE * (2 ** attempt)
                    time.sleep(min(wait_seconds, 30))
                    last_error = RuntimeError(f"HTTP {response.status_code}")
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= BINANCE_MAX_RETRIES - 1:
                    break
                time.sleep(min(BINANCE_BACKOFF_BASE * (2 ** attempt), 30))
        raise RuntimeError(f"تعذر الاتصال بالمصدر الخارجي: {last_error}")

    def spot_asset_tags(self, tag: str = "Monitoring") -> Set[str]:
        key = f"spot_asset_tags:{tag.lower()}"
        cached = self._cache_get(key, RISK_CACHE_SECONDS)
        if cached is not None:
            return cached

        if not BINANCE_RISK_ENDPOINTS_ENABLED:
            return self._cache_set(key, set())

        try:
            data = self.get("/sapi/v1/spot/asset/tags", {"tag": tag})
        except Exception:
            # لا نكرر خطأ HTTP 400 في اللوق؛ الحظر اليدوي وفحص exchangeInfo يظلان فعالين.
            return self._cache_set(key, set())

        rows = data.get("data", data) if isinstance(data, dict) else data
        assets: Set[str] = set()
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, str):
                    assets.add(row.upper())
                elif isinstance(row, dict):
                    value = row.get("asset") or row.get("symbol") or row.get("coin")
                    if value:
                        assets.add(str(value).upper())
        return self._cache_set(key, assets)

    def delist_symbols(self) -> Set[str]:
        key = "spot_delist_schedule"
        cached = self._cache_get(key, RISK_CACHE_SECONDS)
        if cached is not None:
            return cached

        if not BINANCE_RISK_ENDPOINTS_ENABLED:
            return self._cache_set(key, set())

        try:
            data = self.get("/sapi/v1/spot/delist-schedule")
        except Exception:
            # لا نكرر خطأ HTTP 400 في اللوق؛ العملات غير TRADING تُستبعد أصلًا من exchangeInfo.
            return self._cache_set(key, set())

        rows = data.get("data", data) if isinstance(data, dict) else data
        symbols: Set[str] = set()
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                vals = row.get("symbols") or row.get("symbol") or []
                if isinstance(vals, str):
                    vals = [vals]
                for value in vals:
                    symbols.add(str(value).upper())
        return self._cache_set(key, symbols)

    def btc_dominance(self) -> float:
        key = "btc_dominance"
        cached = self._cache_get(key, MARKET_CONTEXT_CACHE_SECONDS)
        if cached is not None:
            return float(cached)
        data = self.get_absolute(COINGECKO_GLOBAL_URL)
        value = float(data["data"]["market_cap_percentage"]["btc"])
        return float(self._cache_set(key, value))

    def exchange_info(self):
        key = "exchange_info"
        cached = self._cache_get(key, EXCHANGE_INFO_CACHE_SECONDS)
        if cached is not None:
            return cached
        return self._cache_set(key, self.get("/api/v3/exchangeInfo"))

    def tickers_24h(self):
        key = "tickers_24h"
        cached = self._cache_get(key, TICKER_24H_CACHE_SECONDS)
        if cached is not None:
            return cached
        return self._cache_set(key, self.get("/api/v3/ticker/24hr"))

    def price(self, symbol: str) -> float:
        return float(
            self.get("/api/v3/ticker/price", {"symbol": symbol})["price"]
        )

    def _kline_ttl(self, interval: str) -> int:
        if interval == "5m":
            return KLINE_CACHE_5M_SECONDS
        if interval == "15m":
            return KLINE_CACHE_15M_SECONDS
        if interval == "1h":
            return KLINE_CACHE_1H_SECONDS
        return 300

    def klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 200,
        start_time: Optional[int] = None,
    ) -> List[Dict]:
        cache_key = f"k:{symbol}:{interval}:{limit}:{start_time}"
        ttl = 86400 if start_time == 0 else self._kline_ttl(interval)
        cached = self._cache_get(cache_key, ttl)
        if cached is not None:
            return cached

        params: Dict = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time

        result = []
        for row in self.get("/api/v3/klines", params):
            result.append(
                {
                    "open_time": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "close_time": int(row[6]),
                }
            )

        return self._cache_set(cache_key, result)

    def listing_year(self, symbol: str) -> int:
        rows = self.klines(symbol, "1d", limit=1, start_time=0)
        if not rows:
            return 9999
        dt = datetime.fromtimestamp(
            rows[0]["open_time"] / 1000,
            tz=timezone.utc,
        )
        return dt.year


# =========================
# المؤشرات
# =========================

def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    alpha = 2 / (period + 1)
    current = values[0]
    for value in values[1:]:
        current = alpha * value + (1 - alpha) * current
    return current


def rsi(values: List[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains = []
    losses = []
    for a, b in zip(values[-period - 1 : -1], values[-period:]):
        delta = b - a
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def atr(candles: List[Dict], period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    values = []
    for previous, current in zip(
        candles[-period - 1 : -1],
        candles[-period:],
    ):
        values.append(
            max(
                current["high"] - current["low"],
                abs(current["high"] - previous["close"]),
                abs(current["low"] - previous["close"]),
            )
        )
    return mean(values)


def volume_ratio(candles: List[Dict], lookback: int = 20) -> float:
    if len(candles) < lookback + 1:
        return 0.0
    baseline = mean(c["volume"] for c in candles[-lookback - 1 : -1])
    return candles[-1]["volume"] / baseline if baseline > 0 else 0.0


def trend_score(values: List[float], fast: int = 9, slow: int = 21) -> float:
    # تقييم تدريجي أكثر ثباتًا: اتجاه EMA + زخم قصير، مع تقليل حساسية القفزات اللحظية.
    if len(values) < slow + 6:
        return 50.0
    fast_ema = ema(values, fast)
    slow_ema = ema(values, slow)
    distance_pct = (fast_ema / slow_ema - 1) * 100 if slow_ema else 0.0
    momentum_pct = (values[-1] / values[-6] - 1) * 100 if values[-6] else 0.0
    score = 50.0 + distance_pct * 10.0 + momentum_pct * 7.0
    return max(0.0, min(100.0, score))


# =========================
# التحليل
# =========================

@dataclass
class Analysis:
    symbol: str
    price: float
    market_score: float
    coin_score: float
    entry_ok: bool
    rebound_ok: bool
    reason: str
    payload: Dict
    candle_time: int


@dataclass
class MarketContext:
    score: float
    btc_price: float
    btc_trend_score: float
    btc_change_1h: float
    btc_dominance: Optional[float]
    btc_dominance_change_1h: Optional[float]
    market_safe: bool
    reason: str


def pct_change(new: float, old: float) -> float:
    if not old:
        return 0.0
    return (new / old - 1.0) * 100.0


def calculate_market_score(btc_15m: List[Dict], btc_1h: List[Dict]) -> float:
    score_15m = trend_score([c["close"] for c in btc_15m])
    score_1h = trend_score([c["close"] for c in btc_1h])
    return round(score_15m * 0.45 + score_1h * 0.55, 1)


def analyze_symbol(
    symbol: str,
    candles_5m: List[Dict],
    candles_15m: List[Dict],
    candles_1h: List[Dict],
    market_score: float,
    learned_entry_score: float,
) -> Analysis:
    price = candles_15m[-1]["close"]
    closes_5m = [c["close"] for c in candles_5m]
    closes_15m = [c["close"] for c in candles_15m]
    closes_1h = [c["close"] for c in candles_1h]

    rsi_15m = rsi(closes_15m)
    volume_5m = volume_ratio(candles_5m)
    volume_15m = volume_ratio(candles_15m)
    trend_5m = trend_score(closes_5m)
    trend_15m = trend_score(closes_15m)
    trend_1h = trend_score(closes_1h)

    change_15m_pct = pct_change(price, closes_15m[-2]) if len(closes_15m) >= 2 else 0.0
    change_1h_pct = pct_change(closes_5m[-1], closes_5m[-13]) if len(closes_5m) >= 13 else 0.0
    recent_high = max(c["high"] for c in candles_15m[-21:-1]) if len(candles_15m) >= 22 else price
    distance_to_breakout_pct = pct_change(recent_high, price)
    breakout_position_ok = (-BREAKOUT_MAX_ABOVE_PCT <= distance_to_breakout_pct <= BREAKOUT_NEAR_PCT)
    recent_volumes = [c["volume"] for c in candles_5m[-7:-1]]
    older_volumes = [c["volume"] for c in candles_5m[-19:-7]]
    volume_build = (mean(recent_volumes) / mean(older_volumes)) if recent_volumes and older_volumes and mean(older_volumes) > 0 else 0.0

    atr_15m = atr(candles_15m)
    ema20 = ema(closes_15m, 20)
    extension_atr = (price - ema20) / atr_15m if atr_15m > 0 else 0

    # بنية تدريجية بدل 100/35 الثنائية؛ تكافئ ترتيب المتوسطات وموقع السعر بدون قفزة حادة.
    ema9 = ema(closes_15m, 9)
    ema21 = ema(closes_15m, 21)
    structure_score = 50.0
    if ema9 > ema21:
        structure_score += 25.0
    else:
        structure_score -= 15.0
    if price > ema20:
        structure_score += 20.0
    else:
        structure_score -= 10.0
    if ema21:
        structure_score += max(-10.0, min(10.0, (ema9 / ema21 - 1) * 500.0))
    structure_score = max(0.0, min(100.0, structure_score))

    coin_score = (
        trend_5m * 0.15
        + trend_15m * 0.30
        + trend_1h * 0.25
        + min(100, volume_15m * 45) * 0.15
        + structure_score * 0.15
    )

    market_ok = bool(
        market_score >= MIN_MARKET_SCORE
        or (
            market_score >= EXCEPTIONAL_MARKET_FLOOR
            and coin_score >= EXCEPTIONAL_COIN_SCORE
        )
    )

    entry_ok = bool(
        market_ok
        and coin_score >= learned_entry_score
        and ENTRY_RSI_MIN <= rsi_15m <= ENTRY_RSI_MAX
        and -0.75 <= extension_atr <= MAX_ENTRY_EXTENSION_ATR
        and change_1h_pct <= MAX_ENTRY_CHANGE_1H_PCT
        and change_15m_pct <= MAX_ENTRY_CHANGE_15M_PCT
        and breakout_position_ok
        and volume_build >= MIN_VOLUME_BUILD
        and max(volume_5m, volume_15m) >= 1.00
    )

    # ارتداد حقيقي:
    # 1) كان السعر قريبًا/تحت EMA20.
    # 2) استعاد EMA20 بشمعة خضراء.
    # 3) RSI يتحسن.
    # 4) الحجم عاد.
    recent_was_below = any(
        candle["close"] < ema20
        for candle in candles_15m[-6:-1]
    )
    reclaim = (
        price > ema20
        and candles_15m[-1]["close"] > candles_15m[-1]["open"]
    )
    rsi_rising = rsi(closes_15m[:-1]) < rsi_15m

    rebound_score = (
        trend_5m * 0.25
        + trend_15m * 0.25
        + min(100, max(volume_5m, volume_15m) * 45) * 0.25
        + (100 if reclaim else 0) * 0.25
    )

    rebound_ok = bool(
        market_score >= 50
        and rebound_score >= MIN_REBOUND_SCORE
        and recent_was_below
        and reclaim
        and rsi_rising
        and max(volume_5m, volume_15m) >= 1.10
    )

    reason = (
        "دخول أول عالي الجودة"
        if entry_ok
        else "ارتداد حقيقي مؤكد"
        if rebound_ok
        else "انتظار"
    )

    payload = {
        "market_score": round(market_score, 1),
        "market_ok": market_ok,
        "coin_score": round(coin_score, 1),
        "rsi_15m": round(rsi_15m, 2),
        "volume_5m": round(volume_5m, 2),
        "volume_15m": round(volume_15m, 2),
        "trend_5m": round(trend_5m, 1),
        "trend_15m": round(trend_15m, 1),
        "trend_1h": round(trend_1h, 1),
        "structure_score": round(structure_score, 1),
        "extension_atr": round(extension_atr, 2),
        "rebound_score": round(rebound_score, 1),
        "change_15m_pct": round(change_15m_pct, 2),
        "change_1h_pct": round(change_1h_pct, 2),
        "recent_high": recent_high,
        "distance_to_breakout_pct": round(distance_to_breakout_pct, 2),
        "volume_build": round(volume_build, 2),
        "strategy_version": STRATEGY_VERSION,
    }

    return Analysis(
        symbol=symbol,
        price=price,
        market_score=market_score,
        coin_score=round(coin_score, 1),
        entry_ok=entry_ok,
        rebound_ok=rebound_ok,
        reason=reason,
        payload=payload,
        candle_time=int(candles_15m[-1]["open_time"]),
    )


def entry_rejection_reasons(analysis: Analysis, learned_min: float) -> List[str]:
    reasons: List[str] = []
    p = analysis.payload

    market_ok = bool(
        analysis.market_score >= MIN_MARKET_SCORE
        or (
            analysis.market_score >= EXCEPTIONAL_MARKET_FLOOR
            and analysis.coin_score >= EXCEPTIONAL_COIN_SCORE
        )
    )
    if not market_ok:
        if analysis.coin_score >= EXCEPTIONAL_COIN_SCORE:
            reasons.append(
                f"السوق {analysis.market_score:.1f}<{EXCEPTIONAL_MARKET_FLOOR:.0f} حتى للاستثنائية"
            )
        else:
            reasons.append(
                f"السوق {analysis.market_score:.1f}<{MIN_MARKET_SCORE:.0f}"
            )
    if analysis.coin_score < learned_min:
        reasons.append(f"التقييم {analysis.coin_score:.1f}<{learned_min:.1f}")
    rsi_value = float(p.get("rsi_15m", 0))
    if not (ENTRY_RSI_MIN <= rsi_value <= ENTRY_RSI_MAX):
        reasons.append(f"RSI {rsi_value:.1f} خارج نطاق الدخول المبكر")
    ext = float(p.get("extension_atr", 0))
    if not (-0.75 <= ext <= MAX_ENTRY_EXTENSION_ATR):
        reasons.append(f"امتداد غير مناسب عن EMA ({ext:.2f} ATR)")
    if float(p.get("change_1h_pct", 0)) > MAX_ENTRY_CHANGE_1H_PCT:
        reasons.append(f"ارتفعت {float(p.get('change_1h_pct', 0)):.1f}% خلال ساعة")
    if float(p.get("change_15m_pct", 0)) > MAX_ENTRY_CHANGE_15M_PCT:
        reasons.append(f"شمعة 15m ممتدة {float(p.get('change_15m_pct', 0)):.1f}%")
    dist = float(p.get("distance_to_breakout_pct", 999))
    if not (-BREAKOUT_MAX_ABOVE_PCT <= dist <= BREAKOUT_NEAR_PCT):
        reasons.append(f"ليست في منطقة ما قبل الاختراق ({dist:.1f}%)")
    if float(p.get("volume_build", 0)) < MIN_VOLUME_BUILD:
        reasons.append(f"تجميع الحجم غير كافٍ {float(p.get('volume_build', 0)):.2f}x")

    return reasons or ["مؤهلة للدخول"]


class AdaptiveLearner:
    """
    تعلم محافظ: لا يغير إدارة رأس المال أو عدد الدفعات.
    فقط يعدل حد تقييم الدخول بعد وجود سجل كافٍ من الصفقات الوهمية.
    """

    def __init__(self, database: Database):
        self.db = database

    def effective_entry_score(self) -> float:
        if not LEARNING_ENABLED:
            return MIN_ENTRY_SCORE

        rows = self.db.recent_learning(LEARNING_WINDOW, STRATEGY_VERSION)
        if len(rows) < LEARNING_MIN_TRADES:
            return MIN_ENTRY_SCORE

        wins = [r for r in rows if int(r["won"]) == 1]
        losses = [r for r in rows if int(r["won"]) == 0]
        win_rate = len(wins) / len(rows)

        adjust = 0.0

        # إذا كانت النتائج ضعيفة، يرفع صرامة الدخول.
        if win_rate < 0.45:
            adjust += min(LEARNING_MAX_ADJUST, (0.45 - win_rate) * 20)

        # إذا كانت النتائج قوية ومستقرة، يسمح بهامش بسيط فقط.
        elif win_rate > 0.70 and len(wins) >= LEARNING_MIN_TRADES // 2:
            adjust -= min(3.0, (win_rate - 0.70) * 10)

        # مقارنة متوسط جودة الصفقات الرابحة والخاسرة.
        if wins and losses:
            avg_win_score = mean(float(r["coin_score"]) for r in wins)
            avg_loss_score = mean(float(r["coin_score"]) for r in losses)
            if avg_win_score > avg_loss_score + 4:
                adjust += min(2.0, (avg_win_score - avg_loss_score) / 10)

        return round(max(66.0, min(82.0, MIN_ENTRY_SCORE + adjust)), 1)

    def record_closed_trade(self, position, pnl: float) -> None:
        if not LEARNING_ENABLED:
            return

        trade_id = int(position["id"])
        payload = self.db.get_first_buy_payload(trade_id)
        if not payload:
            return

        self.db.add_learning_snapshot(
            trade_id=trade_id,
            symbol=str(position["symbol"]),
            pnl=float(pnl),
            market_score=float(payload.get("market_score", 0) or 0),
            coin_score=float(payload.get("coin_score", 0) or 0),
            payload=payload,
        )


# =========================
# التداول الوهمي
# =========================

class PaperBroker:
    def __init__(self, database: Database):
        self.db = database

    def position(self):
        return self.db.get_open_trade()

    def buy(
        self,
        symbol: str,
        price: float,
        reason: str,
        payload: Dict,
    ):
        position = self.position()

        if position and position["symbol"] != symbol:
            raise RuntimeError("يوجد مركز مفتوح لعملة أخرى")

        if position and int(position["tranches"]) >= MAX_TRANCHES:
            raise RuntimeError("تم استخدام جميع الدفعات")

        trade_id = (
            int(position["id"])
            if position
            else self.db.create_trade(symbol)
        )

        quote_amount = TRANCHE_SIZE
        fee = quote_amount * FEE_RATE
        quantity = (quote_amount - fee) / price

        self.db.add_fill(
            trade_id,
            "BUY",
            symbol,
            price,
            quantity,
            quote_amount,
            fee,
            reason,
            payload,
        )
        self.db.refresh_trade(trade_id)
        self.db.mark_buy_state(
            trade_id,
            int(payload.get("candle_time", payload.get("open_time", 0)) or 0),
        )
        updated = self.position()
        self.db.add_trade_event(
            trade_id, symbol, "BUY", price, self.pnl(updated, price) if updated else None,
            {**payload, "reason": reason, "tranches": int(updated["tranches"]) if updated else 0},
        )
        return updated

    def pnl(self, position, price: float) -> float:
        gross_value = float(position["total_qty"]) * price
        sell_fee = gross_value * FEE_RATE
        return gross_value - sell_fee - float(position["total_cost"])

    def target_price(self, position, target_profit: float) -> float:
        quantity = float(position["total_qty"])
        return (
            float(position["total_cost"]) + target_profit
        ) / (quantity * (1 - FEE_RATE))

    def sell_all(
        self,
        position,
        price: float,
        reason: str,
        payload: Dict,
    ) -> float:
        pnl = self.pnl(position, price)
        quantity = float(position["total_qty"])
        gross_value = quantity * price
        fee = gross_value * FEE_RATE

        self.db.add_fill(
            int(position["id"]),
            "SELL",
            str(position["symbol"]),
            price,
            quantity,
            gross_value,
            fee,
            reason,
            payload,
        )
        self.db.close_trade(int(position["id"]), pnl, reason)
        self.db.add_trade_event(
            int(position["id"]), str(position["symbol"]), "SELL", price, pnl,
            {**payload, "reason": reason},
        )
        self.db.set_runtime("last_exit_at", utc_now())
        return pnl


# =========================
# تيليجرام
# =========================

class Notifier:
    def __init__(self, database: Database):
        self.db = database

    def send_once(
        self,
        event_key: str,
        text: str,
        payload: Dict,
    ) -> bool:
        if self.db.notification_seen(event_key):
            return False

        sent = True

        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            url = (
                f"https://api.telegram.org/"
                f"bot{TELEGRAM_TOKEN}/sendMessage"
            )
            response = requests.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text,
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
            sent = response.ok
        else:
            print(text, flush=True)

        self.db.save_notification(event_key, payload, sent)
        return sent


# =========================
# فلتر العملات
# =========================

class Universe:
    def __init__(self, api: BinancePublic, database: Database):
        self.api = api
        self.db = database
        self.listing_year_cache: Dict[str, int] = {}
        self.scan_cursor = 0

    def risk_reason(self, symbol: str) -> Optional[str]:
        manual = self.db.is_excluded(symbol)
        if manual:
            return f"قائمة الحظر: {manual}"

        base = symbol.upper().removesuffix("USDT")
        try:
            monitoring = self.api.spot_asset_tags("Monitoring")
            if base in monitoring or symbol.upper() in monitoring:
                return "Binance Monitoring Tag"
        except Exception:
            pass

        try:
            delisting = self.api.delist_symbols()
            if symbol.upper() in delisting or base in delisting:
                return "Binance Delisting Schedule"
        except Exception:
            pass
        return None

    def candidates(self):
        exchange_info = self.api.exchange_info()

        tradable = {
            item["symbol"]
            for item in exchange_info["symbols"]
            if item["status"] == "TRADING"
            and item["quoteAsset"] == "USDT"
            and item.get("isSpotTradingAllowed", False)
            and item["baseAsset"] not in STABLE_BASES
            and item["symbol"] not in EXCLUDED_MAJORS
        }

        rows = []

        for ticker in self.api.tickers_24h():
            symbol = ticker.get("symbol")
            if symbol not in tradable:
                continue

            if self.risk_reason(symbol):
                continue

            quote_volume = float(ticker.get("quoteVolume", 0))
            if quote_volume < MIN_QUOTE_VOLUME_24H:
                continue

            rows.append((symbol, quote_volume))

        rows.sort(key=lambda item: item[1], reverse=True)
        return rows[:MAX_SYMBOLS_PER_SCAN]

    def scan_batch(self):
        candidates = self.candidates()
        if not candidates:
            return []

        batch_size = min(MAX_ANALYZE_PER_CYCLE, len(candidates))
        start = self.scan_cursor % len(candidates)
        end = start + batch_size

        if end <= len(candidates):
            batch = candidates[start:end]
        else:
            batch = candidates[start:] + candidates[: end - len(candidates)]

        self.scan_cursor = (start + batch_size) % len(candidates)
        return batch

    def listing_year_ok(self, symbol: str) -> bool:
        if symbol not in self.listing_year_cache:
            try:
                self.listing_year_cache[symbol] = self.api.listing_year(symbol)
            except Exception:
                return False

        return self.listing_year_cache[symbol] >= MIN_LISTING_YEAR



# =========================
# أوامر تيليجرام
# =========================

class TelegramCommands:
    def __init__(
        self,
        database: Database,
        api_client: BinancePublic,
        paper_broker: PaperBroker,
        universe_obj: Universe,
        learner_obj: AdaptiveLearner,
    ):
        self.db = database
        self.api = api_client
        self.broker = paper_broker
        self.universe = universe_obj
        self.learner = learner_obj
        self.offset = self._load_offset()

    def _load_offset(self) -> int:
        try:
            with open(TELEGRAM_OFFSET_FILE, "r", encoding="utf-8") as handle:
                return int(json.load(handle).get("offset", 0))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return 0

    def _save_offset(self) -> None:
        try:
            with open(TELEGRAM_OFFSET_FILE, "w", encoding="utf-8") as handle:
                json.dump({"offset": self.offset}, handle)
        except OSError:
            pass

    def _reply(self, text: str) -> None:
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            print(text, flush=True)
            return
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=20,
        )

    def _status_text(self) -> str:
        position = self.broker.position()
        state = "متوقف مؤقتًا ⏸️" if bot_paused() else "يعمل 🟢"
        if not position:
            trade_text = "لا توجد صفقة مفتوحة."
        else:
            try:
                price = self.api.price(str(position["symbol"]))
                pnl = self.broker.pnl(position, price)
                target = self.broker.target_price(position, TARGET_NET_PROFIT)
                trade_text = (
                    f"الصفقة: {position['symbol']}\n"
                    f"الدفعات: {position['tranches']}/{MAX_TRANCHES}\n"
                    f"المستخدم: {position['total_cost']:.2f} USDT\n"
                    f"المتوسط: {position['avg_price']:.8f}\n"
                    f"الربح الحالي: {pnl:+.2f} USDT\n"
                    f"هدف +10$: {target:.8f}\n"
                    f"وضع الإنقاذ: {'نعم 🛟' if int(position['rescue_mode'] or 0) else 'لا'}"
                )
            except Exception as exc:
                trade_text = f"تعذر قراءة الصفقة: {exc}"
        cooldown = "نعم" if post_exit_cooldown_active() else "لا"
        return f"🤖 حالة البوت: {state}\n• حد الدخول المتعلم: {self.learner.effective_entry_score():.1f}/100\n• انتظار بعد آخر بيع: {cooldown}\n\n{trade_text}"

    def _stats_text(self) -> str:
        row = self.db.trade_stats()
        position = self.broker.position()

        if position:
            try:
                current_price = self.api.price(str(position["symbol"]))
                current_pnl = self.broker.pnl(position, current_price)
                pnl_label = "الربح الحالي" if current_pnl >= 0 else "الخسارة الحالية"
                open_text = (
                    f"\n\n🟢 الصفقات المفتوحة: 1\n"
                    f"• العملة: {position['symbol']}\n"
                    f"• الدفعات: {position['tranches']}/{MAX_TRANCHES}\n"
                    f"• المستخدم: {position['total_cost']:.2f} USDT\n"
                    f"• متوسط الدخول: {position['avg_price']:.8f}\n"
                    f"• السعر الحالي: {current_price:.8f}\n"
                    f"• {pnl_label}: {current_pnl:+.2f} USDT"
                )
            except Exception as exc:
                open_text = (
                    f"\n\n🟢 الصفقات المفتوحة: 1\n"
                    f"• تعذر قراءة تفاصيل الصفقة: {exc}"
                )
        else:
            open_text = "\n\n⚪ الصفقات المفتوحة: 0"

        return (
            "📊 إحصائيات Paper Trading\n\n"
            f"• الصفقات المغلقة: {row['total']}\n"
            f"• الرابحة: {row['wins']}\n"
            f"• التعادل: {row['breakeven']}\n"
            f"• الخاسرة: {row['losses']}\n"
            f"• صافي الربح: {row['net_pnl']:+.2f} USDT\n"
            f"• متوسط الصفقة: {row['avg_pnl']:+.2f} USDT"
            + open_text
        )

    def _scan_text(self) -> str:
        try:
            market_score = get_market_score()
            learned_min = self.learner.effective_entry_score()
            candidates = self.universe.candidates()
            checked = 0
            rejected_year = 0
            errors = 0
            rows = []

            for symbol, quote_volume in candidates:
                try:
                    if not self.universe.listing_year_ok(symbol):
                        rejected_year += 1
                        continue

                    analysis = get_analysis(symbol, market_score)
                    checked += 1
                    reasons = entry_rejection_reasons(analysis, learned_min)
                    rows.append(
                        (
                            analysis.coin_score,
                            symbol,
                            analysis,
                            reasons,
                            quote_volume,
                        )
                    )
                except Exception:
                    errors += 1

            rows.sort(key=lambda item: item[0], reverse=True)
            top = rows[:10]

            lines = [
                "🔎 تشخيص الفحص الحالي",
                "",
                f"• تقييم السوق: {market_score:.1f}/100",
                f"• حد السوق الطبيعي: {MIN_MARKET_SCORE:.0f}/100",
                f"• استثناء القوة: {EXCEPTIONAL_COIN_SCORE:.0f}+ مع سوق {EXCEPTIONAL_MARKET_FLOOR:.0f}+",
                f"• حد الدخول المتعلم: {learned_min:.1f}/100",
                f"• مرشحو السيولة: {len(candidates)}",
                f"• تم تحليلهم: {checked}",
                f"• مرفوضون بسبب سنة الإدراج: {rejected_year}",
                f"• أخطاء API/تحليل: {errors}",
                "",
                "🏆 أفضل 10 عملات حاليًا:",
            ]

            if not top:
                lines.append("لا توجد عملات قابلة للتحليل الآن.")
            else:
                for i, (_score, symbol, analysis, reasons, quote_volume) in enumerate(top, 1):
                    status = "✅ دخول" if analysis.entry_ok else "⏳ انتظار"
                    reason_text = "، ".join(reasons[:3])
                    lines.append(
                        f"{i}) {symbol} — {analysis.coin_score:.1f}/100 — {status}\n"
                        f"   {reason_text}"
                    )

            return "\n".join(lines)
        except Exception as exc:
            return f"تعذر تنفيذ /scan: {exc}"

    def handle(self, text: str) -> None:
        raw = text.strip()
        if not raw:
            return
        parts = raw.split()
        command = parts[0].lower().replace("@", " @").split()[0]

        if command in {"/help", "/مساعدة"}:
            self._reply(
                "📋 جميع أوامر البوت\n\n"
                "▶️ /start أو /resume أو /تشغيل — تشغيل الدخول والتعزيز\n"
                "⏸️ /stop أو /pause أو /إيقاف — إيقاف الدخول والتعزيز مؤقتًا مع استمرار متابعة الصفقة المفتوحة والخروج الآمن\n"
                "🤖 /status أو /الحالة — عرض حالة البوت والصفقة الحالية\n"
                "📈 /trade أو /الصفقة — عرض حالة الصفقة الحالية\n"
                "📊 /stats أو /الإحصائيات — عرض إحصائيات التداول والصفقة المفتوحة\n"
                "🔎 /scan أو /فحص — فحص السوق وعرض أفضل المرشحين وأسباب الرفض\n"
                "🚫 /exclude SYMBOL السبب — إضافة عملة إلى قائمة الاستبعاد\n"
                "✅ /include SYMBOL — إزالة عملة من قائمة الاستبعاد\n"
                "📃 /excluded — عرض العملات المستبعدة\n"
                "❓ /help أو /مساعدة — عرض جميع الأوامر"
            )
        elif command in {"/status", "/الحالة", "/trade", "/الصفقة"}:
            self._reply(self._status_text())
        elif command in {"/stats", "/الإحصائيات"}:
            self._reply(self._stats_text())
        elif command in {"/scan", "/فحص"}:
            self._reply(self._scan_text())
        elif command in {"/start", "/resume", "/تشغيل"}:
            self.db.set_runtime("paused", "0")
            self._reply("▶️ تم تشغيل البوت واستئناف الدخول والتعزيز.")
        elif command in {"/pause", "/stop", "/إيقاف"}:
            self.db.set_runtime("paused", "1")
            self._reply("⏸️ تم إيقاف الدخول والتعزيز. متابعة الصفقة المفتوحة والخروج الآمن مستمران.")
        elif command == "/exclude":
            if len(parts) < 2:
                self._reply("الاستخدام: /exclude ABCUSDT Monitoring")
                return
            symbol = parts[1].upper()
            reason = " ".join(parts[2:]).strip() or "Monitoring/Delisting"
            self.db.set_excluded(symbol, reason, "telegram")
            self._reply(f"🚫 تم استبعاد {symbol}\nالسبب: {reason}")
        elif command == "/include":
            if len(parts) < 2:
                self._reply("الاستخدام: /include ABCUSDT")
                return
            symbol = parts[1].upper()
            self.db.remove_excluded(symbol)
            self._reply(f"✅ أزيل {symbol} من قائمة الاستبعاد.")
        elif command == "/excluded":
            rows = self.db.list_excluded()
            if not rows:
                self._reply("قائمة الاستبعاد فارغة.")
            else:
                lines = [f"• {row['symbol']}: {row['reason']}" for row in rows[:50]]
                self._reply("🚫 العملات المستبعدة\n\n" + "\n".join(lines))

    def poll_once(self) -> None:
        if not TELEGRAM_COMMANDS_ENABLED or not TELEGRAM_TOKEN:
            return
        response = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": self.offset, "timeout": 0},
            timeout=20,
        )
        response.raise_for_status()
        for update in response.json().get("result", []):
            self.offset = max(self.offset, int(update["update_id"]) + 1)
            message = update.get("message") or {}
            chat = str((message.get("chat") or {}).get("id", ""))
            text = str(message.get("text", ""))
            if TELEGRAM_CHAT_ID and chat != str(TELEGRAM_CHAT_ID):
                continue
            self.handle(text)
        self._save_offset()


# =========================
# المحرك الرئيسي
# =========================

db = Database(DATABASE_PATH)
api = BinancePublic()
broker = PaperBroker(db)
notifier = Notifier(db)
universe = Universe(api, db)
learner = AdaptiveLearner(db)
commands = TelegramCommands(db, api, broker, universe, learner)


_market_context_cache: Optional[Tuple[float, MarketContext]] = None

def get_market_context() -> MarketContext:
    global _market_context_cache
    if _market_context_cache and time.time() - _market_context_cache[0] <= MARKET_CONTEXT_CACHE_SECONDS:
        return _market_context_cache[1]

    btc_15m = api.klines("BTCUSDT", "15m", limit=120)
    btc_1h = api.klines("BTCUSDT", "1h", limit=120)
    score = calculate_market_score(btc_15m, btc_1h)
    btc_price = float(btc_15m[-1]["close"])
    btc_trend = trend_score([c["close"] for c in btc_1h])
    btc_change_1h = pct_change(btc_15m[-1]["close"], btc_15m[-5]["close"]) if len(btc_15m) >= 5 else 0.0

    dominance: Optional[float] = None
    dominance_change: Optional[float] = None
    dominance_error = ""
    try:
        dominance = api.btc_dominance()
        previous = db.dominance_about_an_hour_ago()
        dominance_change = dominance - previous if previous is not None else 0.0
    except Exception as exc:
        dominance_error = str(exc)

    btc_ok = btc_trend >= BTC_MIN_TREND_SCORE and btc_change_1h >= -0.20
    dominance_ok = (
        dominance is not None
        and dominance <= BTC_MAX_DOMINANCE
        and (dominance_change is None or dominance_change <= BTC_MAX_DOMINANCE_RISE_1H)
    )
    market_safe = bool(btc_ok and dominance_ok)

    reasons = []
    if not btc_ok:
        reasons.append(f"BTC غير آمن: trend={btc_trend:.1f}, 1h={btc_change_1h:+.2f}%")
    if dominance is None:
        reasons.append(f"تعذر قراءة BTC.D: {dominance_error}")
    elif dominance > BTC_MAX_DOMINANCE:
        reasons.append(f"BTC.D مرتفعة {dominance:.2f}%")
    elif dominance_change is not None and dominance_change > BTC_MAX_DOMINANCE_RISE_1H:
        reasons.append(f"BTC.D ترتفع +{dominance_change:.2f} نقطة/ساعة")
    reason = "السوق آمن نسبيًا للألتكوين" if market_safe else " | ".join(reasons)

    context = MarketContext(
        score=score, btc_price=btc_price, btc_trend_score=btc_trend,
        btc_change_1h=btc_change_1h, btc_dominance=dominance,
        btc_dominance_change_1h=dominance_change, market_safe=market_safe, reason=reason,
    )
    db.add_market_snapshot({
        "btc_price": btc_price, "btc_trend_score": btc_trend, "btc_change_1h": btc_change_1h,
        "btc_dominance": dominance, "btc_dominance_change_1h": dominance_change,
        "market_safe": market_safe, "market_score": score, "reason": reason,
    })
    _market_context_cache = (time.time(), context)
    return context


def get_market_score() -> float:
    return get_market_context().score


def get_analysis(symbol: str, market_score: float) -> Analysis:
    analysis = analyze_symbol(
        symbol,
        api.klines(symbol, "5m", limit=120),
        api.klines(symbol, "15m", limit=160),
        api.klines(symbol, "1h", limit=140),
        market_score,
        learner.effective_entry_score(),
    )
    analysis.payload["candle_time"] = analysis.candle_time
    market = get_market_context()
    analysis.payload.update({
        "market_safe": market.market_safe,
        "btc_price": market.btc_price,
        "btc_trend_score": round(market.btc_trend_score, 1),
        "btc_change_1h": round(market.btc_change_1h, 2),
        "btc_dominance": market.btc_dominance,
        "btc_dominance_change_1h": market.btc_dominance_change_1h,
        "market_reason": market.reason,
        "strategy_version": STRATEGY_VERSION,
    })
    # أمان السوق شرط إلزامي للدخول الأول في V3.
    analysis.entry_ok = bool(analysis.entry_ok and market.market_safe)
    return analysis


def send_buy_message(position, analysis: Analysis, averaging: bool) -> None:
    title = "🟡 تعزيز وهمي" if averaging else "🟢 دخول وهمي"
    target = broker.target_price(position, TARGET_NET_PROFIT)

    fill_price = float(analysis.price)
    fill_fee = TRANCHE_SIZE * FEE_RATE
    fill_qty = (TRANCHE_SIZE - fill_fee) / fill_price
    total_qty = float(position["total_qty"])
    base_asset = str(position["symbol"]).removesuffix("USDT")

    if averaging:
        trade_details = (
            f"• قيمة التعزيز: {TRANCHE_SIZE:.2f} USDT\n"
            f"• سعر التعزيز: {fill_price:.8f}\n"
            f"• كمية التعزيز: {fill_qty:.8f} {base_asset}\n"
            f"• إجمالي الكمية: {total_qty:.8f} {base_asset}\n"
            f"• عدد الدفعات: {position['tranches']}/{MAX_TRANCHES}\n"
            f"• متوسط الدخول الجديد: {position['avg_price']:.8f}\n"
            f"• إجمالي المستخدم: {position['total_cost']:.2f} USDT\n"
            f"• هدف +10$ الصافي: {target:.8f}\n"
            f"• تقييم السوق: {analysis.market_score:.1f}/100\n"
            f"• تقييم العملة: {analysis.coin_score:.1f}/100\n"
            f"• تقييم الارتداد: {float(analysis.payload.get('rebound_score', 0)):.1f}/100\n"
            f"• السبب: {analysis.reason}"
        )
    else:
        trade_details = (
            f"• الدفعة: {TRANCHE_SIZE:.2f} USDT\n"
            f"• سعر الدخول: {fill_price:.8f}\n"
            f"• كمية العملة: {fill_qty:.8f} {base_asset}\n"
            f"• إجمالي الكمية: {total_qty:.8f} {base_asset}\n"
            f"• عدد الدفعات: {position['tranches']}/{MAX_TRANCHES}\n"
            f"• إجمالي المستخدم: {position['total_cost']:.2f} USDT\n"
            f"• هدف +10$ الصافي: {target:.8f}\n"
            f"• تقييم السوق: {analysis.market_score:.1f}/100\n"
            f"• تقييم العملة: {analysis.coin_score:.1f}/100\n"
            f"• السبب: {analysis.reason}"
        )

    message = f"""{title} — {position['symbol']}

{trade_details}

🧪 Paper Trading — لا توجد أموال حقيقية."""

    notifier.send_once(
        f"BUY:{position['id']}:{position['tranches']}",
        message,
        {
            "trade_id": int(position["id"]),
            "tranches": int(position["tranches"]),
            "fill_price": fill_price,
            "fill_qty": fill_qty,
            "total_qty": total_qty,
        },
    )


def send_sell_message(
    position,
    price: float,
    pnl: float,
    reason: str,
) -> None:
    message = f"""✅ بيع وهمي — {position['symbol']}

• سعر البيع: {price:.8f}
• الربح الصافي بعد الرسوم: {pnl:+.2f} USDT
• سبب الخروج: {reason}
• عدد الدفعات: {position['tranches']}
• إجمالي المستخدم: {position['total_cost']:.2f} USDT

🔎 بدأ البحث عن صفقة جديدة.
🧪 Paper Trading — لا توجد أموال حقيقية."""

    notifier.send_once(
        f"SELL:{position['id']}",
        message,
        {
            "trade_id": int(position["id"]),
            "pnl": pnl,
            "reason": reason,
        },
    )


def bot_paused() -> bool:
    return db.get_runtime("paused", "0") == "1"


def post_exit_cooldown_active() -> bool:
    last_exit_at = db.get_runtime("last_exit_at", "")
    if not last_exit_at:
        return False
    try:
        last_dt = datetime.fromisoformat(last_exit_at)
        age_minutes = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60
        return age_minutes < POST_EXIT_COOLDOWN_MINUTES
    except ValueError:
        return False


def averaging_allowed(position, analysis: Analysis) -> bool:
    last_candle = int(position["last_buy_candle_time"] or 0)
    if analysis.candle_time and analysis.candle_time == last_candle:
        return False

    avg_price = float(position["avg_price"] or 0)
    if avg_price <= 0:
        return False
    drop_pct = max(0.0, (avg_price - analysis.price) / avg_price * 100.0)
    # كل تعزيز لاحق يحتاج خصمًا أعمق من المتوسط الجديد حتى لا تتكدس الدفعات في نفس المنطقة.
    required_drop = MIN_AVERAGE_DROP_PCT + max(0, int(position["tranches"]) - 1) * 1.25
    if drop_pct < required_drop:
        return False
    if analysis.coin_score < max(MIN_AVERAGE_COIN_SCORE, SMART_AVERAGE_MIN_COIN_SCORE):
        return False
    if float(analysis.payload.get("rebound_score", 0) or 0) < max(MIN_AVERAGE_REBOUND_SCORE, SMART_AVERAGE_MIN_REBOUND):
        return False
    if not bool(analysis.payload.get("market_safe", False)):
        return False
    # لا نعزز أثناء سقوط قوي على الساعة؛ نريد ارتدادًا حقيقيًا لا سكينًا ساقطًا.
    if float(analysis.payload.get("trend_1h", 0) or 0) < 45:
        return False
    if not (32 <= float(analysis.payload.get("rsi_15m", 50) or 50) <= 64):
        return False

    last_buy_at = position["last_buy_at"]
    if last_buy_at:
        try:
            last_dt = datetime.fromisoformat(str(last_buy_at))
            age_minutes = (
                datetime.now(timezone.utc) - last_dt
            ).total_seconds() / 60
            if age_minutes < AVERAGE_COOLDOWN_MINUTES:
                return False
        except ValueError:
            pass

    return True


def rescue_reason_for(position, analysis: Analysis) -> Optional[str]:
    reasons = []
    risk = universe.risk_reason(str(position["symbol"]))
    if risk:
        reasons.append(risk)
    market_safe = bool(analysis.payload.get("market_safe", False))
    dominance_available = analysis.payload.get("btc_dominance") is not None
    trend15 = float(analysis.payload.get("trend_15m", 50) or 50)
    trend1h = float(analysis.payload.get("trend_1h", 50) or 50)
    weak_coin = analysis.coin_score < RESCUE_COIN_SCORE
    weak_structure = trend15 < RESCUE_TREND_15M and trend1h < RESCUE_TREND_1H
    # لا نحول الصفقة إلى إنقاذ بسبب انقطاع مصدر BTC.D فقط. نحتاج ضعفًا فعليًا في العملة/السوق.
    if dominance_available and not market_safe and analysis.coin_score < 55:
        reasons.append("السوق لم يعد آمنًا مع ضعف العملة")
    if weak_coin:
        reasons.append(f"تقييم العملة هبط إلى {analysis.coin_score:.1f}")
    if weak_structure:
        reasons.append("اتجاه 15m و1h ضعيف معًا")
    return " | ".join(reasons) if reasons else None


def manage_open_position(market_score: float) -> bool:
    position = broker.position()

    if not position:
        return False

    symbol = str(position["symbol"])
    current_price = api.price(symbol)
    pnl = broker.pnl(position, current_price)

    db.update_excursions(int(position["id"]), pnl)

    # الهدف الأساسي: +10$ صافي.
    if pnl >= TARGET_NET_PROFIT:
        realized = broker.sell_all(
            position,
            current_price,
            "تحقق هدف +10$ الصافي",
            {"pnl": pnl},
        )
        send_sell_message(
            position,
            current_price,
            realized,
            "تحقق هدف +10$ الصافي",
        )
        learner.record_closed_trade(position, realized)
        return True

    analysis = get_analysis(symbol, market_score)

    db.add_analysis(
        symbol,
        market_score,
        analysis.coin_score,
        "MANAGE",
        analysis.reason,
        analysis.payload,
    )

    # لا نخرج عند التعادل لمجرد أن هناك تعزيزًا. نستمر نحو +10$ ما دام السيناريو سليمًا.
    # إذا ضعفت العملة/السوق أو أصبحت تحت Monitoring/Delisting نثبت وضع الإنقاذ.
    rescue_reason = rescue_reason_for(position, analysis)
    if not int(position["rescue_mode"] or 0) and rescue_reason:
        db.set_rescue_mode(int(position["id"]), rescue_reason)
        db.add_trade_event(
            int(position["id"]), symbol, "RESCUE_MODE", current_price, pnl,
            {"reason": rescue_reason, **analysis.payload},
        )
        position = broker.position()

    # وضع الإنقاذ: لا بيع بخسارة؛ الخروج فقط بصافي موجب بعد كل الرسوم.
    if int(position["rescue_mode"] or 0) and pnl >= RESCUE_NET_BUFFER:
        reason = f"خروج إنقاذ بصافي {RESCUE_NET_BUFFER:.2f}$ بعد الرسوم"
        realized = broker.sell_all(position, current_price, reason, {"pnl": pnl})
        send_sell_message(position, current_price, realized, reason)
        learner.record_closed_trade(position, realized)
        return True

    # التعزيز فقط بعد ارتداد حقيقي، وعلى شمعة جديدة، وبعد فترة انتظار.
    # عند دخول وضع الإنقاذ نتوقف عن إضافة رأس مال جديد.
    if (
        int(position["tranches"]) < MAX_TRANCHES
        and not int(position["rescue_mode"] or 0)
        and analysis.rebound_ok
        and not bot_paused()
        and averaging_allowed(position, analysis)
    ):
        position = broker.buy(
            symbol,
            current_price,
            "تعزيز بعد ارتداد حقيقي مؤكد",
            analysis.payload,
        )
        send_buy_message(position, analysis, averaging=True)
        return True

    return True


def scan_for_entry(market_score: float) -> None:
    if bot_paused() or post_exit_cooldown_active():
        return
    best: Optional[Analysis] = None

    for symbol, _quote_volume in universe.scan_batch():
        try:
            if not universe.listing_year_ok(symbol):
                db.add_analysis(
                    symbol,
                    market_score,
                    0,
                    "REJECT",
                    f"العملة مدرجة قبل {MIN_LISTING_YEAR}",
                    {"min_listing_year": MIN_LISTING_YEAR},
                )
                continue

            analysis = get_analysis(symbol, market_score)

            db.add_analysis(
                symbol,
                market_score,
                analysis.coin_score,
                "BUY" if analysis.entry_ok else "WAIT",
                analysis.reason,
                analysis.payload,
            )

            if analysis.entry_ok and (
                best is None
                or analysis.coin_score > best.coin_score
            ):
                best = analysis

        except Exception as exc:
            db.add_analysis(
                symbol,
                market_score,
                0,
                "ERROR",
                str(exc),
                {},
            )

    if best:
        # فحص أخير لحظة التنفيذ: لا Monitoring/Delisting ولا سوق غير آمن.
        risk = universe.risk_reason(best.symbol)
        market = get_market_context()
        if risk:
            db.add_analysis(best.symbol, market_score, best.coin_score, "BLOCK", risk, best.payload)
            print(f"Entry blocked {best.symbol}: {risk}", flush=True)
            return
        if not market.market_safe:
            print(f"Entry blocked: market unsafe | {market.reason}", flush=True)
            return
        position = broker.buy(
            best.symbol,
            best.price,
            "دخول مبكر قبل الانطلاق — V3",
            best.payload,
        )
        send_buy_message(position, best, averaging=False)
    else:
        print(
            f"Scan complete: no entry | batch={MAX_ANALYZE_PER_CYCLE} | "
            f"market={market_score:.1f}",
            flush=True,
        )


def run_forever() -> None:
    print(
        f"AI Spot Trader — Paper Trading {STRATEGY_VERSION} started | "
        f"learned entry={learner.effective_entry_score():.1f}",
        flush=True,
    )
    notifier.send_once(
        f"STARTUP:{STRATEGY_VERSION}",
        f"🤖 AI Spot Trader {STRATEGY_VERSION} بدأ العمل\n\n🧪 Paper Trading فقط — لا توجد أموال حقيقية.",
        {"version": STRATEGY_VERSION},
    )

    while True:
        started = time.time()

        try:
            commands.poll_once()
            market_context = get_market_context()
            current_market_score = market_context.score

            if not manage_open_position(current_market_score):
                scan_for_entry(current_market_score)

        except KeyboardInterrupt:
            raise
        except Exception:
            traceback.print_exc()

        elapsed = time.time() - started
        time.sleep(max(5, SCAN_SECONDS - elapsed))


if __name__ == "__main__":
    run_forever()
