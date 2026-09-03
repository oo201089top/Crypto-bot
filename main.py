
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

import base64
import json
import os
import re
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
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
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

# Binance Alpha — بيانات سوق عامة فقط، وتداولها هنا Paper Trading مثل بقية البوت
BINANCE_ALPHA_ENABLED = os.getenv("BINANCE_ALPHA_ENABLED", "1") == "1"
BINANCE_ALPHA_BASE = os.getenv("BINANCE_ALPHA_BASE", "https://www.binance.com")
BINANCE_ALPHA_CACHE_SECONDS = int(os.getenv("BINANCE_ALPHA_CACHE_SECONDS", "300"))
BINANCE_ALPHA_MAX_CANDIDATES = int(os.getenv("BINANCE_ALPHA_MAX_CANDIDATES", "60"))
BINANCE_ALPHA_MIN_QUOTE_VOLUME_24H = float(os.getenv("BINANCE_ALPHA_MIN_QUOTE_VOLUME_24H", "500000"))
# إذا تعذر معرفة سنة إدراج Alpha-only نرفضها من المضاربة بدل افتراض أنها حديثة.
ALPHA_UNKNOWN_LISTING_FAIL_CLOSED = os.getenv("ALPHA_UNKNOWN_LISTING_FAIL_CLOSED", "1") == "1"

# مشتقات — رادار مساعد لقرارات Spot/Paper فقط، ولا يفتح أي صفقة Futures.
DERIVATIVES_ENABLED = os.getenv("DERIVATIVES_ENABLED", "1") == "1"
DERIVATIVES_CACHE_SECONDS = int(os.getenv("DERIVATIVES_CACHE_SECONDS", "60"))
DERIVATIVES_MIN_TECH_SCORE = float(os.getenv("DERIVATIVES_MIN_TECH_SCORE", "64"))
DERIVATIVES_SCORE_WEIGHT = float(os.getenv("DERIVATIVES_SCORE_WEIGHT", "0.12"))
MOMENTUM_QUALITY_SCORE_WEIGHT = float(os.getenv("MOMENTUM_QUALITY_SCORE_WEIGHT", "0.06"))
DERIVATIVES_BLOCK_SCORE = float(os.getenv("DERIVATIVES_BLOCK_SCORE", "38"))

# Early Flow — التقاط بناء الحركة قبل الانطلاق، مع منع المطاردة المتأخرة.
EARLY_FLOW_ENABLED = os.getenv("EARLY_FLOW_ENABLED", "1") == "1"
EARLY_FLOW_ALERT_SCORE = float(os.getenv("EARLY_FLOW_ALERT_SCORE", "75"))
EARLY_FLOW_ENTRY_SCORE = float(os.getenv("EARLY_FLOW_ENTRY_SCORE", "82"))
EARLY_FLOW_MIN_TIMING = float(os.getenv("EARLY_FLOW_MIN_TIMING", "72"))
EARLY_FLOW_MIN_TECH_SCORE = float(os.getenv("EARLY_FLOW_MIN_TECH_SCORE", "64"))
EARLY_FLOW_ALERT_COOLDOWN_MINUTES = int(os.getenv("EARLY_FLOW_ALERT_COOLDOWN_MINUTES", "120"))

# Chase Guard — يمنع شراء الحركة بعد انطلاقها إذا اجتمع الإجهاد الفني مع ضغط مشتقات سلبي.
# لا يمنع بسبب عامل واحد منفرد حتى لا يقتل الفرص المبكرة الجيدة.
CHASE_GUARD_ENABLED = os.getenv("CHASE_GUARD_ENABLED", "1") == "1"
CHASE_RSI_1H = float(os.getenv("CHASE_RSI_1H", "75"))
CHASE_CHANGE_1H_PCT = float(os.getenv("CHASE_CHANGE_1H_PCT", "3.5"))
CHASE_NEAR_RESISTANCE_PCT = float(os.getenv("CHASE_NEAR_RESISTANCE_PCT", "0.80"))
CHASE_WEAK_15M_VOLUME = float(os.getenv("CHASE_WEAK_15M_VOLUME", "1.10"))
CHASE_DERIV_SCORE_MAX = float(os.getenv("CHASE_DERIV_SCORE_MAX", "45"))
CHASE_TAKER_MAX = float(os.getenv("CHASE_TAKER_MAX", "0.80"))
CHASE_LONG_SHORT_MIN = float(os.getenv("CHASE_LONG_SHORT_MIN", "1.60"))
CHASE_TOP_POSITIONS_MIN = float(os.getenv("CHASE_TOP_POSITIONS_MIN", "2.00"))

# التعزيز الذكي ووضع الإنقاذ
SMART_AVERAGE_MIN_REBOUND = float(os.getenv("SMART_AVERAGE_MIN_REBOUND", "80"))
SMART_AVERAGE_MIN_COIN_SCORE = float(os.getenv("SMART_AVERAGE_MIN_COIN_SCORE", "60"))
RESCUE_COIN_SCORE = float(os.getenv("RESCUE_COIN_SCORE", "48"))
RESCUE_TREND_15M = float(os.getenv("RESCUE_TREND_15M", "43"))
RESCUE_TREND_1H = float(os.getenv("RESCUE_TREND_1H", "43"))

# حماية Monitoring / Delisting من Binance
RISK_CACHE_SECONDS = int(os.getenv("RISK_CACHE_SECONDS", "300"))
# Get Spot Asset Tags هو MARKET_DATA ويحتاج X-MBX-APIKEY (لا يحتاج Secret أو توقيع).
# حماية Fail-Closed: إذا تعذر التحقق من Monitoring Tag نمنع الدخول بدل السماح بالخطأ.
BINANCE_RISK_ENDPOINTS_ENABLED = os.getenv("BINANCE_RISK_ENDPOINTS_ENABLED", "1") == "1"
RISK_FAIL_CLOSED = os.getenv("RISK_FAIL_CLOSED", "1") == "1"
RISK_CHECK_FAILED_SENTINEL = "__RISK_CHECK_FAILED__"
# حماية إضافية: يمكن إضافة رموز Monitoring يدويًا من متغير البيئة عند الحاجة.
# PORTAL مضاف افتراضيًا لأنه ظهر فعليًا تحت Monitoring بينما endpoint لم يلتقطه في النسخة السابقة.
MONITORING_FALLBACK_ASSETS = {
    x.strip().upper()
    for x in os.getenv("MONITORING_FALLBACK_ASSETS", "PORTAL").split(",")
    if x.strip()
}

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

# تحليل صور قوائم العملات المرسلة إلى تيليجرام.
# نستخدم Vision فقط لاستخراج أزواج USDT الظاهرة في الصورة، ثم كل التحليل يتم من Binance نفسه.
PHOTO_SCAN_ENABLED = os.getenv("PHOTO_SCAN_ENABLED", "1") == "1"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-5.6-luna").strip()
PHOTO_SCAN_MIN_SCORE = float(os.getenv("PHOTO_SCAN_MIN_SCORE", "65"))
PHOTO_SCAN_MAX_REPORTS = int(os.getenv("PHOTO_SCAN_MAX_REPORTS", "10"))

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

CREATE TABLE IF NOT EXISTS derivatives_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price REAL NOT NULL DEFAULT 0,
    taker_ratio REAL,
    oi_change_1h REAL,
    funding REAL,
    global_ls REAL,
    top_positions_ls REAL,
    futures_spot_ratio REAL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_derivatives_snapshots_symbol_id
ON derivatives_snapshots(symbol, id DESC);
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

    def last_derivatives_snapshot(self, symbol: str):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM derivatives_snapshots WHERE symbol=? ORDER BY id DESC LIMIT 1",
                (symbol.upper(),),
            ).fetchone()

    def add_derivatives_snapshot(self, symbol: str, price: float, payload: Dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO derivatives_snapshots(
                    ts,symbol,price,taker_ratio,oi_change_1h,funding,global_ls,
                    top_positions_ls,futures_spot_ratio,payload
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    utc_now(), symbol.upper(), float(price or 0),
                    payload.get("taker_ratio"), payload.get("oi_change_1h"),
                    payload.get("funding"), payload.get("long_short"),
                    payload.get("top_positions_ls"), payload.get("futures_spot_ratio"),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

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
        if BINANCE_API_KEY:
            self.session.headers.update({"X-MBX-APIKEY": BINANCE_API_KEY})
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

    # ---------- Binance Alpha public market data ----------
    def _alpha_get(self, path: str, params: Optional[Dict] = None):
        return self.get_absolute(BINANCE_ALPHA_BASE.rstrip("/") + path, params)

    def alpha_token_list(self) -> List[Dict]:
        if not BINANCE_ALPHA_ENABLED:
            return []
        key = "alpha_token_list"
        cached = self._cache_get(key, BINANCE_ALPHA_CACHE_SECONDS)
        if cached is not None:
            return cached
        data = self._alpha_get("/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list")
        rows = data.get("data", []) if isinstance(data, dict) else []
        if isinstance(rows, dict):
            rows = rows.get("list") or rows.get("tokens") or rows.get("rows") or []
        rows = [row for row in rows if isinstance(row, dict)]
        return self._cache_set(key, rows)

    @staticmethod
    def _alpha_trade_symbol(alpha_id: object) -> str:
        value = str(alpha_id or "").upper().strip()
        if not value:
            return ""
        if value.endswith("USDT"):
            return value
        if not value.startswith("ALPHA_"):
            value = f"ALPHA_{value}"
        return value + "USDT"

    def alpha_resolve(self, symbol: str) -> Optional[Dict]:
        base = symbol.upper().strip().removesuffix("USDT")
        if not base:
            return None
        for row in self.alpha_token_list():
            token_symbol = str(row.get("symbol") or row.get("tokenSymbol") or "").upper().strip()
            token_name = str(row.get("name") or row.get("tokenName") or "").upper().strip()
            if base not in {token_symbol, token_name}:
                continue
            alpha_id = row.get("alphaId") or row.get("alphaID") or row.get("id")
            trade_symbol = self._alpha_trade_symbol(alpha_id)
            if not trade_symbol:
                continue
            resolved = dict(row)
            resolved["display_symbol"] = (token_symbol or base) + "USDT"
            resolved["alpha_trade_symbol"] = trade_symbol
            return resolved
        return None

    def is_alpha_symbol(self, symbol: str) -> bool:
        try:
            return self.alpha_resolve(symbol) is not None
        except Exception:
            return False

    def is_spot_symbol(self, symbol: str) -> bool:
        symbol = symbol.upper().strip()
        try:
            return any(
                item.get("symbol") == symbol
                and item.get("status") == "TRADING"
                and item.get("isSpotTradingAllowed", False)
                for item in self.exchange_info().get("symbols", [])
            )
        except Exception:
            return False

    def market_membership(self, symbol: str) -> str:
        """تصنيف مكان تداول الرمز بدون خلط Spot مع Alpha."""
        spot = self.is_spot_symbol(symbol)
        alpha = self.is_alpha_symbol(symbol)
        if spot and alpha:
            return "SPOT_ALPHA"
        if spot:
            return "SPOT"
        if alpha:
            return "ALPHA_ONLY"
        return "UNKNOWN"

    def market_kind(self, symbol: str) -> str:
        """مصدر بيانات التحليل/التنفيذ الوهمي: نفضّل Spot عند توفره."""
        membership = self.market_membership(symbol)
        if membership in {"SPOT", "SPOT_ALPHA"}:
            return "SPOT"
        if membership == "ALPHA_ONLY":
            return "ALPHA"
        return "UNKNOWN"

    def alpha_klines(self, symbol: str, interval: str, limit: int = 200, start_time: Optional[int] = None) -> List[Dict]:
        resolved = self.alpha_resolve(symbol)
        if not resolved:
            raise RuntimeError(f"{symbol} ليست ضمن Binance Alpha")
        params: Dict = {"symbol": resolved["alpha_trade_symbol"], "interval": interval, "limit": min(int(limit), 1000)}
        if start_time is not None:
            params["startTime"] = int(start_time)
        payload = self._alpha_get("/bapi/defi/v1/public/alpha-trade/klines", params)
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if isinstance(rows, dict):
            rows = rows.get("list") or rows.get("rows") or []
        result: List[Dict] = []
        for row in rows or []:
            if not isinstance(row, (list, tuple)) or len(row) < 7:
                continue
            result.append({
                "open_time": int(row[0]), "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                "close_time": int(row[6]),
            })
        if not result:
            raise RuntimeError(f"لا توجد Klines متاحة لـ {symbol} على Binance Alpha")
        return result

    def alpha_ticker(self, symbol: str) -> Dict:
        resolved = self.alpha_resolve(symbol)
        if not resolved:
            raise RuntimeError(f"{symbol} ليست ضمن Binance Alpha")
        key = f"alpha_ticker:{resolved['alpha_trade_symbol']}"
        cached = self._cache_get(key, TICKER_24H_CACHE_SECONDS)
        if cached is not None:
            return cached
        payload = self._alpha_get("/bapi/defi/v1/public/alpha-trade/ticker", {"symbol": resolved["alpha_trade_symbol"]})
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        if isinstance(data, list):
            data = data[0] if data else {}
        return self._cache_set(key, data if isinstance(data, dict) else {})

    def market_price(self, symbol: str) -> float:
        if self.is_spot_symbol(symbol):
            return self.price(symbol)
        ticker = self.alpha_ticker(symbol)
        for key in ("lastPrice", "price", "close", "last"):
            value = ticker.get(key)
            if value not in (None, ""):
                return float(value)
        return float(self.alpha_klines(symbol, "1m", limit=2)[-1]["close"])

    def market_klines(self, symbol: str, interval: str, limit: int = 200, start_time: Optional[int] = None) -> List[Dict]:
        if self.is_spot_symbol(symbol):
            return self.klines(symbol, interval, limit=limit, start_time=start_time)
        return self.alpha_klines(symbol, interval, limit=limit, start_time=start_time)

    def market_ticker_24h(self, symbol: str) -> Dict:
        if self.is_spot_symbol(symbol):
            return next((x for x in self.tickers_24h() if x.get("symbol") == symbol), {})
        return self.alpha_ticker(symbol)

    def market_listing_year(self, symbol: str) -> Optional[int]:
        # Spot (ومن ضمنه Spot + Alpha): سنة الإدراج المعتمدة هي تاريخ زوج Spot.
        if self.is_spot_symbol(symbol):
            return self.listing_year(symbol)

        # Alpha-only: نحاول أخذ تاريخ موثوق من بيانات Alpha.
        resolved = self.alpha_resolve(symbol)
        if not resolved:
            return None
        for key in ("listingTime", "onlineTime", "listTime", "startTime", "createdAt"):
            value = resolved.get(key)
            if value in (None, ""):
                continue
            try:
                number = float(value)
                if number > 10_000_000_000:
                    number /= 1000.0
                year = datetime.fromtimestamp(number, tz=timezone.utc).year
                if 2009 <= year <= datetime.now(timezone.utc).year + 1:
                    return year
            except Exception:
                pass

        # لا نخترع سنة حالية لعملة Alpha-only إذا Binance لم توفر تاريخًا صالحًا.
        return None

    def alpha_candidates(self) -> List[Tuple[str, float]]:
        if not BINANCE_ALPHA_ENABLED:
            return []
        key = "alpha_candidates"
        cached = self._cache_get(key, BINANCE_ALPHA_CACHE_SECONDS)
        if cached is not None:
            return cached
        rows: List[Tuple[str, float]] = []
        tokens = self.alpha_token_list()
        # نبدأ بالأحدث إذا كان تاريخ الإدراج متاحًا، وإلا بترتيب Binance نفسه.
        def _rank(row: Dict) -> float:
            for k in ("listingTime", "onlineTime", "listTime", "startTime", "createdAt"):
                try:
                    return float(row.get(k) or 0)
                except Exception:
                    pass
            return 0.0
        ordered = sorted(tokens, key=_rank, reverse=True) if tokens else []
        for token in ordered[:BINANCE_ALPHA_MAX_CANDIDATES]:
            base = str(token.get("symbol") or token.get("tokenSymbol") or "").upper().strip()
            if not base or base in STABLE_BASES:
                continue
            logical = base + "USDT"
            try:
                # إذا كان الرمز موجودًا في Spot أيضًا فهو موجود أصلًا بقائمة Spot،
                # فلا نكرره كمرشح Alpha. هنا نضيف Alpha-only فقط.
                if self.is_spot_symbol(logical):
                    continue
                ticker = self.alpha_ticker(logical)
                qv = float(ticker.get("quoteVolume") or ticker.get("quote_volume") or 0)
                if qv <= 0:
                    vol = float(ticker.get("volume") or 0)
                    last = float(ticker.get("lastPrice") or ticker.get("price") or 0)
                    qv = vol * last
                if qv < BINANCE_ALPHA_MIN_QUOTE_VOLUME_24H:
                    continue
                rows.append((logical, qv))
            except Exception:
                continue
        rows.sort(key=lambda x: x[1], reverse=True)
        return self._cache_set(key, rows)

    def spot_asset_tags(self, tag: str = "Monitoring", force_refresh: bool = False) -> Set[str]:
        key = f"spot_asset_tags:{tag.lower()}"
        if not force_refresh:
            cached = self._cache_get(key, RISK_CACHE_SECONDS)
            if cached is not None:
                return cached

        # Binance يصنف هذا endpoint كـ MARKET_DATA، أي يحتاج API Key فقط.
        if not BINANCE_RISK_ENDPOINTS_ENABLED or not BINANCE_API_KEY:
            failed = {RISK_CHECK_FAILED_SENTINEL}
            return self._cache_set(key, failed)

        try:
            data = self.get("/sapi/v1/spot/asset/tags", {"tag": tag})
        except Exception as exc:
            print(f"تعذر التحقق من Binance {tag} Tag — سيتم منع الدخول احترازيًا: {exc}", flush=True)
            failed = {RISK_CHECK_FAILED_SENTINEL}
            return self._cache_set(key, failed)

        rows = data.get("data", data) if isinstance(data, dict) else data
        assets: Set[str] = set()

        # Binance غيّر شكل بعض الاستجابات عبر الوقت؛ نقرأ الحقول المعروفة حتى لو كانت متداخلة.
        asset_keys = {
            "asset", "symbol", "coin", "baseAsset", "assetCode",
            "token", "tokenSymbol", "code"
        }

        def collect_assets(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k in asset_keys and isinstance(v, (str, int, float)):
                        value = str(v).upper().strip()
                        if value:
                            # لو رجع زوجًا مثل PORTALUSDT نحفظ الزوج والـbase معًا.
                            assets.add(value)
                            if value.endswith("USDT") and len(value) > 4:
                                assets.add(value[:-4])
                    elif isinstance(v, (dict, list, tuple)):
                        collect_assets(v)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    collect_assets(item)
            elif isinstance(obj, str):
                value = obj.upper().strip()
                # نقبل strings المباشرة فقط إذا بدت كرمز أصل قصير.
                if value and value.replace("_", "").replace("-", "").isalnum() and len(value) <= 20:
                    assets.add(value)

        collect_assets(rows)

        # طبقة احتياطية يدوية لا تعتمد على endpoint وحده.
        if tag.lower() == "monitoring":
            assets.update(MONITORING_FALLBACK_ASSETS)

        return self._cache_set(key, assets)

    def delist_symbols(self) -> Set[str]:
        # لا نعتمد على endpoint غير موثوق لجدول الحذف.
        # العملات غير TRADING تُستبعد أصلًا من exchangeInfo،
        # والعملات المعرضة لخطر الحذف تُمنع عبر Monitoring Tag.
        return set()

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
    rsi_1h = rsi(closes_1h)
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

    entry_setup_ok = bool(
        market_ok
        and ENTRY_RSI_MIN <= rsi_15m <= ENTRY_RSI_MAX
        and -0.75 <= extension_atr <= MAX_ENTRY_EXTENSION_ATR
        and change_1h_pct <= MAX_ENTRY_CHANGE_1H_PCT
        and change_15m_pct <= MAX_ENTRY_CHANGE_15M_PCT
        and breakout_position_ok
        and volume_build >= MIN_VOLUME_BUILD
        and max(volume_5m, volume_15m) >= 1.00
    )
    entry_ok = bool(entry_setup_ok and coin_score >= learned_entry_score)

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
        "entry_setup_ok": entry_setup_ok,
        "coin_score": round(coin_score, 1),
        "rsi_15m": round(rsi_15m, 2),
        "rsi_1h": round(rsi_1h, 2),
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
    deriv = p.get("derivatives") or {}
    if p.get("derivatives_block"):
        reasons.append(f"المشتقات تميل {deriv.get('lean','SHORT')} بدرجة {float(deriv.get('derivatives_score',0)):.0f}/100")
    elif deriv.get("derivatives_score") is not None and float(deriv.get("derivatives_score")) < 45:
        reasons.append(f"المشتقات ضعيفة لصفقة Spot ({float(deriv.get('derivatives_score')):.0f}/100)")

    if p.get("chase_guard"):
        chase = p.get("chase_guard_reasons") or []
        reasons.append("Chase Guard: " + " + ".join(str(x) for x in chase[:4]))

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
        self.risk_guard = None

    def position(self):
        return self.db.get_open_trade()

    def buy(
        self,
        symbol: str,
        price: float,
        reason: str,
        payload: Dict,
    ):
        # بوابة أمان نهائية داخل طبقة التنفيذ نفسها:
        # لا يمكن إنشاء BUY حتى لو أخطأ مسار scan أو التعزيز في استدعاء فلتر المخاطر.
        if self.risk_guard is not None:
            risk = self.risk_guard(symbol, force_refresh=True)
            if risk:
                raise RuntimeError(f"BUY BLOCKED BY RISK GUARD: {risk}")

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
        self.listing_year_cache: Dict[str, Optional[int]] = {}
        self.scan_cursor = 0

    def risk_reason(self, symbol: str, force_refresh: bool = False) -> Optional[str]:
        manual = self.db.is_excluded(symbol)
        if manual:
            return f"قائمة الحظر: {manual}"

        base = symbol.upper().removesuffix("USDT")

        # Alpha-only لا يملك Spot Monitoring Tag؛ قائمة الحظر اليدوية تبقى فعالة.
        if self.api.market_kind(symbol) == "ALPHA":
            return None

        monitoring = self.api.spot_asset_tags("Monitoring", force_refresh=force_refresh)
        if RISK_CHECK_FAILED_SENTINEL in monitoring:
            if RISK_FAIL_CLOSED:
                return "تعذر التحقق من Binance Monitoring Tag — دخول ممنوع احترازيًا"
        elif base in monitoring or symbol.upper() in monitoring:
            return "Binance Monitoring Tag — دخول ممنوع"

        # الحذف الفعلي يُمنع أيضًا عبر status=TRADING في exchangeInfo داخل candidates().
        # ويمكن إضافة أي إعلان حذف يدويًا عبر قائمة الحظر دون حذف السجل التاريخي.
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

        # احتفظ بأفضل Spot أولًا، ثم أضف Alpha كحصة مستقلة حتى لا تختفي عملات Alpha
        # لمجرد أن أحجام Spot الكبيرة ملأت أول MAX_SYMBOLS_PER_SCAN مركز.
        rows.sort(key=lambda item: item[1], reverse=True)
        combined = rows[:MAX_SYMBOLS_PER_SCAN]
        seen = {symbol for symbol, _ in combined}
        try:
            for alpha_symbol, alpha_quote_volume in self.api.alpha_candidates():
                if alpha_symbol in seen or alpha_symbol in EXCLUDED_MAJORS:
                    continue
                if self.risk_reason(alpha_symbol):
                    continue
                combined.append((alpha_symbol, alpha_quote_volume))
                seen.add(alpha_symbol)
        except Exception as exc:
            print(f"تعذر تحديث Binance Alpha candidates: {exc}", flush=True)

        # scan_batch يدور على القائمة على دفعات؛ لذلك Alpha ستأخذ دورها في الفحص
        # حتى لو كان حجمها أقل من أكبر أزواج Spot.
        return combined

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

    def listing_year_value(self, symbol: str) -> Optional[int]:
        if symbol not in self.listing_year_cache:
            try:
                self.listing_year_cache[symbol] = self.api.market_listing_year(symbol)
            except Exception:
                self.listing_year_cache[symbol] = None
        return self.listing_year_cache[symbol]

    def listing_year_ok(self, symbol: str) -> bool:
        year = self.listing_year_value(symbol)
        if year is None:
            # Alpha-only بسنة مجهولة: Fail-Closed افتراضيًا.
            if self.api.market_membership(symbol) == "ALPHA_ONLY":
                return not ALPHA_UNKNOWN_LISTING_FAIL_CLOSED
            return False
        return year >= MIN_LISTING_YEAR



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

        # Telegram يقبل حتى 4096 حرفًا تقريبًا في الرسالة؛ نقسم التقارير الطويلة بأمان.
        remaining = str(text or "")
        chunks = []
        while len(remaining) > 3900:
            cut = remaining.rfind("\n", 0, 3900)
            if cut < 1200:
                cut = 3900
            chunks.append(remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        if remaining:
            chunks.append(remaining)

        for chunk in chunks or [""]:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk},
                timeout=20,
            )

    def _status_text(self) -> str:
        position = self.broker.position()
        state = "متوقف مؤقتًا ⏸️" if bot_paused() else "يعمل 🟢"
        if not position:
            trade_text = "لا توجد صفقة مفتوحة."
        else:
            try:
                price = self.api.market_price(str(position["symbol"]))
                pnl = self.broker.pnl(position, price)
                target = self.broker.target_price(position, TARGET_NET_PROFIT)
                trade_text = (
                    f"الصفقة: {position['symbol']}\n"
                    f"الدفعات: {position['tranches']}/{MAX_TRANCHES}\n"
                    f"المستخدم: {position['total_cost']:.2f} USDT\n"
                    f"المتوسط: {position['avg_price']:.8f}\n"
                    f"{'الربح الحالي' if pnl >= 0 else 'الخسارة الحالية'}: {pnl:+.2f} USDT\n"
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
                current_price = self.api.market_price(str(position["symbol"]))
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
                    membership = str(analysis.payload.get("market_membership") or self.api.market_membership(symbol))
                    market_badge = (
                        " 🅰️Alpha Only" if membership == "ALPHA_ONLY"
                        else " 🟡Spot+Alpha" if membership == "SPOT_ALPHA"
                        else " 🟢Spot"
                    )
                    deriv_info = analysis.payload.get("derivatives") or {}
                    deriv_text = ""
                    if deriv_info.get("derivatives_score") is not None:
                        deriv_text = f" | مشتقات {deriv_info.get('lean','NEUTRAL')} {float(deriv_info.get('derivatives_score')):.0f}/100"
                    lines.append(
                        f"{i}) {symbol}{market_badge} — {analysis.coin_score:.1f}/100 — {status}{deriv_text}\n"
                        f"   {reason_text}"
                    )

            return "\n".join(lines)
        except Exception as exc:
            return f"تعذر تنفيذ /scan: {exc}"

    @staticmethod
    def _level_strength(touches: int, frames: int) -> str:
        score = touches + max(0, frames - 1) * 2
        if score >= 7:
            return "قوي جدًا"
        if score >= 4:
            return "قوي"
        if score >= 2:
            return "متوسط"
        return "ضعيف"

    @staticmethod
    def _trend_label(score: float) -> str:
        if score >= 65:
            return "🟢 صاعد قوي"
        if score >= 55:
            return "🟢 صاعد"
        if score <= 35:
            return "🔴 هابط قوي"
        if score <= 45:
            return "🔴 هابط"
        return "🟡 حيادي"

    @staticmethod
    def _volume_label(ratio: float) -> str:
        if ratio >= 2.0:
            return "قوي جدًا"
        if ratio >= 1.35:
            return "قوي"
        if ratio >= 0.80:
            return "متوسط"
        return "ضعيف"

    def _sr_candidates(self, candles: List[Dict], price: float, frame: str) -> Tuple[List[Dict], List[Dict]]:
        # Pivot-based support/resistance.  We deliberately ignore the live candle
        # and cluster nearby pivots so repeated reactions become stronger levels.
        rows = candles[:-1] if len(candles) > 1 else candles
        if len(rows) < 7 or price <= 0:
            return [], []
        pivots = []
        for i in range(2, len(rows) - 2):
            c = rows[i]
            if c["high"] >= max(rows[j]["high"] for j in range(i - 2, i + 3)):
                pivots.append((float(c["high"]), "R"))
            if c["low"] <= min(rows[j]["low"] for j in range(i - 2, i + 3)):
                pivots.append((float(c["low"]), "S"))
        # 0.35% minimum clustering band, widened slightly for volatile markets.
        a = atr(rows[-80:]) if rows else 0.0
        band = max(price * 0.0035, a * 0.35)
        clusters: List[Dict] = []
        for level, kind in sorted(pivots, key=lambda x: x[0]):
            found = None
            for cl in clusters:
                if cl["kind"] == kind and abs(level - cl["price"]) <= band:
                    found = cl
                    break
            if found:
                n = found["touches"]
                found["price"] = (found["price"] * n + level) / (n + 1)
                found["touches"] += 1
            else:
                clusters.append({"price": level, "kind": kind, "touches": 1, "frames": {frame}})
        supports = [x for x in clusters if x["price"] < price]
        resistances = [x for x in clusters if x["price"] > price]
        supports.sort(key=lambda x: x["price"], reverse=True)
        resistances.sort(key=lambda x: x["price"])
        return supports[:3], resistances[:3]

    def _merge_levels(self, levels: List[Dict], price: float) -> List[Dict]:
        merged: List[Dict] = []
        band = max(price * 0.0045, 1e-12)
        for item in sorted(levels, key=lambda x: x["price"]):
            hit = next((x for x in merged if abs(x["price"] - item["price"]) <= band), None)
            if hit:
                total = hit["touches"] + item["touches"]
                hit["price"] = (hit["price"] * hit["touches"] + item["price"] * item["touches"]) / total
                hit["touches"] = total
                hit["frames"].update(item["frames"])
            else:
                merged.append({"price": item["price"], "touches": item["touches"], "frames": set(item["frames"])})
        return merged

    def _futures_snapshot(self, symbol: str) -> Dict:
        # نفس محرك المشتقات المستخدم في قرار بوت المضاربة، حتى لا يختلف /USDT عن التداول الوهمي.
        return get_derivatives_snapshot(symbol)

    def _coin_report(self, symbol: str, full: bool = False) -> str:
        symbol = symbol.upper().strip()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        try:
            market_kind = self.api.market_kind(symbol)
            membership = self.api.market_membership(symbol)
            is_alpha = membership in {"SPOT_ALPHA", "ALPHA_ONLY"}
            if market_kind == "UNKNOWN":
                return f"❌ {symbol} غير موجودة على Binance Spot ولا ضمن Binance Alpha."

            frames = [("5M", "5m", 240), ("15M", "15m", 240), ("1H", "1h", 240),
                      ("4H", "4h", 240), ("1D", "1d", 240), ("1W", "1w", 180)]
            data: Dict[str, Dict] = {}
            all_s, all_r = [], []
            structural_high_candidates = []
            price = self.api.market_price(symbol)
            for label, interval, limit in frames:
                candles = self.api.market_klines(symbol, interval, limit=limit)
                closes = [c["close"] for c in candles]
                tr = trend_score(closes)
                rv = volume_ratio(candles)
                rs = rsi(closes)
                supports, resistances = self._sr_candidates(candles, price, label)
                all_s.extend(supports); all_r.extend(resistances)
                # نحفظ كذلك القمم/القيعان الفعلية المغلقة حتى لا يضيع مستوى واضح
                # بسبب تجميع الـ pivots في مستوى متوسط قريب منه.
                closed_rows = candles[:-1] if len(candles) > 1 else candles
                recent_window = closed_rows[-120:] if closed_rows else []
                raw_high = max((float(c["high"]) for c in recent_window), default=price)
                raw_low = min((float(c["low"]) for c in recent_window), default=price)
                data[label] = {"trend": tr, "rsi": rs, "vol": rv, "raw_high": raw_high, "raw_low": raw_low}

                # المقاومة الهيكلية: آخر Swing High حقيقي على الفريمات الأهم،
                # بدل اعتبار كل مقاومة حسابية صغيرة سقفًا رئيسيًا للحركة.
                if label in {"1H", "4H", "1D"} and len(closed_rows) >= 7:
                    look = closed_rows[-90:]
                    for i in range(len(look) - 3, 1, -1):
                        h = float(look[i]["high"])
                        if h <= price * 1.002:
                            continue
                        left = [float(look[i-j]["high"]) for j in (1, 2)]
                        right = [float(look[i+j]["high"]) for j in (1, 2)]
                        if h > max(left) and h >= max(right):
                            structural_high_candidates.append({"price": h, "frame": label})
                            break

            supports = [x for x in self._merge_levels(all_s, price) if x["price"] < price]
            resistances = [x for x in self._merge_levels(all_r, price) if x["price"] > price]
            supports.sort(key=lambda x: x["price"], reverse=True)
            resistances.sort(key=lambda x: x["price"])

            # دمج قمم Swing المتقاربة بين 1H/4H/1D وتحديد المقاومة الهيكلية الرئيسية.
            structural_resistance = None
            structural_frames = []
            if structural_high_candidates:
                clusters = []
                for cand in sorted(structural_high_candidates, key=lambda x: x["price"]):
                    placed = False
                    for cl in clusters:
                        center = mean(x["price"] for x in cl)
                        if center and abs(cand["price"] / center - 1.0) <= 0.005:
                            cl.append(cand)
                            placed = True
                            break
                    if not placed:
                        clusters.append([cand])
                clusters.sort(key=lambda cl: (-len({x["frame"] for x in cl}), mean(x["price"] for x in cl)))
                best = clusters[0]
                structural_resistance = max(x["price"] for x in best)
                structural_frames = sorted({x["frame"] for x in best}, key=lambda z:["1H","4H","1D"].index(z))

            tick = self.api.market_ticker_24h(symbol)
            quote_vol = float(tick.get("quoteVolume", 0) or 0)
            # حركة غير طبيعية قصيرة: آخر دقيقة مكتملة مقارنة بإغلاق الدقيقة السابقة.
            move_1m = None
            try:
                one_min = self.api.market_klines(symbol, "1m", limit=4)
                closed_1m = one_min[:-1] if len(one_min) > 1 else one_min
                if len(closed_1m) >= 2:
                    move_1m = pct_change(float(closed_1m[-1]["close"]), float(closed_1m[-2]["close"]))
            except Exception:
                move_1m = None
            if quote_vol >= 100_000_000: liquidity = "قوية جدًا"
            elif quote_vol >= 25_000_000: liquidity = "قوية"
            elif quote_vol >= 5_000_000: liquidity = "متوسطة"
            else: liquidity = "ضعيفة"

            market = get_market_context()
            fut = self._futures_snapshot(symbol)
            # نفصل بين الاتجاه التداولي الحالي (الأهم للدخول) والاتجاه الهيكلي البعيد.
            # هذا يمنع 1D/1W من تحويل تصحيح هابط واضح على 1H/4H إلى حكم صاعد مضلل.
            short_trend = mean([data["5M"]["trend"], data["15M"]["trend"]])
            trading_trend = mean([data["1H"]["trend"], data["4H"]["trend"]])
            structural_trend = mean([data["1D"]["trend"], data["1W"]["trend"]])
            macro_trend = trading_trend  # توافق خلفي: الحكم النهائي يعتمد الاتجاه التداولي 1H+4H.
            vol_support = mean([min(100.0, data[x]["vol"] * 50.0) for x in ("5M", "15M", "1H")])
            btc_support = max(0.0, min(100.0, market.btc_trend_score))
            deriv = float(fut.get("derivatives_score", 50.0) or 50.0)

            # قوة الاستمرار تعطي الوزن الأكبر لـ 1H/4H، ثم الزخم القصير؛ اليومي/الأسبوعي سياق لا تصريح دخول.
            upside = round(max(0.0, min(100.0,
                short_trend * .20 + trading_trend * .35 + structural_trend * .10
                + vol_support * .15 + btc_support * .10 + deriv * .10
            )))
            overbought = mean([data["15M"]["rsi"], data["1H"]["rsi"], data["4H"]["rsi"]])
            downside = round(max(0.0, min(100.0,
                (100-short_trend) * .20 + (100-trading_trend) * .35
                + (100-structural_trend) * .05 + (100-deriv) * .10
                + max(0, overbought-60) * 1.0 + max(0, 1-data["15M"]["vol"]) * 15
            )))

            lines = [f"🔬 التحليل الشامل — {symbol}", f"💰 السعر: {price:.8f}"]
            lines += ["", "🅰️ Binance Alpha"]
            if membership == "SPOT_ALPHA":
                lines.append("• نوع السوق: 🟡 Spot + Alpha")
                lines.append("• العملة ضمن Binance Alpha: نعم ✅")
                lines.append("• مصدر بيانات هذا التحليل: Binance Spot — بدون تكرارها كمرشح Alpha")
            elif membership == "ALPHA_ONLY":
                lines.append("• نوع السوق: 🅰️ Alpha Only")
                lines.append("• العملة ضمن Binance Alpha: نعم ✅")
                lines.append("• مصدر بيانات هذا التحليل: Binance Alpha")
                lines.append("• ⚠️ Alpha سوق مبكر وعالي المخاطر؛ البوت يتعامل معه Paper Trading فقط")
            else:
                lines.append("• نوع السوق: 🟢 Spot")
                lines.append("• العملة ضمن Binance Alpha: لا")
                lines.append("• مصدر بيانات هذا التحليل: Binance Spot")
            lines += ["", "📊 الفريمات"]
            for label, _, _ in frames:
                d=data[label]
                lines.append(f"• {label}: {self._trend_label(d['trend'])} | RSI {d['rsi']:.1f} | Volume {self._volume_label(d['vol'])} ({d['vol']:.2f}x)")

            lines += ["", "🛡️ أقوى الدعوم"]
            for x in supports[:3]:
                fr=" + ".join(sorted(x["frames"], key=lambda z:["5M","15M","1H","4H","1D","1W"].index(z)))
                lines.append(f"• {x['price']:.8f} — {self._level_strength(x['touches'], len(x['frames']))} — {fr}")
            if not supports: lines.append("• لا يوجد دعم موثوق قريب ضمن البيانات المتاحة")
            lines += ["", "🧱 أقوى المقاومات"]
            for x in resistances[:3]:
                fr=" + ".join(sorted(x["frames"], key=lambda z:["5M","15M","1H","4H","1D","1W"].index(z)))
                lines.append(f"• {x['price']:.8f} — {self._level_strength(x['touches'], len(x['frames']))} — {fr}")
            if not resistances: lines.append("• لا توجد مقاومة موثوقة قريبة ضمن البيانات المتاحة")

            v15=data["15M"]["vol"]
            lines += ["", "💧 السيولة والفوليوم",
                      f"• سيولة 24h: {liquidity} — {quote_vol/1_000_000:.2f}M USDT",
                      f"• الفوليوم اللحظي 15M: {self._volume_label(v15)} ({v15:.2f}x)",
                      f"• دعم الفوليوم للصعود: {'نعم ✅' if v15 >= 1.15 else 'جزئي 🟡' if v15 >= .8 else 'ضعيف ⚠️'}"]

            dom = "غير متاح" if market.btc_dominance is None else f"{market.btc_dominance:.2f}%"
            btc_state = "🟢 صاعد" if market.btc_trend_score >= 58 else "🔴 هابط" if market.btc_trend_score <= 42 else "⚪ حيادي"
            lines += ["", "₿ BTC والسوق",
                      f"• حالة BTC: {btc_state} — Trend {market.btc_trend_score:.1f}/100 | 1H {market.btc_change_1h:+.2f}%",
                      f"• BTC.D: {dom}",
                      f"• دعم السوق للألتكوين: {'نعم ✅' if market.market_safe else 'لا/حذر ⚠️'}"]
            if market.btc_dominance is None:
                lines.append("• ⚪ BTC.D غير متاح — لم يتم احتسابه في حكم مصدر الحركة")
            # Relative strength: coin 1H trend vs BTC trend.
            rel = data["1H"]["trend"] - market.btc_trend_score
            rel_label = "قوة ذاتية أعلى" if rel >= 8 else "مرتبطة بالسوق" if rel > -8 else "أضعف من BTC"
            lines.append(f"• قوة العملة مقابل BTC: {rel_label} ({rel:+.1f})")

            # مصدر الحركة: يفسر الصعود والهبوط ولا يكتفي برقم BTC.
            coin_1h = data["1H"]["trend"]
            if coin_1h >= 58:
                if market.btc_trend_score >= 58 and rel < 8:
                    movement_source = "🚀 الصعود مدعوم من BTC والسوق"
                elif market.btc_trend_score >= 58 and rel >= 8:
                    movement_source = "🚀🔥 الصعود مدعوم من BTC + قوة ذاتية للعملة"
                elif rel >= 8:
                    movement_source = "🔥 الصعود مستقل نسبيًا عن BTC — قوة ذاتية للعملة"
                else:
                    movement_source = "🟡 صعود العملة غير مدعوم بوضوح من BTC"
            elif coin_1h <= 42:
                if market.btc_trend_score <= 42 and rel <= -8:
                    movement_source = "🔴 الهبوط مدعوم من ضعف BTC + ضعف ذاتي للعملة"
                elif market.btc_trend_score <= 42:
                    movement_source = "🔴 الهبوط مدعوم من ضعف BTC والسوق"
                elif market.btc_trend_score >= 58:
                    movement_source = "⚠️ ضعف ذاتي — العملة تهبط رغم دعم BTC"
                else:
                    movement_source = "🔻 الهبوط أقرب لضعف ذاتي في العملة"
            else:
                movement_source = "⚪ حركة العملة حيادية/غير محسومة بالنسبة لـ BTC"
            lines += ["", "🧭 مصدر الحركة", f"• {movement_source}"]

            lines += ["", "⚔️ المشتقات — رادار مساعد لتداول Spot"]
            if fut.get("available") is False:
                lines.append(f"• غير متاحة: {fut.get('reason','لا يوجد عقد Futures موثوق')}")
            else:
                if "funding" in fut: lines.append(f"• Funding: {fut['funding']:+.4f}%")
                if fut.get("funding_trend"):
                    lines.append("• Funding Trend: " + " → ".join(f"{x:+.4f}%" for x in fut['funding_trend']))
                if "oi_value" in fut: lines.append(f"• Open Interest: {fut['oi_value']/1_000_000:.2f}M USDT")
                if "oi_change_1h" in fut: lines.append(f"• OI تغير 1H: {fut['oi_change_1h']:+.2f}%")
                if "oi_change_24h" in fut: lines.append(f"• OI تغير 24H: {fut['oi_change_24h']:+.2f}%")
                if "long_short" in fut: lines.append(f"• Global Long/Short: {fut['long_short']:.2f}")
                if "top_accounts_ls" in fut: lines.append(f"• Top Accounts L/S: {fut['top_accounts_ls']:.2f}")
                if "top_positions_ls" in fut: lines.append(f"• Top Positions L/S: {fut['top_positions_ls']:.2f}")
                if "taker_ratio" in fut: lines.append(f"• Taker Buy/Sell: {fut['taker_ratio']:.2f} — {'شراء أقوى' if fut['taker_ratio']>1.05 else 'بيع أقوى' if fut['taker_ratio']<.95 else 'متوازن'}")
                if "futures_spot_ratio" in fut:
                    fs_ratio = float(fut['futures_spot_ratio'])
                    driver = "العقود تهيمن بشدة ⚠️" if fs_ratio >= 10 else "العقود تهيمن ⚠️" if fs_ratio >= 5 else "العقود أعلى" if fs_ratio >= 2 else "Spot صحي/متوازن"
                    lines.append(f"• Futures/Spot Volume: {fs_ratio:.1f}x — {driver}")
                if "oi_futures_volume_ratio" in fut: lines.append(f"• OI/Futures Volume: {fut['oi_futures_volume_ratio']:.2f}")
                if "oi_market_cap_ratio" in fut:
                    lines.append(f"• OI/Market Cap: {fut['oi_market_cap_ratio']:.1f}%")
                else:
                    lines.append("• OI/Market Cap: غير متاح — لا يوجد Market Cap موثوق من المصدر الحالي")
                lines.append("• Liquidated 24h: غير متاح من Binance العام بشكل موثوق")
                if fut.get("derivatives_score") is not None:
                    lean_ar = "LONG 🟢" if fut.get("lean") == "LONG" else "SHORT 🔴" if fut.get("lean") == "SHORT" else "NEUTRAL ⚪"
                    conf_ar = {"low":"منخفضة", "medium":"متوسطة", "high":"عالية"}.get(str(fut.get("confidence")), "منخفضة")
                    lines.append(f"• 🧭 Derivatives Lean: {lean_ar} — {fut['derivatives_score']:.0f}/100 | الثقة {conf_ar}")
                    if fut.get("derivatives_reasons"):
                        lines.append("• السبب: " + " + ".join(fut['derivatives_reasons'][:4]))
                    if fut.get("momentum_quality") is not None:
                        mq = float(fut.get("momentum_quality", 50) or 50)
                        mq_label = str(fut.get("momentum_label") or "مختلط")
                        lines.append(f"• ⚡ جودة الزخم: {mq:.0f}/100 — {mq_label}")
                        if fut.get("momentum_reasons"):
                            lines.append("• تغير الزخم: " + " + ".join(fut["momentum_reasons"][:3]))

            lines += ["", "🚨 الحركة اللحظية"]
            if move_1m is None:
                lines.append("• تغير 1M: غير متاح")
            elif move_1m >= 3.0:
                lines.append(f"• 🟢 PUMP ALERT: {move_1m:+.2f}% خلال آخر دقيقة مكتملة")
            elif move_1m <= -3.0:
                lines.append(f"• 🔴 DUMP ALERT: {move_1m:+.2f}% خلال آخر دقيقة مكتملة")
            else:
                lines.append(f"• لا توجد حركة Pump/Dump استثنائية الآن — 1M {move_1m:+.2f}%")

            lines += ["", "⛓️ On-chain و Token Unlock"]
            lines.append("• Exchange Inflow/Outflow والمحافظ الكبيرة: غير متاح — يحتاج مصدر On-chain خارجي موثوق")
            lines.append("• Token Unlock القادم: غير متاح — يحتاج مصدر Unlock خارجي موثوق")
            lines.append("• ⚪ لم تدخل هذه البيانات في الحكم حتى يتم ربط مصدر حقيقي؛ لا توجد تقديرات أو بيانات وهمية")

            # استخدم نفس المستويات المعروضة في "أقوى الدعوم/المقاومات" في كل أجزاء التقرير.
            # هذا يمنع ظهور مستوى مختلف في "موقع السعر" أو الخلاصة.
            nearest_r = resistances[0]["price"] if resistances else None
            nearest_s = supports[0]["price"] if supports else None

            # التشبع: نذكر النوع والفريمات صراحة.
            overbought = [(fr, data[fr]["rsi"]) for fr in ["5M","15M","1H","4H","1D","1W"] if data[fr]["rsi"] >= 70]
            oversold = [(fr, data[fr]["rsi"]) for fr in ["5M","15M","1H","4H","1D","1W"] if data[fr]["rsi"] <= 30]
            lines += ["", "🌡️ التشبع"]
            if overbought:
                lines.append("• ⚠️ تشبع شرائي: " + " | ".join(f"{fr} RSI {rv:.1f}" for fr,rv in overbought))
            elif oversold:
                lines.append("• 🟢 تشبع بيعي: " + " | ".join(f"{fr} RSI {rv:.1f}" for fr,rv in oversold))
            else:
                lines.append("• لا يوجد تشبع شرائي أو بيعي واضح على الفريمات الرئيسية")

            # جودة الدخول الآن منفصلة عن قوة الاتجاه.
            distance_r_pct = ((nearest_r / price - 1) * 100) if nearest_r and price else None
            if overbought and any(fr in {"15M","1H","4H"} for fr,_ in overbought):
                entry_quality = "🔴 ضعيفة — تشبع شرائي/مطاردة محتملة"
            elif distance_r_pct is not None and 0 <= distance_r_pct <= 0.5 and v15 < 0.8:
                entry_quality = "🟠 مخاطرة مرتفعة — قرب مقاومة مع فوليوم ضعيف"
            elif upside >= 70 and v15 >= 1.0:
                entry_quality = "🟢 جيدة — الاتجاه والحجم يدعمان الحركة"
            else:
                entry_quality = "🟡 متوسطة — تحتاج تأكيد إضافي"
            lines += ["", "🎯 حالة الدخول الآن", f"• {entry_quality}"]

            # معلومات تنفيذية: المسافة للمستويات + توافق الفريمات + جودة الاختراق.
            lines += ["", "📐 موقع السعر"]
            if nearest_r:
                dist_r = (nearest_r / price - 1) * 100
                lines.append(f"• المقاومة الأقرب: {nearest_r:.8f} — تبعد {dist_r:.2f}% عن السعر")
            if nearest_s:
                dist_s = (price / nearest_s - 1) * 100
                lines.append(f"• الدعم الأقرب: {nearest_s:.8f} — السعر أعلى منه {dist_s:.2f}%")

            short_labels = [data[x]["trend"] for x in ("5M", "15M")]
            mid_labels = [data[x]["trend"] for x in ("1H", "4H")]
            short_avg = mean(short_labels); mid_avg = mean(mid_labels)
            if mid_avg >= 58 and short_avg < 55:
                frame_note = "⚠️ الاتجاه التداولي 1H/4H صاعد لكن الزخم القصير يهدأ"
            elif mid_avg <= 42 and short_avg > 45:
                frame_note = "⚠️ الاتجاه التداولي 1H/4H هابط؛ يوجد ارتداد قصير فقط"
            elif mid_avg >= 58 and short_avg >= 55:
                frame_note = "🟢 الفريمات القصيرة والمتوسطة متوافقة على الصعود"
            elif mid_avg <= 42 and short_avg <= 45:
                frame_note = "🔴 الفريمات القصيرة والمتوسطة متوافقة على الهبوط"
            else:
                frame_note = "🟡 الفريمات متباينة — لا يوجد توافق كامل"
            lines += ["", "🧩 توافق الفريمات", f"• {frame_note}"]

            if nearest_r:
                dist_r = (nearest_r / price - 1) * 100
                if price >= nearest_r and v15 >= 1.15:
                    breakout_state = "🟢 اختراق مؤكد مبدئيًا — السعر فوق المقاومة والفوليوم داعم"
                elif price >= nearest_r:
                    breakout_state = "🟠 اختراق ضعيف — السعر فوق المقاومة لكن الفوليوم لا يؤكد"
                elif 0 <= dist_r <= 0.50 and v15 >= 1.15 and short_avg >= 50 and data["15M"]["trend"] >= 45:
                    breakout_state = "🟡 محاولة اختراق — السعر ملاصق للمقاومة والزخم/الفوليوم يتحسنان"
                elif 0 <= dist_r <= 0.50 and v15 >= 1.15:
                    breakout_state = "🟡 ارتداد من الدعم قيد الاختبار — الفوليوم تحسن لكن الاتجاه لم يتحول بعد"
                elif 0 <= dist_r <= 0.50:
                    breakout_state = "⚠️ اختبار مقاومة — السعر قريب جدًا لكن الفوليوم ضعيف"
                else:
                    breakout_state = "⚪ لا يوجد اختراق حاليًا"
                lines += ["", "🚧 حالة الاختراق", f"• {breakout_state}"]

            # دعم حاسم: لا نساوي بين دعم 5M ضعيف ومستوى متعدد الفريمات.
            critical_support = None
            for level in supports[:8]:
                if len(level.get("frames", set())) >= 2 or int(level.get("touches", 0)) >= 4:
                    critical_support = level["price"]
                    break
            if critical_support is None and nearest_s:
                critical_support = nearest_s

            lines += ["", "🛡️ مستويات القرار"]
            if nearest_s:
                lines.append(f"• الدعم الأقرب: {nearest_s:.8f}")
            if critical_support:
                lines.append(f"• الدعم الحاسم: {critical_support:.8f} — كسره يضعف الهيكل أكثر من كسر دعم لحظي ضعيف")
            if nearest_r:
                lines.append(f"• المقاومة الأقرب: {nearest_r:.8f}")
            if structural_resistance:
                fr_txt = " + ".join(structural_frames) if structural_frames else "1H/4H/1D"
                lines.append(f"• 🏔️ المقاومة الهيكلية الرئيسية: {structural_resistance:.8f} — آخر Swing High واضح — {fr_txt}")

            # ميزان القوة: يحول المؤشرات إلى أسباب مفهومة بدل أرقام منفصلة.
            bull_factors, bear_factors = [], []
            if trading_trend >= 58: bull_factors.append("الاتجاه التداولي 1H/4H صاعد")
            if structural_trend >= 58: bull_factors.append("اليومي/الأسبوعي ما زال داعمًا هيكليًا")
            if short_trend >= 58: bull_factors.append("الزخم القصير صاعد")
            if trading_trend <= 42: bear_factors.append("الاتجاه التداولي 1H/4H هابط")
            if data["1D"]["vol"] >= 1.3: bull_factors.append("فوليوم يومي داعم")
            if float(fut.get("oi_change_1h", 0) or 0) >= 2: bull_factors.append("OI يرتفع")
            if float(fut.get("taker_ratio", 1) or 1) > 1.05: bull_factors.append("Taker شراء أقوى")
            if rel >= 8: bull_factors.append("قوة ذاتية أعلى من BTC")
            if overbought: bear_factors.append("تشبع شرائي")
            if v15 < 0.8: bear_factors.append("فوليوم لحظي ضعيف")
            if float(fut.get("taker_ratio", 1) or 1) < 0.95: bear_factors.append("Taker بيع أقوى")
            if float(fut.get("long_short", 1) or 1) >= 1.6 or float(fut.get("top_positions_ls", 1) or 1) >= 2.0:
                bear_factors.append("ازدحام Longs")
            if float(fut.get("derivatives_score", 50) or 50) >= 58: bull_factors.append("المشتقات تدعم Spot")
            if float(fut.get("derivatives_score", 50) or 50) <= 42: bear_factors.append("المشتقات تميل ضد Spot")
            if distance_r_pct is not None and 0 <= distance_r_pct <= 0.75: bear_factors.append("مقاومة قريبة")
            lines += ["", "⚖️ ميزان القوة"]
            lines.append("• 🟢 عوامل الصعود: " + (" + ".join(bull_factors[:5]) if bull_factors else "لا توجد عوامل قوية كافية"))
            lines.append("• 🔴 عوامل الهبوط/التصحيح: " + (" + ".join(bear_factors[:5]) if bear_factors else "لا توجد ضغوط بارزة"))
            if trading_trend <= 42 and upside > downside:
                balance_note = "يوجد دعم هيكلي/ارتداد، لكن الكفة التداولية لم تتحول للصعود بعد"
            elif upside >= downside + 15 and trading_trend >= 50:
                balance_note = "الكفة للصعود، لكن جودة الدخول تُحكم منفصلة"
            elif downside >= upside + 15:
                balance_note = "الكفة للهبوط/التصحيح"
            else:
                balance_note = "الكفتان متقاربتان — يحتاج تأكيد"
            lines.append(f"• ⚖️ المحصلة: {balance_note}")

            lines += ["", "🎯 سيناريوهات السعر"]
            if nearest_r:
                next_r = resistances[1]["price"] if len(resistances) > 1 else None
                if next_r:
                    lines.append(f"• 🚀 اختراق {nearest_r:.8f} بإغلاق وفوليوم قوي → يفتح الطريق نحو {next_r:.8f}")
                else:
                    lines.append(f"• 🚀 اختراق {nearest_r:.8f} بإغلاق وفوليوم قوي → يدعم استمرار الصعود")
            if structural_resistance and (nearest_r is None or structural_resistance > nearest_r * 1.003):
                lines.append(f"• 🏔️ اختراق المقاومة الهيكلية {structural_resistance:.8f} وتثبيت فوقها بفوليوم قوي → تأكيد أقوى لموجة صاعدة جديدة")
            if nearest_s:
                fallback = supports[1]["price"] if len(supports) > 1 else critical_support
                if fallback and abs(float(fallback) - float(nearest_s)) > 1e-15:
                    lines.append(f"• ⚠️ فشل الاختراق/فقد {nearest_s:.8f} → مراقبة {fallback:.8f}")
            if critical_support:
                lines.append(f"• 🔴 كسر الدعم الحاسم {critical_support:.8f} بفوليوم بيع → ضعف واضح في الهيكل")

            lines += ["", f"🚀 قوة استمرار الصعود: {upside}/100", f"📉 خطر الهبوط/التصحيح: {downside}/100", "", "🧠 الخلاصة"]
            # الحكم النهائي يعطي الوزن الأكبر للاتجاه التداولي 1H + 4H.
            # 1D/1W سياق هيكلي، ولا يحق لهما وحدهما تحويل تصحيح 1H/4H الهابط إلى إشارة صعود.
            if macro_trend <= 42:
                if downside >= 65:
                    verdict = "🔴 هابط — خطر الهبوط مرتفع"
                else:
                    verdict = "🔴 ميل هابط — الصعود الحالي ضعيف وغير مؤكد"
            elif macro_trend >= 58:
                if upside >= 70:
                    verdict = "🟢 الصعود مدعوم"
                elif upside >= 55:
                    verdict = "🟡 ميل صاعد لكن يحتاج تأكيد"
                else:
                    verdict = "🟡 الاتجاه التداولي صاعد لكن الزخم الحالي ضعيف"
            elif upside >= 70:
                verdict = "🟢 الصعود مدعوم"
            elif downside >= 65:
                verdict = "🔴 خطر الهبوط مرتفع"
            elif upside >= 55:
                verdict = "🟡 ميل صاعد لكن يحتاج تأكيد"
            elif downside > upside and macro_trend < 50:
                verdict = "🔴 ميل هابط — الصعود الحالي ضعيف وغير مؤكد"
            else:
                verdict = "🟡 الحركة غير محسومة"
            lines.append(f"• الحكم: {verdict}")
            lines.append(f"• مصدر الحركة: {movement_source}")
            lines.append(f"• جودة الدخول الآن: {entry_quality}")
            lines.append(f"• توافق الفريمات: {frame_note}")
            market_summary = "🅰️ Alpha Only" if membership == "ALPHA_ONLY" else "🟡 Spot + Alpha" if membership == "SPOT_ALPHA" else "🟢 Spot"
            lines.append(f"• نوع السوق: {market_summary}")
            if critical_support:
                lines.append(f"• الدعم الحاسم: {critical_support:.8f}")
            if structural_resistance:
                lines.append(f"• المقاومة الهيكلية الرئيسية: {structural_resistance:.8f} — آخر Swing High واضح")
            lines.append(f"• ميزان القوة: {balance_note}")
            if nearest_r:
                lines.append(f"• المقاومة الأقرب {nearest_r:.8f} تبعد {(nearest_r / price - 1) * 100:.2f}% عن السعر.")
                lines.append(f"• حالة الاختراق: {breakout_state}")
                lines.append(f"• اختراق {nearest_r:.8f} بإغلاق وفوليوم قوي يقوي استمرار الصعود.")
            if nearest_s: lines.append(f"• كسر {nearest_s:.8f} بإغلاق وفوليوم بيع قوي يرفع خطر الهبوط.")
            lines.append("• الدرجات احتمالية تحليلية وليست ضمانًا لاتجاه السعر.")

            if full:
                return "\n".join(lines)

            # التقرير الافتراضي مختصر وموجّه للقرار؛ كل الحسابات التفصيلية أعلاه تبقى فعالة.
            position = self.broker.position()
            same_position = bool(position and str(position["symbol"]).upper() == symbol)
            rescue_mode = bool(same_position and int(position["rescue_mode"] or 0))

            if same_position:
                current_pnl = self.broker.pnl(position, price)
                if rescue_mode:
                    decision = "🛟 إنقاذ — لا تعزيز؛ انتظار خروج آمن بصافي موجب"
                elif entry_quality.startswith("🔴") or entry_quality.startswith("🟠"):
                    decision = "🟡 احتفاظ ومراقبة — لا تعزيز الآن"
                else:
                    decision = "🟢 الصفقة مدعومة — التعزيز فقط عند تحقق شروط الارتداد"
            else:
                current_pnl = None
                if entry_quality.startswith("🟢"):
                    decision = "🟢 فرصة جيدة — مع انتظار تأكيد التنفيذ"
                elif entry_quality.startswith("🔴"):
                    decision = "🔴 لا تدخل الآن — مطاردة/تشبع"
                else:
                    decision = "🟡 انتظار — جودة الدخول غير مكتملة"

            reason_bits = []
            if overbought:
                reason_bits.append("تشبع شرائي")
            if v15 < 0.8:
                reason_bits.append("فوليوم لحظي ضعيف")
            if distance_r_pct is not None and 0 <= distance_r_pct <= 0.75:
                reason_bits.append("مقاومة قريبة")
            mq = fut.get("momentum_quality") if isinstance(fut, dict) else None
            if mq is not None and float(mq) < 50:
                reason_bits.append("الزخم يبرد")
            if not reason_bits:
                if trading_trend >= 58:
                    reason_bits.append("الاتجاه التداولي صاعد")
                elif trading_trend <= 42:
                    reason_bits.append("الاتجاه التداولي هابط")
                else:
                    reason_bits.append("الإشارات مختلطة وتحتاج تأكيد")

            resistance_show = nearest_r or structural_resistance
            compact_lines = [
                f"🔬 {symbol} — {price:.8f}",
                "",
            ]
            if same_position:
                compact_lines += [
                    f"💼 الصفقة: مفتوحة — {position['tranches']}/{MAX_TRANCHES} دفعات | المتوسط {float(position['avg_price']):.8f}",
                    f"💰 {'الربح' if current_pnl >= 0 else 'الخسارة'} الحالية: {current_pnl:+.2f} USDT",
                ]
            compact_lines += [
                f"🎯 القرار: {decision}",
                f"📈 الاتجاه: {verdict}",
                f"🚀 قوة الصعود: {upside}/100 | 📉 خطر التصحيح: {downside}/100",
            ]
            if mq is not None:
                compact_lines.append(f"⚡ جودة الزخم: {float(mq):.0f}/100 — {fut.get('momentum_label','مختلط')}")
            compact_lines += [
                f"⚠️ السبب: {' + '.join(reason_bits[:3])}",
                "",
                f"🛡️ الدعم: {critical_support:.8f}" if critical_support else "🛡️ الدعم: لا يوجد مستوى موثوق قريب",
                f"🧱 المقاومة: {resistance_show:.8f}" if resistance_show else "🧱 المقاومة: لا توجد مقاومة موثوقة قريبة",
                f"₿ السوق: {btc_state} — {movement_source}",
            ]
            if fut.get("available") is False:
                compact_lines.append("⚔️ المشتقات: غير متاحة")
            else:
                dscore = fut.get("derivatives_score")
                if dscore is not None:
                    compact_lines.append(f"⚔️ المشتقات: {fut.get('lean','NEUTRAL')} {float(dscore):.0f}/100")
            compact_lines.append(f"🏷️ السوق: {market_summary}")
            compact_lines += ["", "📋 للتفاصيل الكاملة: /full " + symbol]
            return "\n".join(compact_lines)
        except Exception as exc:
            return f"❌ تعذر تحليل {symbol}: {exc}"

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
                "🔬 /[رمز العملة]USDT — تقرير قرار مختصر (مثال: /SPKUSDT)\n"
                "🌊 /early SYMBOLUSDT — اختبار Early Flow فورًا\n"
                "📋 /full [SYMBOLUSDT] — التقرير الكامل؛ بدون رمز يستخدم آخر عملة\n"
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
        elif command == "/early":
            if len(parts) < 2:
                self._reply("الاستخدام: /early KITEUSDT")
                return
            symbol = parts[1].upper().strip().lstrip("/")
            if not symbol.endswith("USDT"):
                symbol += "USDT"
            try:
                market = get_market_context()
                analysis = get_analysis(symbol, market.score)
                flow = analysis.payload.get("early_flow") or {}
                deriv = analysis.payload.get("derivatives") or {}
                reasons = flow.get("reasons") or []
                late = flow.get("late_reasons") or []
                if not deriv.get("available"):
                    self._reply(
                        f"🌊 Early Flow — {symbol}\n\n"
                        f"⚪ غير متاح حاليًا\n"
                        f"السبب: {deriv.get('reason','بيانات المشتقات غير متاحة')}\n"
                        f"📊 Technical: {analysis.coin_score:.0f}/100"
                    )
                    return
                verdict = "✅ شروط Early Flow للدخول مكتملة" if flow.get("entry_ok") else ("🚨 يستحق التنبيه والمراقبة" if flow.get("strong") else "👀 لم يصل شرط التنبيه بعد")
                why = " + ".join(reasons[:5]) or "لا توجد عوامل تدفق قوية كافية"
                late_text = " + ".join(late[:3]) if late else "لا توجد علامة مطاردة واضحة"
                self._reply(
                    f"🌊 Early Flow — {symbol}\n\n"
                    f"💰 السعر: {analysis.price:.8f}\n"
                    f"🌊 التدفق: {float(flow.get('score',0)):.0f}/100 (تنبيه من {EARLY_FLOW_ALERT_SCORE:.0f})\n"
                    f"⏱️ التوقيت: {float(flow.get('timing',0)):.0f}/100 — {flow.get('timing_label','')}\n"
                    f"📊 Technical: {analysis.coin_score:.0f}/100\n"
                    f"📈 OI 1H: {deriv.get('oi_change_1h','غير متاح')}%\n"
                    f"⚔️ Taker: {deriv.get('taker_ratio','غير متاح')}\n"
                    f"💸 Funding: {deriv.get('funding','غير متاح')}%\n\n"
                    f"🧠 التدفق: {why}\n"
                    f"🚧 التوقيت: {late_text}\n\n"
                    f"🎯 {verdict}"
                )
            except Exception as exc:
                self._reply(f"❌ تعذر اختبار Early Flow لـ {symbol}: {exc}")
        elif command == "/full":
            if len(parts) >= 2:
                full_symbol = parts[1].upper().strip().lstrip("/")
            else:
                full_symbol = self.db.get_runtime("last_report_symbol", "").upper().strip()
            if not full_symbol:
                self._reply("الاستخدام: /full KITEUSDT — أو حلل عملة أولًا ثم أرسل /full")
                return
            if not full_symbol.endswith("USDT"):
                full_symbol += "USDT"
            self._reply(self._coin_report(full_symbol, full=True))
        elif command.startswith("/") and command[1:].upper().endswith("USDT") and command[1:].replace("_", "").isalnum():
            symbol = command[1:].upper()
            self.db.set_runtime("last_report_symbol", symbol)
            self._reply(self._coin_report(symbol, full=False))
        elif command == "/excluded":
            rows = self.db.list_excluded()
            if not rows:
                self._reply("قائمة الاستبعاد فارغة.")
            else:
                lines = [f"• {row['symbol']}: {row['reason']}" for row in rows[:50]]
                self._reply("🚫 العملات المستبعدة\n\n" + "\n".join(lines))


    def _telegram_photo_bytes(self, message: Dict) -> bytes:
        photos = message.get("photo") or []
        if not photos:
            raise RuntimeError("الرسالة لا تحتوي صورة.")
        # Telegram يرسل عدة أحجام؛ آخر عنصر عادة أعلى دقة.
        file_id = str(photos[-1].get("file_id") or "")
        if not file_id:
            raise RuntimeError("تعذر الحصول على file_id للصورة.")

        meta = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getFile",
            params={"file_id": file_id},
            timeout=20,
        )
        meta.raise_for_status()
        file_path = str(((meta.json().get("result") or {}).get("file_path")) or "")
        if not file_path:
            raise RuntimeError("Telegram لم يرجع مسار الصورة.")

        response = requests.get(
            f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}",
            timeout=30,
        )
        response.raise_for_status()
        return response.content

    @staticmethod
    def _openai_output_text(payload: Dict) -> str:
        # استخراج نص Responses API بدون الاعتماد على SDK خارجي.
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()

        parts = []
        for item in payload.get("output") or []:
            if not isinstance(item, dict):
                continue
            for content in item.get("content") or []:
                if not isinstance(content, dict):
                    continue
                value = content.get("text")
                if isinstance(value, str):
                    parts.append(value)
        return "\n".join(parts).strip()

    def _extract_usdt_symbols_from_photo(self, image_bytes: bytes) -> List[str]:
        if not PHOTO_SCAN_ENABLED:
            raise RuntimeError("PHOTO_SCAN_ENABLED=0")
        if not OPENAI_API_KEY:
            raise RuntimeError(
                "ميزة قراءة الصور تحتاج OPENAI_API_KEY في متغيرات البيئة. "
                "لن يحاول البوت تخمين العملات بدون Vision."
            )

        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        prompt = (
            "اقرأ لقطة شاشة لقائمة أزواج عملات في منصة تداول. "
            "استخرج فقط الأزواج الظاهرة فعليًا التي يكون Quote فيها USDT بالضبط. "
            "لا تحول USDC أو FDUSD أو BTC أو ETH أو EUR إلى USDT، ولا تستنتج زوجًا غير ظاهر. "
            "أعد النتيجة كسطر واحد فقط، رموز Binance بدون شرطة مائلة ومفصولة بفواصل، "
            "مثال: ETHFIUSDT,ENAUSDT,EIGENUSDT. "
            "إذا لم يوجد أي زوج USDT أعد NONE."
        )

        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_VISION_MODEL,
                "input": [{
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {
                            "type": "input_image",
                            "image_url": f"data:image/jpeg;base64,{image_b64}",
                            "detail": "high",
                        },
                    ],
                }],
            },
            timeout=90,
        )
        response.raise_for_status()
        answer = self._openai_output_text(response.json()).upper()

        # لا نثق بالنص الخام وحده؛ نلتقط فقط Tokens تنتهي USDT.
        symbols = re.findall(r"\b[A-Z0-9]{2,20}USDT\b", answer)
        unique = []
        seen = set()
        for symbol in symbols:
            if symbol not in seen:
                unique.append(symbol)
                seen.add(symbol)
        return unique

    def _photo_scan(self, message: Dict) -> None:
        self._reply("🖼️ استلمت الصورة — أقرأ أزواج USDT الظاهرة ثم أفحصها من Binance...")

        try:
            image_bytes = self._telegram_photo_bytes(message)
            extracted = self._extract_usdt_symbols_from_photo(image_bytes)
        except Exception as exc:
            self._reply(f"❌ تعذر قراءة الصورة: {exc}")
            return

        if not extracted:
            self._reply("⚪ لم أجد أزواج USDT واضحة في الصورة.")
            return

        # تحقق من أن الزوج موجود فعلًا في Binance Spot؛ الصورة وحدها لا تكفي.
        valid = [s for s in extracted if self.api.is_spot_symbol(s)]
        invalid = [s for s in extracted if s not in valid]

        if not valid:
            msg = "⚪ وجدت رموزًا في الصورة لكن لا يوجد بينها زوج Binance Spot/USDT صالح حاليًا."
            if invalid:
                msg += "\nالمرفوض: " + ", ".join(invalid[:20])
            self._reply(msg)
            return

        market_score = get_market_score()
        learned_min = self.learner.effective_entry_score()
        rows = []
        rejected = []

        for symbol in valid:
            try:
                risk = self.universe.risk_reason(symbol)
                if risk:
                    rejected.append((symbol, str(risk)))
                    continue
                if not self.universe.listing_year_ok(symbol):
                    rejected.append((symbol, f"سنة الإدراج أقدم من {MIN_LISTING_YEAR}"))
                    continue

                analysis = get_analysis(symbol, market_score)
                reasons = entry_rejection_reasons(analysis, learned_min)
                rows.append((analysis.coin_score, symbol, analysis, reasons))
            except Exception as exc:
                rejected.append((symbol, f"خطأ تحليل: {exc}"))

        rows.sort(key=lambda x: x[0], reverse=True)

        # المرشح = درجة فنية كافية. لا يعني BUY؛ حالة الدخول تبقى ظاهرة منفصلة.
        qualified = [row for row in rows if row[0] >= PHOTO_SCAN_MIN_SCORE]
        selected = qualified[:max(1, PHOTO_SCAN_MAX_REPORTS)]

        lines = [
            "🔎 فحص الصورة — أزواج USDT فقط",
            "",
            "📌 الموجود بالصورة: " + ", ".join(valid),
            f"• تقييم السوق: {market_score:.1f}/100",
            f"• حد ترشيح الصورة: {PHOTO_SCAN_MIN_SCORE:.0f}/100",
            f"• المرشحون: {len(qualified)}",
            "",
            "🏆 الترتيب:",
        ]

        if not rows:
            lines.append("لا توجد عملات قابلة للتحليل بعد فلاتر الأمان.")
        else:
            for i, (score, symbol, analysis, reasons) in enumerate(rows, 1):
                if score >= PHOTO_SCAN_MIN_SCORE:
                    status = "✅ مرشح"
                else:
                    status = "⏳ أقل من حد الترشيح"
                if analysis.entry_ok:
                    entry = "🟢 دخول مؤهل"
                elif analysis.payload.get("chase_guard"):
                    entry = "🔴 Chase Guard"
                else:
                    entry = "🟡 انتظار"
                reason_text = "، ".join(reasons[:2])
                lines.append(
                    f"{i}) {symbol} — {score:.1f}/100 — {status} | {entry}\n"
                    f"   {reason_text}"
                )

        if rejected:
            lines += ["", "🚫 مستبعدة:"]
            for symbol, reason in rejected[:10]:
                lines.append(f"• {symbol}: {reason}")

        if invalid:
            lines += ["", "⚪ ظهرت بالصورة لكن ليست Binance Spot/USDT صالحة:"]
            lines.append("• " + ", ".join(invalid[:20]))

        self._reply("\n".join(lines))

        if not selected:
            self._reply("⚪ لا يوجد مرشح تجاوز حد الصورة حاليًا؛ لن أرسل تقارير طويلة لعملات ضعيفة.")
            return

        self._reply(
            f"🔬 سأرسل التقرير الشامل لـ {len(selected)} مرشح"
            + ("ين" if len(selected) == 2 else "ات" if len(selected) > 2 else "")
            + "، مرتبة من الأقوى للأضعف."
        )
        for _score, symbol, _analysis, _reasons in selected:
            try:
                self._reply(self._coin_report(symbol))
            except Exception as exc:
                self._reply(f"❌ تعذر إنشاء التقرير الشامل لـ {symbol}: {exc}")

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

            # صورة = Scan بصري لأزواج USDT الظاهرة فقط.
            if message.get("photo"):
                self._photo_scan(message)
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
# حماية تنفيذية مزدوجة: كل BUY يمر على risk_reason مرة أخرى لحظة التنفيذ.
broker.risk_guard = universe.risk_reason
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


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _alpha_market_cap(symbol: str) -> Optional[float]:
    """أفضل محاولة من بيانات Binance Alpha فقط؛ لا نخمن Market Cap لرموز Spot."""
    try:
        row = api.alpha_resolve(symbol)
        if not row:
            return None
        for key in ("marketCap", "market_cap", "circulatingMarketCap", "circulating_market_cap"):
            value = row.get(key)
            if value not in (None, ""):
                number = _safe_float(value, 0.0)
                if number > 0:
                    return number
    except Exception:
        pass
    return None


def _derivatives_score(snapshot: Dict) -> Tuple[float, str, str, List[str]]:
    """درجة دعم/خطر للمضاربة Spot، وليست توصية Futures."""
    score = 50.0
    reasons: List[str] = []
    available = 0

    taker = snapshot.get("taker_ratio")
    if taker is not None:
        available += 1
        taker = float(taker)
        if taker >= 1.15:
            score += 8; reasons.append("Taker شراء قوي")
        elif taker > 1.05:
            score += 4; reasons.append("Taker شراء أعلى")
        elif taker <= 0.85:
            score -= 8; reasons.append("Taker بيع قوي")
        elif taker < 0.95:
            score -= 4; reasons.append("Taker بيع أعلى")

    top_pos = snapshot.get("top_positions_ls")
    if top_pos is not None:
        available += 1
        top_pos = float(top_pos)
        if 1.10 <= top_pos <= 1.80:
            score += 4; reasons.append("مراكز الكبار تميل Long بشكل صحي")
        elif top_pos >= 2.50:
            score -= 5; reasons.append("ازدحام Long في مراكز الكبار")
        elif top_pos <= 0.80:
            score -= 4; reasons.append("مراكز الكبار تميل Short")

    top_acc = snapshot.get("top_accounts_ls")
    if top_acc is not None:
        available += 1
        top_acc = float(top_acc)
        if 1.05 <= top_acc <= 1.80:
            score += 2
        elif top_acc >= 2.50:
            score -= 3; reasons.append("ازدحام Long في حسابات الكبار")
        elif top_acc <= 0.80:
            score -= 3

    global_ls = snapshot.get("long_short")
    if global_ls is not None:
        available += 1
        global_ls = float(global_ls)
        if global_ls >= 2.20:
            score -= 6; reasons.append("الجمهور مزدحم Long")
        elif global_ls <= 0.55:
            score += 2; reasons.append("الجمهور مزدحم Short")

    funding = snapshot.get("funding")
    if funding is not None:
        available += 1
        funding = float(funding)
        if funding >= 0.05:
            score -= 8; reasons.append("Funding موجب مرتفع")
        elif funding >= 0.02:
            score -= 4; reasons.append("Funding موجب ملحوظ")
        elif funding <= -0.05:
            score -= 2; reasons.append("Funding سالب شديد/سوق مضغوط")
        elif funding < 0:
            score += 2

    fs = snapshot.get("futures_spot_ratio")
    if fs is not None:
        available += 1
        fs = float(fs)
        if fs >= 15:
            score -= 10; reasons.append("العقود تهيمن بشدة على السبوت")
        elif fs >= 8:
            score -= 7; reasons.append("هيمنة عقود مرتفعة")
        elif fs >= 5:
            score -= 4; reasons.append("العقود أعلى من السبوت")
        elif fs <= 2:
            score += 5; reasons.append("دعم Spot صحي")

    oi_fut = snapshot.get("oi_futures_volume_ratio")
    if oi_fut is not None:
        available += 1
        oi_fut = float(oi_fut)
        if oi_fut <= 0.15:
            score += 3
        elif oi_fut >= 0.50:
            score -= 5; reasons.append("OI مرتفع مقارنة بدوران العقود")

    oi_mcap = snapshot.get("oi_market_cap_ratio")
    if oi_mcap is not None:
        available += 1
        oi_mcap = float(oi_mcap)
        if oi_mcap >= 25:
            score -= 10; reasons.append("OI ضخم مقابل القيمة السوقية")
        elif oi_mcap >= 15:
            score -= 6; reasons.append("رافعة مرتفعة مقابل حجم المشروع")
        elif oi_mcap <= 8:
            score += 3

    oi1h = snapshot.get("oi_change_1h")
    if oi1h is not None and taker is not None:
        available += 1
        oi1h = float(oi1h)
        if oi1h >= 4 and float(taker) >= 1.05:
            score += 4; reasons.append("OI يرتفع مع شراء")
        elif oi1h >= 4 and float(taker) < 0.95:
            score -= 4; reasons.append("OI يرتفع مع بيع")

    score = max(0.0, min(100.0, score))
    if score >= 58:
        lean = "LONG"
    elif score <= 42:
        lean = "SHORT"
    else:
        lean = "NEUTRAL"

    distance = abs(score - 50)
    if available >= 7 and distance >= 15:
        confidence = "high"
    elif available >= 5 and distance >= 8:
        confidence = "medium"
    else:
        confidence = "low"
    return round(score, 1), lean, confidence, reasons


def _derivatives_momentum_quality(symbol: str, current: Dict, price: float) -> Tuple[float, str, List[str], Dict]:
    """يقارن آخر Snapshot مشتقات بالقراءة الحالية لقياس جودة الزخم، لا اتجاه السعر وحده."""
    previous = db.last_derivatives_snapshot(symbol)
    score = 50.0
    reasons: List[str] = []
    deltas: Dict = {}

    if previous:
        prev_price = _safe_float(previous["price"], 0.0)
        price_delta = pct_change(price, prev_price) if prev_price > 0 else 0.0
        deltas["price_pct"] = round(price_delta, 3)

        def _delta(field: str, previous_field: str) -> Optional[float]:
            cur = current.get(field)
            old = previous[previous_field]
            if cur is None or old is None:
                return None
            return float(cur) - float(old)

        taker_delta = _delta("taker_ratio", "taker_ratio")
        oi_delta = _delta("oi_change_1h", "oi_change_1h")
        funding_delta = _delta("funding", "funding")
        top_delta = _delta("top_positions_ls", "top_positions_ls")
        deltas.update({
            "taker_delta": None if taker_delta is None else round(taker_delta, 3),
            "oi_acceleration": None if oi_delta is None else round(oi_delta, 3),
            "funding_delta": None if funding_delta is None else round(funding_delta, 5),
            "top_positions_delta": None if top_delta is None else round(top_delta, 3),
        })

        taker = current.get("taker_ratio")
        oi1h = current.get("oi_change_1h")
        funding = current.get("funding")

        if taker_delta is not None:
            if price_delta > 0.15 and taker_delta <= -0.20:
                score -= 8; reasons.append("السعر يصعد لكن Taker انقلب للبيع")
            elif price_delta > 0.15 and taker_delta >= 0.15:
                score += 9; reasons.append("السعر يصعد وTaker يتحسن")
            elif price_delta < -0.15 and taker_delta <= -0.15:
                score -= 8; reasons.append("الهبوط يتزامن مع زيادة ضغط البيع")

        if oi_delta is not None and oi1h is not None:
            if price_delta > 0.15 and float(oi1h) > 0 and oi_delta <= -2.0:
                score -= 5; reasons.append("نمو OI يتباطأ أثناء الصعود")
            elif price_delta > 0.15 and float(oi1h) > 0 and oi_delta >= 2.0:
                score += 8; reasons.append("OI يتسارع مع الصعود")
            elif price_delta < -0.15 and float(oi1h) > 2:
                score -= 8; reasons.append("مراكز جديدة تدخل أثناء الهبوط")

        if funding_delta is not None and funding is not None:
            if price_delta > 0 and funding_delta > 0.004:
                score -= 2; reasons.append("Funding يرتفع واللونغ يزدحم تدريجيًا")
            elif funding_delta < -0.004 and price_delta > 0:
                score += 3; reasons.append("Funding يهدأ رغم استمرار الصعود")

        global_ls = current.get("long_short")
        top_pos = current.get("top_positions_ls")
        if global_ls is not None and top_pos is not None:
            if float(global_ls) <= 1.0 and 1.15 <= float(top_pos) <= 2.5:
                score += 6; reasons.append("الكبار Long والجمهور غير مزدحم")
            elif float(global_ls) <= 1.0 and float(top_pos) > 2.5:
                score += 3; reasons.append("الكبار ما زالوا Long والجمهور متوازن")
            elif float(global_ls) >= 1.6 and float(top_pos) >= 2.0:
                score -= 6; reasons.append("ازدحام Long عند الجمهور والكبار")
    else:
        reasons.append("أول قراءة — نحتاج Snapshot لاحق لقياس تغير الزخم")

    fs = current.get("futures_spot_ratio")
    if previous and fs is not None and float(fs) >= 5:
        score -= 2; reasons.append("الحركة تقودها العقود أكثر من Spot")

    score = max(0.0, min(100.0, score))
    if score >= 75:
        label = "🟢 يتقوى"
    elif score >= 50:
        label = "🟡 مستمر/مختلط"
    elif score >= 30:
        label = "🟠 يبرد"
    else:
        label = "🔴 يتدهور"
    return round(score, 1), label, reasons, deltas


def _early_flow_score(analysis: Analysis, deriv: Dict) -> Dict:
    """رادار دخول مبكر: يفصل قوة التدفق عن جودة التوقيت حتى لا نطارد حركة متأخرة."""
    p = analysis.payload
    score = 45.0
    reasons: List[str] = []

    taker = deriv.get("taker_ratio")
    oi1h = deriv.get("oi_change_1h")
    funding = deriv.get("funding")
    top_pos = deriv.get("top_positions_ls")
    global_ls = deriv.get("long_short")
    fs = deriv.get("futures_spot_ratio")
    deltas = deriv.get("momentum_deltas") or {}
    oi_accel = deltas.get("oi_acceleration")
    taker_delta = deltas.get("taker_delta")

    if oi1h is not None:
        oi1h = float(oi1h)
        if oi1h >= 5:
            score += 13; reasons.append(f"OI يتوسع بقوة +{oi1h:.1f}%")
        elif oi1h >= 2:
            score += 8; reasons.append(f"OI يتوسع +{oi1h:.1f}%")
        elif oi1h < -2:
            score -= 8; reasons.append("OI ينكمش")

    if oi_accel is not None:
        oi_accel = float(oi_accel)
        if oi_accel >= 2:
            score += 8; reasons.append("OI يتسارع")
        elif oi_accel <= -2:
            score -= 5; reasons.append("نمو OI يتباطأ")

    if taker is not None:
        taker = float(taker)
        if taker >= 1.15:
            score += 13; reasons.append(f"Taker شراء قوي {taker:.2f}")
        elif taker >= 1.05:
            score += 8; reasons.append(f"Taker شراء أعلى {taker:.2f}")
        elif taker <= 0.85:
            score -= 10; reasons.append(f"Taker بيع قوي {taker:.2f}")
    if taker_delta is not None and float(taker_delta) >= 0.12:
        score += 5; reasons.append("Taker يتحسن")

    vol15 = float(p.get("volume_15m", 0) or 0)
    vol5 = float(p.get("volume_5m", 0) or 0)
    vol_build = float(p.get("volume_build", 0) or 0)
    if max(vol5, vol15) >= 1.2:
        score += 9; reasons.append("Spot Volume يتوسع")
    elif max(vol5, vol15) >= 0.9:
        score += 4
    if vol_build >= 1.05:
        score += 6; reasons.append("الفوليوم يتزايد تدريجيًا")

    if funding is not None:
        f = float(funding)
        if -0.01 <= f <= 0.02:
            score += 5; reasons.append("Funding ما زال معتدلًا")
        elif f >= 0.05:
            score -= 8; reasons.append("Funding مزدحم")

    if top_pos is not None and global_ls is not None:
        tp, gl = float(top_pos), float(global_ls)
        if tp >= 1.10 and gl <= 1.10:
            score += 6; reasons.append("الكبار Long والجمهور غير مزدحم")
        elif tp >= 2.5 and gl >= 1.5:
            score -= 5; reasons.append("ازدحام Long واسع")

    if fs is not None and float(fs) >= 8:
        score -= 5; reasons.append("العقود تهيمن على Spot")

    # Entry Timing مستقل: 100 = مبكر/قريب من البنية، 0 = مطاردة متأخرة.
    timing = 100.0
    late_reasons: List[str] = []
    rsi15 = float(p.get("rsi_15m", 50) or 50)
    rsi1h = float(p.get("rsi_1h", 50) or 50)
    change1h = float(p.get("change_1h_pct", 0) or 0)
    change15 = float(p.get("change_15m_pct", 0) or 0)
    ext = float(p.get("extension_atr", 0) or 0)

    if change1h > 3.0:
        penalty = min(40.0, (change1h - 3.0) * 6.0)
        timing -= penalty; late_reasons.append(f"ارتفع 1H +{change1h:.1f}%")
    if change15 > 1.8:
        timing -= min(20.0, (change15 - 1.8) * 8.0); late_reasons.append(f"قفزة 15M +{change15:.1f}%")
    if rsi15 > 68:
        timing -= min(25.0, (rsi15 - 68) * 1.5); late_reasons.append(f"RSI 15M مرتفع {rsi15:.1f}")
    if rsi1h > 72:
        timing -= min(20.0, (rsi1h - 72) * 1.2); late_reasons.append(f"RSI 1H مرتفع {rsi1h:.1f}")
    if ext > 0.85:
        timing -= min(30.0, (ext - 0.85) * 20.0); late_reasons.append(f"بعيد عن EMA/ATR {ext:.2f}")
    timing = max(0.0, min(100.0, timing))
    score = max(0.0, min(100.0, score))

    if timing >= 80:
        timing_label = "🟢 مبكر"
    elif timing >= 60:
        timing_label = "🟡 مقبول"
    elif timing >= 35:
        timing_label = "🟠 متأخر نسبيًا"
    else:
        timing_label = "🔴 متأخر — ممنوع المطاردة"

    strong = bool(score >= EARLY_FLOW_ALERT_SCORE and timing >= 60)
    entry_ok = bool(
        EARLY_FLOW_ENABLED
        and score >= EARLY_FLOW_ENTRY_SCORE
        and timing >= EARLY_FLOW_MIN_TIMING
        and float(analysis.coin_score) >= EARLY_FLOW_MIN_TECH_SCORE
        and 42 <= rsi15 <= 68
        and float(p.get("trend_5m", 0) or 0) >= 50
        and float(p.get("trend_15m", 0) or 0) >= 50
        and max(vol5, vol15) >= 0.90
    )
    return {
        "score": round(score, 1), "strong": strong, "entry_ok": entry_ok,
        "reasons": reasons[:6], "timing": round(timing, 1),
        "timing_label": timing_label, "late_reasons": late_reasons[:5],
    }


def _early_flow_alert_allowed(symbol: str) -> bool:
    key = f"early_flow_alert_at:{symbol.upper()}"
    raw = db.get_runtime(key, "")
    if raw:
        try:
            last = datetime.fromisoformat(raw)
            minutes = (datetime.now(timezone.utc) - last).total_seconds() / 60.0
            if minutes < EARLY_FLOW_ALERT_COOLDOWN_MINUTES:
                return False
        except Exception:
            pass
    db.set_runtime(key, utc_now())
    return True


def send_early_flow_alert(analysis: Analysis) -> None:
    flow = analysis.payload.get("early_flow") or {}
    if not flow.get("strong") or not _early_flow_alert_allowed(analysis.symbol):
        return
    deriv = analysis.payload.get("derivatives") or {}
    reasons = flow.get("reasons") or []
    why = " + ".join(reasons[:4]) if reasons else "تدفق مبكر متعدد العوامل"
    text = (
        f"🚨 Early Flow — {analysis.symbol}\n\n"
        f"💰 السعر: {analysis.price:.8f}\n"
        f"🌊 قوة التدفق: {float(flow.get('score',0)):.0f}/100\n"
        f"⏱️ توقيت الدخول: {float(flow.get('timing',0)):.0f}/100 — {flow.get('timing_label','')}\n"
        f"📈 OI 1H: {deriv.get('oi_change_1h','غير متاح')}%\n"
        f"⚔️ Taker: {deriv.get('taker_ratio','غير متاح')}\n"
        f"💸 Funding: {deriv.get('funding','غير متاح')}%\n\n"
        f"🧠 السبب: {why}\n"
        + ("✅ مرشح دخول مبكر للبوت إذا بقيت الحماية سليمة." if flow.get("entry_ok") else "👀 مراقبة مبكرة — لم تكتمل شروط الدخول بعد.")
    )
    notifier.send_once(
        f"EARLY_FLOW:{analysis.symbol}:{int(time.time() // (EARLY_FLOW_ALERT_COOLDOWN_MINUTES*60))}",
        text, {"symbol": analysis.symbol, "early_flow": flow, "derivatives": deriv},
    )


def get_derivatives_snapshot(symbol: str) -> Dict:
    if not DERIVATIVES_ENABLED or api.market_kind(symbol) != "SPOT":
        return {"available": False, "reason": "لا توجد مشتقات Binance Futures موثوقة لهذا السوق"}

    cache_key = f"deriv_snapshot:{symbol}"
    cached = api._cache_get(cache_key, DERIVATIVES_CACHE_SECONDS)
    if cached is not None:
        return cached

    base = "https://fapi.binance.com"
    out: Dict = {"available": True}

    try:
        premium = api.get_absolute(base + "/fapi/v1/premiumIndex", {"symbol": symbol})
        out["funding"] = _safe_float(premium.get("lastFundingRate")) * 100
    except Exception as exc:
        out["funding_error"] = str(exc)

    try:
        funding_hist = api.get_absolute(base + "/fapi/v1/fundingRate", {"symbol": symbol, "limit": 6})
        if isinstance(funding_hist, list):
            out["funding_trend"] = [round(_safe_float(x.get("fundingRate")) * 100, 5) for x in funding_hist[-6:]]
    except Exception as exc:
        out["funding_trend_error"] = str(exc)

    try:
        oi = api.get_absolute(base + "/fapi/v1/openInterest", {"symbol": symbol})
        out["oi_qty"] = _safe_float(oi.get("openInterest"))
        hist = api.get_absolute(base + "/futures/data/openInterestHist", {"symbol": symbol, "period": "5m", "limit": 13})
        if isinstance(hist, list) and hist:
            out["oi_value"] = _safe_float(hist[-1].get("sumOpenInterestValue"))
            if len(hist) >= 2:
                old = _safe_float(hist[0].get("sumOpenInterestValue"))
                new = _safe_float(hist[-1].get("sumOpenInterestValue"))
                out["oi_change_1h"] = pct_change(new, old) if old else 0.0
        hist24 = api.get_absolute(base + "/futures/data/openInterestHist", {"symbol": symbol, "period": "1h", "limit": 25})
        if isinstance(hist24, list) and len(hist24) >= 2:
            old24 = _safe_float(hist24[0].get("sumOpenInterestValue"))
            new24 = _safe_float(hist24[-1].get("sumOpenInterestValue"))
            out["oi_change_24h"] = pct_change(new24, old24) if old24 else 0.0
    except Exception as exc:
        out["oi_error"] = str(exc)

    endpoints = (
        ("long_short", "/futures/data/globalLongShortAccountRatio", "longShortRatio"),
        ("top_accounts_ls", "/futures/data/topLongShortAccountRatio", "longShortRatio"),
        ("top_positions_ls", "/futures/data/topLongShortPositionRatio", "longShortRatio"),
        ("taker_ratio", "/futures/data/takerlongshortRatio", "buySellRatio"),
    )
    for key, path, field in endpoints:
        try:
            rows = api.get_absolute(base + path, {"symbol": symbol, "period": "5m", "limit": 1})
            if isinstance(rows, list) and rows:
                out[key] = _safe_float(rows[-1].get(field), 0.0)
        except Exception as exc:
            out[key + "_error"] = str(exc)

    try:
        ft = api.get_absolute(base + "/fapi/v1/ticker/24hr", {"symbol": symbol})
        out["futures_quote_volume_24h"] = _safe_float(ft.get("quoteVolume"))
    except Exception as exc:
        out["futures_volume_error"] = str(exc)

    try:
        spot = api.market_ticker_24h(symbol)
        out["spot_quote_volume_24h"] = _safe_float(spot.get("quoteVolume"))
    except Exception as exc:
        out["spot_volume_error"] = str(exc)

    fv = _safe_float(out.get("futures_quote_volume_24h"))
    sv = _safe_float(out.get("spot_quote_volume_24h"))
    oiv = _safe_float(out.get("oi_value"))
    if fv > 0 and sv > 0:
        out["futures_spot_ratio"] = fv / sv
    if oiv > 0 and fv > 0:
        out["oi_futures_volume_ratio"] = oiv / fv

    market_cap = _alpha_market_cap(symbol)
    if market_cap and market_cap > 0:
        out["market_cap"] = market_cap
        if oiv > 0:
            out["oi_market_cap_ratio"] = (oiv / market_cap) * 100.0
    else:
        out["market_cap_unavailable"] = True

    # Binance العام لا يوفر لنا إجمالي Liquidations 24h بشكل موثوق دون مصدر خارجي.
    out["liquidations_24h"] = None
    out["liquidations_note"] = "غير متاح من مصدر Binance العام بشكل موثوق"

    score, lean, confidence, reasons = _derivatives_score(out)
    out["derivatives_score"] = score
    out["lean"] = lean
    out["confidence"] = confidence
    out["derivatives_reasons"] = reasons

    # جودة الزخم تقارن القراءة الحالية بالسابقة: Taker/OI/Funding/ازدحام المراكز مع حركة السعر.
    try:
        current_price = api.market_price(symbol)
        mq, mq_label, mq_reasons, mq_deltas = _derivatives_momentum_quality(symbol, out, current_price)
        out["momentum_quality"] = mq
        out["momentum_label"] = mq_label
        out["momentum_reasons"] = mq_reasons
        out["momentum_deltas"] = mq_deltas
        db.add_derivatives_snapshot(symbol, current_price, out)
    except Exception as exc:
        out["momentum_quality_error"] = str(exc)

    return api._cache_set(cache_key, out)


def get_analysis(symbol: str, market_score: float) -> Analysis:
    learned_min = learner.effective_entry_score()
    analysis = analyze_symbol(
        symbol,
        api.market_klines(symbol, "5m", limit=120),
        api.market_klines(symbol, "15m", limit=160),
        api.market_klines(symbol, "1h", limit=140),
        market_score,
        learned_min,
    )
    analysis.payload["candle_time"] = analysis.candle_time
    analysis.payload["market_source"] = api.market_kind(symbol)
    analysis.payload["market_membership"] = api.market_membership(symbol)
    analysis.payload["is_binance_alpha"] = api.is_alpha_symbol(symbol)
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

    # لا نضغط Futures API على كل السوق: نفحص المشتقات عندما تكون العملة أصلًا قريبة من جودة الدخول.
    deriv = {}
    if DERIVATIVES_ENABLED and analysis.coin_score >= max(DERIVATIVES_MIN_TECH_SCORE, learned_min - 8):
        try:
            deriv = get_derivatives_snapshot(symbol)
        except Exception as exc:
            deriv = {"available": False, "reason": str(exc)}
    analysis.payload["derivatives"] = deriv

    # المشتقات تعدّل التقييم بنطاق محافظ (حوالي ±6 نقاط عند وزن 12%).
    technical_score = float(analysis.coin_score)
    deriv_score = deriv.get("derivatives_score") if isinstance(deriv, dict) else None
    if deriv_score is not None:
        adjusted = technical_score + (float(deriv_score) - 50.0) * DERIVATIVES_SCORE_WEIGHT
        analysis.coin_score = round(max(0.0, min(100.0, adjusted)), 1)
        analysis.payload["technical_coin_score"] = round(technical_score, 1)
        analysis.payload["coin_score"] = analysis.coin_score
        analysis.payload["derivatives_adjustment"] = round(analysis.coin_score - technical_score, 1)

    # جودة الزخم طبقة تأكيد ناعمة وليست شرط منع مستقلًا: تؤثر بنطاق صغير فقط.
    momentum_quality = deriv.get("momentum_quality") if isinstance(deriv, dict) else None
    if momentum_quality is not None:
        before_momentum = float(analysis.coin_score)
        momentum_adjusted = before_momentum + (float(momentum_quality) - 50.0) * MOMENTUM_QUALITY_SCORE_WEIGHT
        analysis.coin_score = round(max(0.0, min(100.0, momentum_adjusted)), 1)
        analysis.payload["coin_score"] = analysis.coin_score
        analysis.payload["momentum_quality"] = float(momentum_quality)
        analysis.payload["momentum_quality_adjustment"] = round(analysis.coin_score - before_momentum, 1)

    derivative_block = bool(
        isinstance(deriv, dict)
        and deriv.get("derivatives_score") is not None
        and float(deriv.get("derivatives_score")) <= DERIVATIVES_BLOCK_SCORE
        and str(deriv.get("confidence", "low")) in {"medium", "high"}
    )
    analysis.payload["derivatives_block"] = derivative_block

    # Chase Guard مركّب:
    # يلزم إجهاد فني واضح + ضغط مشتقات سلبي، وليس مجرد RSI مرتفع أو عامل منفرد.
    p = analysis.payload
    chase_technical = []
    chase_derivatives = []

    rsi_1h = float(p.get("rsi_1h", 50) or 50)
    change_1h = float(p.get("change_1h_pct", 0) or 0)
    distance_breakout = float(p.get("distance_to_breakout_pct", 999) or 999)
    volume_15m = float(p.get("volume_15m", 0) or 0)

    if rsi_1h >= CHASE_RSI_1H:
        chase_technical.append(f"RSI 1H مرتفع {rsi_1h:.1f}")
    if change_1h >= CHASE_CHANGE_1H_PCT:
        chase_technical.append(f"ارتفاع سريع 1H +{change_1h:.1f}%")
    if 0 <= distance_breakout <= CHASE_NEAR_RESISTANCE_PCT:
        chase_technical.append(f"قرب مقاومة {distance_breakout:.2f}%")
    if volume_15m < CHASE_WEAK_15M_VOLUME:
        chase_technical.append(f"فوليوم 15M غير كافٍ {volume_15m:.2f}x")

    if isinstance(deriv, dict) and deriv.get("available", True):
        ds = deriv.get("derivatives_score")
        taker = float(deriv.get("taker_ratio", 1) or 1)
        global_ls = float(deriv.get("long_short", 1) or 1)
        top_pos = float(deriv.get("top_positions_ls", 1) or 1)

        if ds is not None and float(ds) <= CHASE_DERIV_SCORE_MAX:
            chase_derivatives.append(f"Derivatives {float(ds):.0f}/100")
        if taker <= CHASE_TAKER_MAX:
            chase_derivatives.append(f"Taker Sell قوي {taker:.2f}")
        if global_ls >= CHASE_LONG_SHORT_MIN or top_pos >= CHASE_TOP_POSITIONS_MIN:
            chase_derivatives.append("ازدحام Longs")

    chase_guard = bool(
        CHASE_GUARD_ENABLED
        and len(chase_technical) >= 2
        and len(chase_derivatives) >= 1
        and (len(chase_technical) + len(chase_derivatives)) >= 4
    )
    chase_reasons = chase_technical + chase_derivatives
    analysis.payload["chase_guard"] = chase_guard
    analysis.payload["chase_guard_reasons"] = chase_reasons

    # Early Flow: مسار مبكر محافظ، لكنه لا يتجاوز أمان السوق أو المشتقات أو Chase Guard.
    early_flow = {"score": 0.0, "strong": False, "entry_ok": False, "timing": 0.0, "timing_label": "غير متاح", "reasons": []}
    if EARLY_FLOW_ENABLED and isinstance(deriv, dict) and deriv.get("available"):
        try:
            early_flow = _early_flow_score(analysis, deriv)
        except Exception as exc:
            early_flow = {"score": 0.0, "strong": False, "entry_ok": False, "timing": 0.0, "timing_label": "خطأ", "reasons": [str(exc)]}
    analysis.payload["early_flow"] = early_flow

    # قرار الدخول النهائي:
    # setup الفني + التقييم بعد المشتقات + أمان السوق + لا تحذير مشتقات قوي + لا مطاردة مركبة.
    normal_entry_ok = bool(analysis.payload.get("entry_setup_ok") and analysis.coin_score >= learned_min)
    early_entry_ok = bool(early_flow.get("entry_ok"))
    analysis.payload["normal_entry_ok"] = normal_entry_ok
    analysis.payload["early_flow_entry_ok"] = early_entry_ok
    analysis.entry_ok = bool(
        (normal_entry_ok or early_entry_ok)
        and market.market_safe
        and not derivative_block
        and not chase_guard
    )
    if derivative_block:
        analysis.reason = "انتظار — مشتقات تميل بقوة ضد دخول Spot"
    elif chase_guard:
        analysis.reason = "انتظار — Chase Guard: " + " + ".join(chase_reasons[:4])
    elif analysis.entry_ok:
        if early_entry_ok and not normal_entry_ok:
            analysis.reason = f"دخول Early Flow مبكر — {float(early_flow.get('score',0)):.0f}/100 | توقيت {float(early_flow.get('timing',0)):.0f}/100"
        else:
            analysis.reason = "دخول أول عالي الجودة — مؤكد برادار المشتقات" if deriv_score is not None else "دخول أول عالي الجودة"
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

    source_line = "\n🅰️ المصدر: Binance Alpha" if bool(analysis.payload.get("is_binance_alpha")) and analysis.payload.get("market_source") == "ALPHA" else ""
    message = f"""{title} — {position['symbol']}

{trade_details}{source_line}

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
    current_price = api.market_price(symbol)
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

            # تنبيه تلقائي عند ظهور تدفق مبكر قوي حتى لو لم تكتمل صفقة الدخول بعد.
            try:
                send_early_flow_alert(analysis)
            except Exception as exc:
                print(f"Early Flow alert error {symbol}: {exc}", flush=True)

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
        risk = universe.risk_reason(best.symbol, force_refresh=True)
        market = get_market_context()
        if risk:
            db.add_analysis(best.symbol, market_score, best.coin_score, "BLOCK", risk, best.payload)
            print(f"Entry blocked {best.symbol}: {risk}", flush=True)
            return
        if not market.market_safe:
            print(f"Entry blocked: market unsafe | {market.reason}", flush=True)
            return

        # إعادة تحقق لحظة التنفيذ من Chase Guard والمشتقات بدل الاعتماد على Snapshot أقدم.
        fresh = get_analysis(best.symbol, market.score)
        if not fresh.entry_ok or fresh.payload.get("chase_guard"):
            reason = fresh.reason or "انتظار — فشل فحص الدخول النهائي"
            db.add_analysis(fresh.symbol, market.score, fresh.coin_score, "BLOCK", reason, fresh.payload)
            print(f"Entry blocked {fresh.symbol}: {reason}", flush=True)
            return

        buy_reason = (
            "دخول Early Flow مبكر — V3"
            if fresh.payload.get("early_flow_entry_ok") and not fresh.payload.get("normal_entry_ok")
            else "دخول مبكر قبل الانطلاق — V3"
        )
        position = broker.buy(
            fresh.symbol,
            fresh.price,
            buy_reason,
            fresh.payload,
        )
        best = fresh
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
