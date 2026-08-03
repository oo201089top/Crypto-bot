# V38: smart market manager / trade tracking built on V36 first-leg engine
# EPIC, HOME, PROM, NEIRO, EUL, UAI, COLLECT, IRYS, MYX, SCRT, ORDI, KAITO,
# RIF, ESP, ROSE, WLD and HEI examples.
# Keeps A/A+ only, catches the first leg, rejects late blow-off/re-breakout entries,
# and keeps BTC independence only when the altcoin structure is genuinely early.
import os, time, json
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple
import requests

# V27: ترتيب صريح عند تحقق أكثر من استراتيجية في الشمعة نفسها.
# هذا يطابق ترتيب elif التاريخي ولا يغير سلوك التداول.
MODE_PRIORITY = ("surge_continuation", "trend_ignition", "accumulation", "strong_reclaim", "reversal", "pullback", "momentum", "balanced")
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def ENGINE_ENV(prefix: str, name: str, default=None):
    """Read PREFIX_NAME first, then the legacy NAME for backward compatibility."""
    prefixed = f"{prefix}_{name}"
    if prefixed in os.environ:
        return os.environ[prefixed]
    return os.getenv(name, default)

BINANCE_BASE = "https://data-api.binance.vision"
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

SCAN_MINUTES = int(ENGINE_ENV("SPOT", "SCAN_MINUTES", "1"))
COOLDOWN_MINUTES = int(ENGINE_ENV("SPOT", "COOLDOWN_MINUTES", "120"))
MAX_ALERTS_PER_SCAN = int(ENGINE_ENV("SPOT", "MAX_ALERTS_PER_SCAN", "5"))
MAX_WORKERS = int(ENGINE_ENV("SPOT", "MAX_WORKERS", "16"))
SYMBOL_REFRESH_MINUTES = int(ENGINE_ENV("SPOT", "SYMBOL_REFRESH_MINUTES", "30"))
MIN_DAILY_QUOTE_VOLUME = float(ENGINE_ENV("SPOT", "MIN_DAILY_QUOTE_VOLUME", "500000"))
PREFILTER_LIMIT = int(ENGINE_ENV("SPOT", "PREFILTER_LIMIT", "60"))
MIN_SCORE = int(ENGINE_ENV("SPOT", "MIN_SCORE", "76"))
MIN_15M_VOLUME_RATIO = float(ENGINE_ENV("SPOT", "MIN_15M_VOLUME_RATIO", "1.6"))
MAX_1H_RISE_PCT = float(ENGINE_ENV("SPOT", "MAX_1H_RISE_PCT", "8"))
MAX_4H_RISE_PCT = float(ENGINE_ENV("SPOT", "MAX_4H_RISE_PCT", "14"))
MAX_15M_RISE_PCT = float(ENGINE_ENV("SPOT", "MAX_15M_RISE_PCT", "3"))
MAX_RISK_PCT = float(ENGINE_ENV("SPOT", "MAX_RISK_PCT", "4"))
MAX_CANDLE_BODY_PCT = float(ENGINE_ENV("SPOT", "MAX_CANDLE_BODY_PCT", "2.8"))
MAX_EMA20_DISTANCE_PCT = float(ENGINE_ENV("SPOT", "MAX_EMA20_DISTANCE_PCT", "1.5"))
MAX_CONSECUTIVE_GREEN = int(ENGINE_ENV("SPOT", "MAX_CONSECUTIVE_GREEN", "2"))

# فلاتر جودة الاختراق V2.2
BREAKOUT_CONFIRM_5M = ENGINE_ENV("SPOT", "BREAKOUT_CONFIRM_5M", "1") == "1"
CONFIRM_MAX_DIP_PCT = float(ENGINE_ENV("SPOT", "CONFIRM_MAX_DIP_PCT", "0.8"))
MIN_CONFIRM_CLOSE_LOCATION = float(ENGINE_ENV("SPOT", "MIN_CONFIRM_CLOSE_LOCATION", "0.55"))
MAX_RSI_HARD = float(ENGINE_ENV("SPOT", "MAX_RSI_HARD", "70"))
MAX_BREAKOUT_RANGE_ATR = float(ENGINE_ENV("SPOT", "MAX_BREAKOUT_RANGE_ATR", "2.0"))
MAX_BREAKOUT_BODY_ATR = float(ENGINE_ENV("SPOT", "MAX_BREAKOUT_BODY_ATR", "1.6"))
MIN_NEXT_RESISTANCE_PCT = float(ENGINE_ENV("SPOT", "MIN_NEXT_RESISTANCE_PCT", "2.5"))
MOMENTUM_MIN_NEXT_RESISTANCE_PCT = float(ENGINE_ENV("SPOT", "MOMENTUM_MIN_NEXT_RESISTANCE_PCT", "1.5"))
RESISTANCE_MAX_DISTANCE_PCT = float(ENGINE_ENV("SPOT", "RESISTANCE_MAX_DISTANCE_PCT", "15"))
RESISTANCE_SWING_WINDOW = int(ENGINE_ENV("SPOT", "RESISTANCE_SWING_WINDOW", "2"))
RESISTANCE_CLUSTER_TOLERANCE_PCT = float(ENGINE_ENV("SPOT", "RESISTANCE_CLUSTER_TOLERANCE_PCT", "0.45"))
RESISTANCE_MIN_TOUCHES = int(ENGINE_ENV("SPOT", "RESISTANCE_MIN_TOUCHES", "2"))
REJECTION_LOG_ENABLED = ENGINE_ENV("SPOT", "REJECTION_LOG_ENABLED", "1") == "1"
REJECTION_LOG_FILE = Path(ENGINE_ENV("SPOT", "REJECTION_LOG_FILE", "rejected_signals.jsonl"))

# فلتر سوق ذكي متعدد الفريمات V2.4
SMART_MARKET_FILTER = ENGINE_ENV("SPOT", "SMART_MARKET_FILTER", "1") == "1"
BTC_5M_DROP_BLOCK_PCT = float(ENGINE_ENV("SPOT", "BTC_5M_DROP_BLOCK_PCT", "0.75"))
BTC_15M_RSI_BLOCK = float(ENGINE_ENV("SPOT", "BTC_15M_RSI_BLOCK", "43"))
BTC_1H_DROP_BLOCK_PCT = float(ENGINE_ENV("SPOT", "BTC_1H_DROP_BLOCK_PCT", "1.2"))
BTC_WEAK_RSI_15M = float(ENGINE_ENV("SPOT", "BTC_WEAK_RSI_15M", "48"))
BTC_WEAK_RSI_5M = float(ENGINE_ENV("SPOT", "BTC_WEAK_RSI_5M", "45"))
BTC_WEAK_15M_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_WEAK_15M_DROP_PCT", "0.35"))
BTC_WEAK_MIN_RELATIVE_STRENGTH = float(ENGINE_ENV("SPOT", "BTC_WEAK_MIN_RELATIVE_STRENGTH", "1.75"))

# V25: استثناء ذكي للعملات المستقلة أثناء ضعف BTC، مع بقاء منع الانهيار الحقيقي.
BTC_OVERRIDE_ENABLED = ENGINE_ENV("SPOT", "BTC_OVERRIDE_ENABLED", "1") == "1"
BTC_OVERRIDE_MIN_RELATIVE_STRENGTH = float(ENGINE_ENV("SPOT", "BTC_OVERRIDE_MIN_RELATIVE_STRENGTH", "3.0"))
BTC_OVERRIDE_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "BTC_OVERRIDE_MIN_VOLUME_15M", "1.8"))
BTC_OVERRIDE_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "BTC_OVERRIDE_MIN_VOLUME_5M", "1.25"))
BTC_OVERRIDE_MIN_MTF_SCORE = float(ENGINE_ENV("SPOT", "BTC_OVERRIDE_MIN_MTF_SCORE", "60"))
BTC_CATASTROPHIC_5M_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_CATASTROPHIC_5M_DROP_PCT", "1.20"))
BTC_CATASTROPHIC_15M_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_CATASTROPHIC_15M_DROP_PCT", "1.80"))
BTC_CATASTROPHIC_1H_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_CATASTROPHIC_1H_DROP_PCT", "2.50"))

# V26: استثناء أقوى للعملات المنفصلة جدًا عن BTC، حتى أثناء الحظر الكارثي النظري،
# مع إبقاء منع انهيار BTC اللحظي الحقيقي (Flash Crash).
BTC_EXTREME_OVERRIDE_ENABLED = ENGINE_ENV("SPOT", "BTC_EXTREME_OVERRIDE_ENABLED", "1") == "1"
BTC_EXTREME_MIN_RELATIVE_STRENGTH = float(ENGINE_ENV("SPOT", "BTC_EXTREME_MIN_RELATIVE_STRENGTH", "4.5"))
BTC_EXTREME_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "BTC_EXTREME_MIN_VOLUME_15M", "2.4"))
BTC_EXTREME_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "BTC_EXTREME_MIN_VOLUME_5M", "1.5"))
BTC_EXTREME_MIN_MTF_SCORE = float(ENGINE_ENV("SPOT", "BTC_EXTREME_MIN_MTF_SCORE", "58"))
BTC_FLASH_CRASH_5M_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_FLASH_CRASH_5M_DROP_PCT", "2.0"))
BTC_FLASH_CRASH_15M_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_FLASH_CRASH_15M_DROP_PCT", "3.0"))
BTC_FLASH_CRASH_1H_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_FLASH_CRASH_1H_DROP_PCT", "4.5"))

# V26: تأكيد مرن للانطلاقة المبكرة؛ يسمح بإغلاق 5m المتزامن مع 15m
# وبـ MACD 5m محايد قليلًا إذا ثبت الاختراق وكان هيكل Trend Ignition قويًا.
TI_CONFIRM_ALLOW_FLAT_MACD = ENGINE_ENV("SPOT", "TI_CONFIRM_ALLOW_FLAT_MACD", "1") == "1"
TI_CONFIRM_MAX_NEGATIVE_HIST_RATIO = float(ENGINE_ENV("SPOT", "TI_CONFIRM_MAX_NEGATIVE_HIST_RATIO", "0.15"))

# V28: مسار A المبكر — يسمح بإشارة A فقط عندما يكون النمط الأساسي قويًا
# لكن السكور النهائي ناقص بسبب شروط ثانوية أو ضعف السوق، دون تمرير B/C.
EARLY_A_ENABLED = ENGINE_ENV("SPOT", "EARLY_A_ENABLED", "1") == "1"
EARLY_A_MAX_SCORE_GAP = int(ENGINE_ENV("SPOT", "EARLY_A_MAX_SCORE_GAP", "8"))
EARLY_A_MIN_MTF_SCORE = float(ENGINE_ENV("SPOT", "EARLY_A_MIN_MTF_SCORE", "70"))
EARLY_A_MIN_RELATIVE_STRENGTH = float(ENGINE_ENV("SPOT", "EARLY_A_MIN_RELATIVE_STRENGTH", "1.50"))
EARLY_A_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "EARLY_A_MIN_VOLUME_15M", "2.0"))
EARLY_A_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "EARLY_A_MIN_VOLUME_5M", "1.10"))
EARLY_A_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "EARLY_A_MAX_RSI_15M", "70"))
EARLY_A_MIN_STRUCTURE_SCORE = float(ENGINE_ENV("SPOT", "EARLY_A_MIN_STRUCTURE_SCORE", "70"))

# V29: مسار Strong Reclaim مستقل لالتقاط ارتداد قوي مبكر مثل DEXE دون إرسال B/C.
STRONG_RECLAIM_ENABLED = ENGINE_ENV("SPOT", "STRONG_RECLAIM_ENABLED", "1") == "1"
STRONG_RECLAIM_MIN_DRAWDOWN_PCT = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_DRAWDOWN_PCT", "2.5"))
STRONG_RECLAIM_MIN_RELATIVE_STRENGTH = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_RELATIVE_STRENGTH", "2.25"))
STRONG_RECLAIM_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_VOLUME_15M", "1.35"))
STRONG_RECLAIM_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_VOLUME_5M", "1.15"))
STRONG_RECLAIM_MIN_RSI_15M = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_RSI_15M", "45"))
STRONG_RECLAIM_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MAX_RSI_15M", "68"))
STRONG_RECLAIM_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_ADX_15M", "16"))
STRONG_RECLAIM_MIN_MTF_SCORE = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_MTF_SCORE", "40"))
STRONG_RECLAIM_MAX_EMA20_DISTANCE = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MAX_EMA20_DISTANCE", "2.0"))
STRONG_RECLAIM_MIN_SCORE = int(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MIN_SCORE", "84"))
STRONG_RECLAIM_MAX_RISK_PCT = float(ENGINE_ENV("SPOT", "STRONG_RECLAIM_MAX_RISK_PCT", "4.5"))

# V31: مسار اندفاع متكيف لالتقاط العملات التي تنفصل عن BTC وتكسر قممها
# على عدة فريمات. لا يضعف الفلاتر العامة؛ بل يعالج هذه الحالة كمسار مستقل A/A+.
SURGE_CONTINUATION_ENABLED = ENGINE_ENV("SPOT", "SURGE_CONTINUATION_ENABLED", "1") == "1"
SURGE_MIN_RELATIVE_STRENGTH = float(ENGINE_ENV("SPOT", "SURGE_MIN_RELATIVE_STRENGTH", "2.0"))
SURGE_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "SURGE_MIN_VOLUME_15M", "2.0"))
SURGE_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "SURGE_MIN_VOLUME_5M", "1.00"))
SURGE_MIN_MTF_SCORE = float(ENGINE_ENV("SPOT", "SURGE_MIN_MTF_SCORE", "70"))
SURGE_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "SURGE_MIN_ADX_15M", "14"))
SURGE_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "SURGE_MAX_RSI_15M", "74"))
SURGE_MAX_EXTENSION_ATR = float(ENGINE_ENV("SPOT", "SURGE_MAX_EXTENSION_ATR", "3.2"))
SURGE_MIN_SCORE = int(ENGINE_ENV("SPOT", "SURGE_MIN_SCORE", "84"))
SURGE_MAX_RISK_PCT = float(ENGINE_ENV("SPOT", "SURGE_MAX_RISK_PCT", "4.5"))
SURGE_RECENT_VOLUME_LOOKBACK = int(ENGINE_ENV("SPOT", "SURGE_RECENT_VOLUME_LOOKBACK", "4"))
SURGE_MIN_RECENT_VOLUME_15M = float(ENGINE_ENV("SPOT", "SURGE_MIN_RECENT_VOLUME_15M", "1.35"))
SURGE_MIN_RECENT_VOLUME_5M = float(ENGINE_ENV("SPOT", "SURGE_MIN_RECENT_VOLUME_5M", "1.15"))
SURGE_NEAR_HIGH_TOLERANCE_PCT = float(ENGINE_ENV("SPOT", "SURGE_NEAR_HIGH_TOLERANCE_PCT", "1.2"))
SURGE_MIN_RISING_CLOSES = int(ENGINE_ENV("SPOT", "SURGE_MIN_RISING_CLOSES", "4"))

# V33: التقاط بداية الموجة ومنع الإشارة بعد امتداد الحركة.
EARLY_WAVE_ENABLED = ENGINE_ENV("SPOT", "EARLY_WAVE_ENABLED", "1") == "1"
EARLY_WAVE_MIN_PROJECTED_VOLUME_15M = float(ENGINE_ENV("SPOT", "EARLY_WAVE_MIN_PROJECTED_VOLUME_15M", "1.30"))
EARLY_WAVE_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "EARLY_WAVE_MIN_VOLUME_5M", "1.10"))
EARLY_WAVE_MIN_CLOSE_LOCATION = float(ENGINE_ENV("SPOT", "EARLY_WAVE_MIN_CLOSE_LOCATION", "0.55"))
EARLY_WAVE_MAX_BREAKOUT_EXTENSION_PCT = float(ENGINE_ENV("SPOT", "EARLY_WAVE_MAX_BREAKOUT_EXTENSION_PCT", "1.80"))
EARLY_WAVE_MAX_EMA20_DISTANCE_ATR = float(ENGINE_ENV("SPOT", "EARLY_WAVE_MAX_EMA20_DISTANCE_ATR", "1.80"))
CHASE_MAX_BREAKOUT_EXTENSION_PCT = float(ENGINE_ENV("SPOT", "CHASE_MAX_BREAKOUT_EXTENSION_PCT", "3.20"))
CHASE_MAX_EMA20_DISTANCE_ATR = float(ENGINE_ENV("SPOT", "CHASE_MAX_EMA20_DISTANCE_ATR", "2.60"))
CHASE_MAX_15M_RISE_PCT = float(ENGINE_ENV("SPOT", "CHASE_MAX_15M_RISE_PCT", "3.0"))
CHASE_MAX_1H_RISE_PCT = float(ENGINE_ENV("SPOT", "CHASE_MAX_1H_RISE_PCT", "6.0"))
CHASE_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "CHASE_MAX_RSI_15M", "75"))

# V34: فلاتر منع الإشارة عند القمة أو بعد انتهاء الموجة الأولى.
LATE_BREAKOUT_FILTER_ENABLED = ENGINE_ENV("SPOT", "LATE_BREAKOUT_FILTER_ENABLED", "1") == "1"
LATE_MAX_WAVE_PROGRESS = float(ENGINE_ENV("SPOT", "LATE_MAX_WAVE_PROGRESS", "0.45"))
LATE_MAX_GREEN_15M = int(ENGINE_ENV("SPOT", "LATE_MAX_GREEN_15M", "3"))
LATE_MAX_BASE_DISTANCE_ATR = float(ENGINE_ENV("SPOT", "LATE_MAX_BASE_DISTANCE_ATR", "2.40"))
LATE_MAX_BASE_DISTANCE_PCT = float(ENGINE_ENV("SPOT", "LATE_MAX_BASE_DISTANCE_PCT", "5.0"))
LATE_BLOWOFF_MIN_RISE_PCT = float(ENGINE_ENV("SPOT", "LATE_BLOWOFF_MIN_RISE_PCT", "6.0"))
LATE_BLOWOFF_MIN_RANGE_ATR = float(ENGINE_ENV("SPOT", "LATE_BLOWOFF_MIN_RANGE_ATR", "2.2"))
LATE_BLOWOFF_LOOKBACK = int(ENGINE_ENV("SPOT", "LATE_BLOWOFF_LOOKBACK", "12"))
LATE_REBREAK_LOOKBACK = int(ENGINE_ENV("SPOT", "LATE_REBREAK_LOOKBACK", "18"))
LATE_REBREAK_RESET_PCT = float(ENGINE_ENV("SPOT", "LATE_REBREAK_RESET_PCT", "1.8"))
LATE_VOLUME_DECAY_RATIO = float(ENGINE_ENV("SPOT", "LATE_VOLUME_DECAY_RATIO", "0.58"))
LATE_MIN_ROOM_DAILY_PCT = float(ENGINE_ENV("SPOT", "LATE_MIN_ROOM_DAILY_PCT", "1.5"))

# V36: نافذة دخول صارمة — أول شمعة أو ثاني شمعة فقط بعد الكسر.
FIRST_LEG_ONLY_ENABLED = ENGINE_ENV("SPOT", "FIRST_LEG_ONLY_ENABLED", "1") == "1"
FIRST_LEG_LOOKBACK = int(ENGINE_ENV("SPOT", "FIRST_LEG_LOOKBACK", "10"))
FIRST_LEG_REFERENCE_BARS = int(ENGINE_ENV("SPOT", "FIRST_LEG_REFERENCE_BARS", "14"))
FIRST_LEG_MAX_CLOSED_BARS = int(ENGINE_ENV("SPOT", "FIRST_LEG_MAX_CLOSED_BARS", "1"))
FIRST_LEG_MAX_EXTENSION_PCT = float(ENGINE_ENV("SPOT", "FIRST_LEG_MAX_EXTENSION_PCT", "1.80"))
FIRST_LEG_MAX_EXTENSION_ATR = float(ENGINE_ENV("SPOT", "FIRST_LEG_MAX_EXTENSION_ATR", "1.25"))
FIRST_LEG_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "FIRST_LEG_MAX_RSI_15M", "72"))
FIRST_LEG_MAX_TRIGGER_RANGE_ATR = float(ENGINE_ENV("SPOT", "FIRST_LEG_MAX_TRIGGER_RANGE_ATR", "1.80"))
FIRST_LEG_MAX_TRIGGER_BODY_ATR = float(ENGINE_ENV("SPOT", "FIRST_LEG_MAX_TRIGGER_BODY_ATR", "1.35"))
FIRST_LEG_MIN_VOLUME_RETENTION = float(ENGINE_ENV("SPOT", "FIRST_LEG_MIN_VOLUME_RETENTION", "0.60"))
FIRST_LEG_MIN_RESET_PCT = float(ENGINE_ENV("SPOT", "FIRST_LEG_MIN_RESET_PCT", "1.20"))

# V19: صحة BTC متعددة الفريمات 1W/1D/4H/1H/15M/5M
BTC_HEALTH_ENABLED = ENGINE_ENV("SPOT", "BTC_HEALTH_ENABLED", "1") == "1"
BTC_HEALTH_WEIGHTS = {"1w": 10, "1d": 20, "4h": 25, "1h": 20, "15m": 20, "5m": 5}
BTC_HEALTH_HARD_BLOCK_SCORE = float(ENGINE_ENV("SPOT", "BTC_HEALTH_HARD_BLOCK_SCORE", "42"))
BTC_HEALTH_WEAK_SCORE = float(ENGINE_ENV("SPOT", "BTC_HEALTH_WEAK_SCORE", "60"))
BTC_PULLBACK_MIN_HEALTH_SCORE = float(ENGINE_ENV("SPOT", "BTC_PULLBACK_MIN_HEALTH_SCORE", "55"))
BTC_PULLBACK_MAX_15M_DROP_PCT = float(ENGINE_ENV("SPOT", "BTC_PULLBACK_MAX_15M_DROP_PCT", "0.50"))
BTC_HEALTH_WEAK_SCORE_BONUS = int(ENGINE_ENV("SPOT", "BTC_HEALTH_WEAK_SCORE_BONUS", "5"))

# V20: استثناء القوة الاستثنائية لالتقاط العملات المستقلة عن BTC مثل EUL
EXCEPTIONAL_STRENGTH_ENABLED = ENGINE_ENV("SPOT", "EXCEPTIONAL_STRENGTH_ENABLED", "1") == "1"
EXCEPTIONAL_MIN_RELATIVE_STRENGTH = float(ENGINE_ENV("SPOT", "EXCEPTIONAL_MIN_RELATIVE_STRENGTH", "3.0"))
EXCEPTIONAL_MIN_MTF_SCORE = float(ENGINE_ENV("SPOT", "EXCEPTIONAL_MIN_MTF_SCORE", "78"))
EXCEPTIONAL_MIN_15M_SCORE = float(ENGINE_ENV("SPOT", "EXCEPTIONAL_MIN_15M_SCORE", "85"))
EXCEPTIONAL_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "EXCEPTIONAL_MIN_VOLUME_15M", "2.2"))
EXCEPTIONAL_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "EXCEPTIONAL_MIN_VOLUME_5M", "1.5"))
EXCEPTIONAL_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "EXCEPTIONAL_MIN_ADX_15M", "24"))
EXCEPTIONAL_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "EXCEPTIONAL_MAX_RSI_15M", "72"))
EXCEPTIONAL_SCORE_BONUS = int(ENGINE_ENV("SPOT", "EXCEPTIONAL_SCORE_BONUS", "6"))

# V21: Trend Ignition لالتقاط بدايات الترند الهادئة مثل MMT
TREND_IGNITION_ENABLED = ENGINE_ENV("SPOT","TREND_IGNITION_ENABLED","1")=="1"
TREND_IGNITION_MIN_MTF = float(ENGINE_ENV("SPOT","TREND_IGNITION_MIN_MTF","60"))
TREND_IGNITION_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT","TREND_IGNITION_MIN_VOLUME_15M","1.20"))
TREND_IGNITION_MIN_ADX = float(ENGINE_ENV("SPOT","TREND_IGNITION_MIN_ADX","12"))
TREND_IGNITION_MIN_RSI = float(ENGINE_ENV("SPOT","TREND_IGNITION_MIN_RSI","50"))
TREND_IGNITION_MAX_RSI = float(ENGINE_ENV("SPOT","TREND_IGNITION_MAX_RSI","66"))
TREND_IGNITION_MIN_VOLUME_BUILD = float(ENGINE_ENV("SPOT","TREND_IGNITION_MIN_VOLUME_BUILD","1.05"))
TREND_IGNITION_MAX_15M_RISE = float(ENGINE_ENV("SPOT","TREND_IGNITION_MAX_15M_RISE","2.6"))
TREND_IGNITION_MAX_1H_RISE = float(ENGINE_ENV("SPOT","TREND_IGNITION_MAX_1H_RISE","6.0"))
TREND_IGNITION_MAX_EMA20_DISTANCE = float(ENGINE_ENV("SPOT","TREND_IGNITION_MAX_EMA20_DISTANCE","1.25"))
TREND_IGNITION_MIN_SCORE = int(ENGINE_ENV("SPOT","TREND_IGNITION_MIN_SCORE","84"))
TREND_IGNITION_MAX_RISK_PCT = float(ENGINE_ENV("SPOT","TREND_IGNITION_MAX_RISK_PCT","4.0"))

# V24: Trend Ignition Structure Engine — يركز على بنية السعر قبل المؤشرات المتأخرة
TI_STRUCTURE_LOOKBACK = int(ENGINE_ENV("SPOT","TI_STRUCTURE_LOOKBACK","32"))
TI_STRUCTURE_MIN_BARS = int(ENGINE_ENV("SPOT","TI_STRUCTURE_MIN_BARS","20"))
TI_STRUCTURE_MAX_RANGE_PCT = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MAX_RANGE_PCT","7.0"))
TI_STRUCTURE_MAX_RECENT_RANGE_RATIO = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MAX_RECENT_RANGE_RATIO","0.72"))
TI_STRUCTURE_MIN_HIGHER_LOW_PCT = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MIN_HIGHER_LOW_PCT","0.05"))
TI_STRUCTURE_SHELF_TOLERANCE_PCT = float(ENGINE_ENV("SPOT","TI_STRUCTURE_SHELF_TOLERANCE_PCT","0.75"))
TI_STRUCTURE_MIN_SHELF_TOUCHES = int(ENGINE_ENV("SPOT","TI_STRUCTURE_MIN_SHELF_TOUCHES","3"))
TI_STRUCTURE_MIN_VOLUME_BUILD = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MIN_VOLUME_BUILD","1.05"))
TI_STRUCTURE_MIN_BREAKOUT_VOLUME = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MIN_BREAKOUT_VOLUME","1.25"))
TI_STRUCTURE_MAX_BREAKOUT_EXTENSION_PCT = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MAX_BREAKOUT_EXTENSION_PCT","4.5"))
TI_STRUCTURE_MIN_CLOSE_LOCATION = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MIN_CLOSE_LOCATION","0.58"))
TI_STRUCTURE_MIN_SCORE = float(ENGINE_ENV("SPOT","TI_STRUCTURE_MIN_SCORE","70"))


# مسار الارتداد الذكي بعد الهبوط
REVERSAL_ENABLED = ENGINE_ENV("SPOT", "REVERSAL_ENABLED", "1") == "1"
REVERSAL_MIN_DRAWDOWN_PCT = float(ENGINE_ENV("SPOT", "REVERSAL_MIN_DRAWDOWN_PCT", "3.5"))
REVERSAL_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "REVERSAL_MIN_VOLUME_15M", "1.5"))
REVERSAL_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "REVERSAL_MIN_VOLUME_5M", "1.2"))
REVERSAL_MIN_RSI_NOW = float(ENGINE_ENV("SPOT", "REVERSAL_MIN_RSI_NOW", "42"))
REVERSAL_MAX_RSI_NOW = float(ENGINE_ENV("SPOT", "REVERSAL_MAX_RSI_NOW", "64"))
REVERSAL_MAX_RSI_RECENT_LOW = float(ENGINE_ENV("SPOT", "REVERSAL_MAX_RSI_RECENT_LOW", "40"))
REVERSAL_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "REVERSAL_MIN_ADX_15M", "18"))
REVERSAL_MIN_SCORE = int(ENGINE_ENV("SPOT", "REVERSAL_MIN_SCORE", "84"))
REVERSAL_MAX_RISK_PCT = float(ENGINE_ENV("SPOT", "REVERSAL_MAX_RISK_PCT", "4.5"))
REVERSAL_A_MIN_MTF_SCORE = float(ENGINE_ENV("SPOT", "REVERSAL_A_MIN_MTF_SCORE", "60"))
REVERSAL_A_MIN_4H_SCORE = float(ENGINE_ENV("SPOT", "REVERSAL_A_MIN_4H_SCORE", "60"))
REVERSAL_A_MIN_1H_SCORE = float(ENGINE_ENV("SPOT", "REVERSAL_A_MIN_1H_SCORE", "40"))

# مسار الزخم القوي مثل SHIB
MOMENTUM_ENABLED = ENGINE_ENV("SPOT", "MOMENTUM_ENABLED", "1") == "1"
MOMENTUM_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "MOMENTUM_MIN_VOLUME_15M", "3.0"))
MOMENTUM_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "MOMENTUM_MIN_VOLUME_5M", "2.0"))
MOMENTUM_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "MOMENTUM_MIN_ADX_15M", "25"))
MOMENTUM_MIN_RSI_15M = float(ENGINE_ENV("SPOT", "MOMENTUM_MIN_RSI_15M", "54"))
MOMENTUM_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "MOMENTUM_MAX_RSI_15M", "68"))
MOMENTUM_MAX_15M_RISE = float(ENGINE_ENV("SPOT", "MOMENTUM_MAX_15M_RISE", "5.5"))
MOMENTUM_MAX_1H_RISE = float(ENGINE_ENV("SPOT", "MOMENTUM_MAX_1H_RISE", "12"))
MOMENTUM_MAX_EMA20_DISTANCE = float(ENGINE_ENV("SPOT", "MOMENTUM_MAX_EMA20_DISTANCE", "2.6"))
MOMENTUM_MAX_GREEN = int(ENGINE_ENV("SPOT", "MOMENTUM_MAX_GREEN", "4"))
MOMENTUM_MIN_SCORE = int(ENGINE_ENV("SPOT", "MOMENTUM_MIN_SCORE", "84"))

# المسار الرابع: تجميع مبكر قبل الانطلاقة
ACCUMULATION_ENABLED = ENGINE_ENV("SPOT", "ACCUMULATION_ENABLED", "1") == "1"
ACCUMULATION_LOOKBACK_15M = int(ENGINE_ENV("SPOT", "ACCUMULATION_LOOKBACK_15M", "16"))
ACCUMULATION_MAX_BASE_RANGE_PCT = float(ENGINE_ENV("SPOT", "ACCUMULATION_MAX_BASE_RANGE_PCT", "4.2"))
ACCUMULATION_MAX_AVG_BODY_ATR = float(ENGINE_ENV("SPOT", "ACCUMULATION_MAX_AVG_BODY_ATR", "0.75"))
ACCUMULATION_MIN_HIGHER_LOW_PCT = float(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_HIGHER_LOW_PCT", "0.10"))
ACCUMULATION_MIN_VOLUME_BUILD = float(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_VOLUME_BUILD", "1.12"))
ACCUMULATION_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_VOLUME_15M", "1.25"))
ACCUMULATION_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_VOLUME_5M", "1.10"))
ACCUMULATION_MIN_RSI_15M = float(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_RSI_15M", "48"))
ACCUMULATION_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "ACCUMULATION_MAX_RSI_15M", "67"))
ACCUMULATION_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_ADX_15M", "16"))
ACCUMULATION_MAX_15M_RISE = float(ENGINE_ENV("SPOT", "ACCUMULATION_MAX_15M_RISE", "2.8"))
ACCUMULATION_MAX_1H_RISE = float(ENGINE_ENV("SPOT", "ACCUMULATION_MAX_1H_RISE", "6.0"))
ACCUMULATION_MAX_EMA20_DISTANCE = float(ENGINE_ENV("SPOT", "ACCUMULATION_MAX_EMA20_DISTANCE", "1.35"))
ACCUMULATION_MIN_NEXT_RESISTANCE_PCT = float(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_NEXT_RESISTANCE_PCT", "1.2"))
ACCUMULATION_MIN_SCORE = int(ENGINE_ENV("SPOT", "ACCUMULATION_MIN_SCORE", "84"))
ACCUMULATION_MAX_RISK_PCT = float(ENGINE_ENV("SPOT", "ACCUMULATION_MAX_RISK_PCT", "4.0"))

MARKET_CACHE_SECONDS = int(ENGINE_ENV("SPOT", "MARKET_CACHE_SECONDS", "55"))
TRACK_RESULTS = ENGINE_ENV("SPOT", "TRACK_RESULTS", "1") == "1"

# V37: أوامر تيليجرام التفاعلية وتقارير السوق/التشخيص.
COMMANDS_ENABLED = ENGINE_ENV("SPOT", "COMMANDS_ENABLED", "1") == "1"
COMMAND_POLL_SECONDS = float(ENGINE_ENV("SPOT", "COMMAND_POLL_SECONDS", "2"))
COMMAND_OFFSET_FILE = Path(ENGINE_ENV("SPOT", "COMMAND_OFFSET_FILE", "telegram_update_offset.json"))
DEBUG_MAX_ROWS = int(ENGINE_ENV("SPOT", "DEBUG_MAX_ROWS", "3"))

# V38: إدارة السوق والمتابعة الحية بدون تغيير استراتيجية الدخول الأساسية.
MARKET_ENVIRONMENT_ENABLED = ENGINE_ENV("SPOT", "MARKET_ENVIRONMENT_ENABLED", "1") == "1"
TRACKER_NOTIFICATIONS_ENABLED = ENGINE_ENV("SPOT", "TRACKER_NOTIFICATIONS_ENABLED", "1") == "1"
TRACKER_NOTIFY_TP = ENGINE_ENV("SPOT", "TRACKER_NOTIFY_TP", "1") == "1"
TRACKER_NOTIFY_STOP = ENGINE_ENV("SPOT", "TRACKER_NOTIFY_STOP", "1") == "1"
TRACKER_NOTIFY_RECOVERY = ENGINE_ENV("SPOT", "TRACKER_NOTIFY_RECOVERY", "0") == "1"
TRACKER_RECOVERY_PCT = float(ENGINE_ENV("SPOT", "TRACKER_RECOVERY_PCT", "0.35"))

# تقييم الاتجاه متعدد الفريمات — أوزان وليست شروط منع قاطعة
MTF_ENABLED = ENGINE_ENV("SPOT", "MTF_ENABLED", "1") == "1"
MTF_SCORE_IMPACT = float(ENGINE_ENV("SPOT", "MTF_SCORE_IMPACT", "0.20"))
MTF_WEIGHTS = {"1w": 10, "1d": 20, "4h": 25, "1h": 20, "15m": 20, "5m": 5}
MTF_NEW_COIN_WEEKLY_BARS = int(ENGINE_ENV("SPOT", "MTF_NEW_COIN_WEEKLY_BARS", "60"))
MTF_NEW_COIN_DAILY_BARS = int(ENGINE_ENV("SPOT", "MTF_NEW_COIN_DAILY_BARS", "120"))
HOT_RSI_15M = float(ENGINE_ENV("SPOT", "HOT_RSI_15M", "75"))
HOT_RSI_MAX_BODY_ATR = float(ENGINE_ENV("SPOT", "HOT_RSI_MAX_BODY_ATR", "1.15"))

# وضع الانطلاقة المبكرة V17
LAUNCH_MODE_ENABLED = ENGINE_ENV("SPOT", "LAUNCH_MODE_ENABLED", "1") == "1"
LAUNCH_MIN_15M_SCORE = float(ENGINE_ENV("SPOT", "LAUNCH_MIN_15M_SCORE", "90"))
LAUNCH_MIN_5M_SCORE = float(ENGINE_ENV("SPOT", "LAUNCH_MIN_5M_SCORE", "95"))
LAUNCH_MIN_1H_SCORE = float(ENGINE_ENV("SPOT", "LAUNCH_MIN_1H_SCORE", "75"))
LAUNCH_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "LAUNCH_MIN_VOLUME_15M", "2.0"))
LAUNCH_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "LAUNCH_MIN_ADX_15M", "22"))
LAUNCH_MIN_RSI_15M = float(ENGINE_ENV("SPOT", "LAUNCH_MIN_RSI_15M", "55"))
LAUNCH_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "LAUNCH_MAX_RSI_15M", "72"))
LAUNCH_SCORE_BONUS = int(ENGINE_ENV("SPOT", "LAUNCH_SCORE_BONUS", "4"))
LAUNCH_THRESHOLD_RELIEF = int(ENGINE_ENV("SPOT", "LAUNCH_THRESHOLD_RELIEF", "2"))
LAUNCH_MTF_WEIGHTS = {"1w": 3, "1d": 7, "4h": 15, "1h": 30, "15m": 35, "5m": 10}

# V18: المسار الخامس Trend Pullback + تصنيف جودة الإشارة
PULLBACK_ENABLED = ENGINE_ENV("SPOT", "PULLBACK_ENABLED", "1") == "1"
PULLBACK_MIN_RSI_15M = float(ENGINE_ENV("SPOT", "PULLBACK_MIN_RSI_15M", "38"))
PULLBACK_MAX_RSI_15M = float(ENGINE_ENV("SPOT", "PULLBACK_MAX_RSI_15M", "56"))
PULLBACK_MIN_ADX_15M = float(ENGINE_ENV("SPOT", "PULLBACK_MIN_ADX_15M", "18"))
PULLBACK_MIN_VOLUME_5M = float(ENGINE_ENV("SPOT", "PULLBACK_MIN_VOLUME_5M", "1.15"))
PULLBACK_MAX_EMA20_DISTANCE_PCT = float(ENGINE_ENV("SPOT", "PULLBACK_MAX_EMA20_DISTANCE_PCT", "1.10"))
PULLBACK_MIN_SCORE = int(ENGINE_ENV("SPOT", "PULLBACK_MIN_SCORE", "84"))
PULLBACK_MAX_RISK_PCT = float(ENGINE_ENV("SPOT", "PULLBACK_MAX_RISK_PCT", "4.0"))
PULLBACK_MIN_NEXT_RESISTANCE_PCT = float(ENGINE_ENV("SPOT", "PULLBACK_MIN_NEXT_RESISTANCE_PCT", "1.5"))
PULLBACK_A_PLUS_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "PULLBACK_A_PLUS_MIN_VOLUME_15M", "1.20"))
PULLBACK_A_MIN_VOLUME_15M = float(ENGINE_ENV("SPOT", "PULLBACK_A_MIN_VOLUME_15M", "0.90"))

# تأثير MTF المتدرج في V18
MTF_STRONG_BONUS = int(ENGINE_ENV("SPOT", "MTF_STRONG_BONUS", "8"))
MTF_GOOD_BONUS = int(ENGINE_ENV("SPOT", "MTF_GOOD_BONUS", "5"))
MTF_WEAK_PENALTY = int(ENGINE_ENV("SPOT", "MTF_WEAK_PENALTY", "-6"))
STATE_FILE = Path("spot_signal_state.json")
SESSION = requests.Session()
_retry=Retry(total=2, backoff_factor=0.5, status_forcelist=[429,500,502,503,504], allowed_methods=frozenset(["GET","POST"]))
_adapter=HTTPAdapter(max_retries=_retry)
SESSION.mount("https://",_adapter)
SESSION.mount("http://",_adapter)
SYMBOL_CACHE = {"symbols": [], "updated_at": 0.0}
MARKET_CACHE = {"data": None, "updated_at": 0.0}
SYMBOL_CACHE_LOCK = Lock()
MARKET_CACHE_LOCK = Lock()
REJECTION_LOCK = Lock()

def log(msg:str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

STABLE_BASES = {"USDC","FDUSD","TUSD","USDP","DAI","USD1","BUSD","USDS","EUR","AEUR","EURT","TRY","BRL","GBP","AUD","BIDR","IDRT","UAH","RUB","NGN","VAI","PAX","UST","USTC"}
EXCLUDED_MAJORS = {x.strip().upper() for x in ENGINE_ENV("SPOT","EXCLUDED_MAJORS","").split(",") if x.strip()}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")



def log_rejection(symbol: str, reason: str, details: Optional[Dict] = None) -> None:
    """يسجل فقط أسباب رفض فلاتر الجودة الجديدة بدون إرسالها إلى تيليجرام."""
    if not REJECTION_LOG_ENABLED:
        return
    row = {
        "time": int(time.time() * 1000),
        "symbol": symbol,
        "reason": reason,
        "details": details or {},
    }
    log(f"Rejected {symbol}: {reason} | {row['details']}")
    try:
        with REJECTION_LOCK:
            if REJECTION_LOG_FILE.exists() and REJECTION_LOG_FILE.stat().st_size>20*1024*1024:
                REJECTION_LOG_FILE.replace(REJECTION_LOG_FILE.with_suffix(".jsonl.1"))
            with REJECTION_LOG_FILE.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as exc:
        log(f"Rejection log error: {exc}")


def nearest_overhead_resistance(candles: List[Dict], price: float, lookback: int = 100) -> Optional[Dict]:
    """يرجع أقرب مقاومة موثوقة فوق السعر من قمم محورية متكررة، لا من أي ذيل عشوائي."""
    if price <= 0 or len(candles) < (RESISTANCE_SWING_WINDOW * 2 + 5):
        return None

    sample = candles[-lookback:]
    swing_levels: List[float] = []
    w = max(1, RESISTANCE_SWING_WINDOW)

    # القمة المحورية يجب أن تتفوق على الشموع المحيطة بها.
    for i in range(w, len(sample) - w):
        high = float(sample[i]["high"])
        neighbors = sample[i-w:i] + sample[i+1:i+w+1]
        if high >= max(float(c["high"]) for c in neighbors):
            distance_pct = pct_change(price, high)
            if 0 < distance_pct <= RESISTANCE_MAX_DISTANCE_PCT:
                swing_levels.append(high)

    if not swing_levels:
        return None

    # نجمع القمم المتقاربة في منطقة مقاومة واحدة ونشترط تكرار اللمس.
    swing_levels.sort()
    clusters: List[List[float]] = []
    for level in swing_levels:
        placed = False
        for cluster in clusters:
            center = mean(cluster)
            if abs(level / center - 1) * 100 <= RESISTANCE_CLUSTER_TOLERANCE_PCT:
                cluster.append(level)
                placed = True
                break
        if not placed:
            clusters.append([level])

    valid = []
    for cluster in clusters:
        if len(cluster) < RESISTANCE_MIN_TOUCHES:
            continue
        level = mean(cluster)
        distance_pct = pct_change(price, level)
        if 0 < distance_pct <= RESISTANCE_MAX_DISTANCE_PCT:
            valid.append({"level": level, "distance_pct": distance_pct, "touches": len(cluster)})

    return min(valid, key=lambda x: x["distance_pct"]) if valid else None



def higher_low_structure(candles: List[Dict], lookback: int = 18) -> bool:
    """تأكيد مبسط لتحول الهيكل: قاع حديث أعلى مع استعادة قمة قصيرة."""
    if len(candles) < lookback + 4:
        return False
    w = candles[-lookback:]
    first = w[: lookback // 2]
    second = w[lookback // 2 : -1]
    if not first or not second:
        return False
    low1 = min(float(c["low"]) for c in first)
    low2 = min(float(c["low"]) for c in second)
    short_high = max(float(c["high"]) for c in w[-7:-2])
    return low2 > low1 and float(w[-1]["close"]) >= short_high * 0.997


def recent_drawdown_pct(candles: List[Dict], lookback: int = 16) -> float:
    if len(candles) < lookback:
        return 0.0
    w = candles[-lookback:]
    peak = max(float(c["high"]) for c in w[:-1])
    trough = min(float(c["low"]) for c in w)
    return max(0.0, -pct_change(peak, trough))


def close_location(candle: Dict) -> float:
    """موضع الإغلاق داخل مدى الشمعة: 0 عند القاع و1 عند القمة."""
    span = candle["high"] - candle["low"]
    return (candle["close"] - candle["low"]) / span if span > 0 else 0.5


def projected_volume_ratio(candle: Dict, average_volume: float, interval_seconds: int) -> float:
    """يقدّر نسبة حجم الشمعة الحية دون انتظار إغلاقها، مع حد محافظ للتضخيم."""
    if average_volume <= 0 or interval_seconds <= 0:
        return 0.0
    close_time_s = float(candle.get("close_time", 0)) / 1000.0
    remaining = max(0.0, close_time_s - time.time())
    elapsed_ratio = max(0.20, min(1.0, 1.0 - remaining / interval_seconds))
    raw_ratio = float(candle.get("volume", 0.0)) / average_volume
    return min(raw_ratio / elapsed_ratio, raw_ratio * 3.0)


def consecutive_green_closed(candles: List[Dict], lookback: int = 8) -> int:
    n = 0
    for c in reversed(candles[-lookback:]):
        if float(c["close"]) > float(c["open"]):
            n += 1
        else:
            break
    return n


def wave_stage_context(candles: List[Dict], price: float, ema20_value: float, atr_value: float) -> Dict:
    """يقيس هل السعر في بداية الموجة أم في آخرها/إعادة كسر متأخرة."""
    if len(candles) < 30 or price <= 0:
        return {"progress": 0.0, "base_distance_pct": 0.0, "base_distance_atr": 0.0,
                "blowoff": False, "rebreak_without_reset": False, "volume_decay": False,
                "green_15m": 0, "prior_peak": price, "base_low": price}
    w = candles[-30:]
    recent = w[-LATE_BLOWOFF_LOOKBACK:]
    base = w[:-max(6, LATE_BLOWOFF_LOOKBACK // 2)]
    base_low = min(float(c["low"]) for c in base) if base else min(float(c["low"]) for c in w)
    prior_peak = max(float(c["high"]) for c in recent[:-1]) if len(recent) > 1 else price
    total_leg = max(prior_peak - base_low, atr_value, price * 0.001)
    progress = max(0.0, min(2.0, (price - base_low) / total_leg))
    base_anchor = max(base_low, ema20_value if ema20_value > 0 else base_low)
    base_distance_pct = pct_change(base_anchor, price) if base_anchor > 0 else 999.0
    base_distance_atr = (price - base_anchor) / atr_value if atr_value > 0 else 999.0

    ranges = [float(c["high"]) - float(c["low"]) for c in recent]
    max_range_atr = max(ranges) / atr_value if atr_value > 0 and ranges else 0.0
    recent_low = min(float(c["low"]) for c in recent)
    recent_high = max(float(c["high"]) for c in recent)
    rise_from_low = pct_change(recent_low, recent_high) if recent_low > 0 else 0.0
    blowoff = bool(rise_from_low >= LATE_BLOWOFF_MIN_RISE_PCT and max_range_atr >= LATE_BLOWOFF_MIN_RANGE_ATR)

    rb = candles[-LATE_REBREAK_LOOKBACK:]
    previous_breaks = [i for i,c in enumerate(rb[:-2]) if float(c["close"]) >= prior_peak * 0.995]
    reset_low = min(float(c["low"]) for c in rb[-8:]) if rb else price
    reset_depth = max(0.0, -pct_change(prior_peak, reset_low)) if prior_peak > 0 else 0.0
    rebreak_without_reset = bool(previous_breaks and price >= prior_peak * 0.995 and reset_depth < LATE_REBREAK_RESET_PCT)

    vols = [float(c["volume"]) for c in recent]
    peak_vol = max(vols[:-2]) if len(vols) > 3 else max(vols or [0.0])
    tail_vol = sum(vols[-2:]) / 2 if len(vols) >= 2 else (vols[-1] if vols else 0.0)
    volume_decay = bool(peak_vol > 0 and tail_vol / peak_vol < LATE_VOLUME_DECAY_RATIO and price >= prior_peak * 0.985)

    return {
        "progress": progress,
        "base_distance_pct": base_distance_pct,
        "base_distance_atr": base_distance_atr,
        "blowoff": blowoff,
        "rebreak_without_reset": rebreak_without_reset,
        "volume_decay": volume_decay,
        "green_15m": consecutive_green_closed(candles, 8),
        "prior_peak": prior_peak,
        "base_low": base_low,
        "reset_depth_pct": reset_depth,
        "max_range_atr": max_range_atr,
        "rise_from_low_pct": rise_from_low,
    }


def first_leg_timing_context(
    candles: List[Dict],
    live_candle: Dict,
    live_price: float,
    atr_value: float,
    current_volume_ratio: float,
) -> Dict:
    """
    يحدد عمر الاختراق الحقيقي. العمر 0 = اختراق حي/آخر شمعة، 1 = الشمعة الثانية فقط.
    بعد ذلك تعتبر الفرصة فائتة ما لم يحدث تصحيح فعلي يعيد بناء قاعدة جديدة.
    """
    default = {
        "detected": False, "live_break": False, "age_bars": -1, "reference": 0.0,
        "extension_pct": 0.0, "extension_atr": 0.0, "trigger_range_atr": 0.0,
        "trigger_body_atr": 0.0, "volume_retention": 1.0, "reset_depth_pct": 0.0,
        "exhaustion": False, "late": False,
    }
    if len(candles) < FIRST_LEG_REFERENCE_BARS + 5 or live_price <= 0 or atr_value <= 0:
        return default

    start = max(FIRST_LEG_REFERENCE_BARS + 1, len(candles) - FIRST_LEG_LOOKBACK)
    trigger_idx = None
    trigger_ref = 0.0
    for i in range(start, len(candles)):
        prior = candles[max(0, i - FIRST_LEG_REFERENCE_BARS):i-1]
        if len(prior) < max(6, FIRST_LEG_REFERENCE_BARS // 2):
            continue
        ref = max(float(c["high"]) for c in prior)
        c = candles[i]
        if float(c["close"]) >= ref * 0.998 and float(c["close"]) > float(c["open"]):
            trigger_idx, trigger_ref = i, ref
            break

    current_ref = max(float(c["high"]) for c in candles[-FIRST_LEG_REFERENCE_BARS:-1])
    live_break = bool(
        float(live_candle.get("close", live_price)) >= current_ref * 0.998
        and float(live_candle.get("close", live_price)) > float(live_candle.get("open", live_price))
    )
    if trigger_idx is None and live_break:
        trigger_idx = len(candles)
        trigger_ref = current_ref

    if trigger_idx is None or trigger_ref <= 0:
        return default

    age_bars = max(0, len(candles) - 1 - trigger_idx) if trigger_idx < len(candles) else 0
    trigger = candles[trigger_idx] if trigger_idx < len(candles) else live_candle
    trigger_range = float(trigger["high"]) - float(trigger["low"])
    trigger_body = abs(float(trigger["close"]) - float(trigger["open"]))
    extension_pct = pct_change(trigger_ref, live_price)
    extension_atr = max(0.0, live_price - trigger_ref) / atr_value
    trigger_volume = max(float(trigger.get("volume", 0.0)), 1e-12)
    current_volume = float(live_candle.get("volume", 0.0)) if live_break else float(candles[-1].get("volume", 0.0))
    raw_retention = current_volume / trigger_volume
    volume_retention = max(raw_retention, current_volume_ratio / max(1.0, current_volume_ratio)) if live_break else raw_retention

    post = candles[trigger_idx + 1:] if trigger_idx < len(candles) else []
    reset_low = min([float(c["low"]) for c in post] + [live_price]) if post else live_price
    reset_depth_pct = max(0.0, -pct_change(max(trigger_ref, float(trigger["high"])), reset_low))
    rebuilt = reset_depth_pct >= FIRST_LEG_MIN_RESET_PCT
    exhaustion = bool(
        trigger_range / atr_value > FIRST_LEG_MAX_TRIGGER_RANGE_ATR
        or trigger_body / atr_value > FIRST_LEG_MAX_TRIGGER_BODY_ATR
    )
    late = bool(
        (age_bars > FIRST_LEG_MAX_CLOSED_BARS and not rebuilt)
        or extension_pct > FIRST_LEG_MAX_EXTENSION_PCT
        or extension_atr > FIRST_LEG_MAX_EXTENSION_ATR
        or exhaustion
        or (age_bars >= 1 and volume_retention < FIRST_LEG_MIN_VOLUME_RETENTION and not rebuilt)
    )
    return {
        "detected": True, "live_break": live_break and trigger_idx == len(candles),
        "age_bars": age_bars, "reference": trigger_ref,
        "extension_pct": extension_pct, "extension_atr": extension_atr,
        "trigger_range_atr": trigger_range / atr_value,
        "trigger_body_atr": trigger_body / atr_value,
        "volume_retention": volume_retention, "reset_depth_pct": reset_depth_pct,
        "exhaustion": exhaustion, "rebuilt": rebuilt, "late": late,
    }


def coin_independence_score(relative_strength: float, volume_ratio_15m: float, mtf_score: float, btc_health_score: float, exceptional: bool = False) -> int:
    """درجة تفسيرية 0..100 لاستقلال العملة عن BTC، وليست احتمال ربح."""
    rel_points = max(0.0, min(45.0, relative_strength * 11.0))
    vol_points = max(0.0, min(25.0, (volume_ratio_15m - 1.0) * 12.5))
    mtf_points = max(0.0, min(20.0, (mtf_score - 45.0) * 0.55))
    weak_btc_bonus = max(0.0, min(10.0, (45.0 - btc_health_score) * 0.30))
    score = rel_points + vol_points + mtf_points + weak_btc_bonus + (5.0 if exceptional else 0.0)
    return int(round(max(0.0, min(100.0, score))))


def market_environment_score(market: Dict) -> Dict:
    """درجة بيئة السوق 0..100 مشتقة من صحة BTC والزخم متعدد الفريمات."""
    health = float(market.get("btc_health_score", 50.0))
    frames = market.get("btc_health_frames", {}) or {}
    btc = market.get("btc", {}) or {}
    frame_score = (
        float(frames.get("4h", 50)) * 0.30
        + float(frames.get("1h", 50)) * 0.30
        + float(frames.get("15m", 50)) * 0.25
        + float(frames.get("5m", 50)) * 0.15
    )
    momentum = 50.0
    momentum += max(-20.0, min(20.0, float(btc.get("rise_1h", 0.0)) * 18.0))
    momentum += max(-15.0, min(15.0, float(btc.get("rise_15m", 0.0)) * 30.0))
    momentum += max(-15.0, min(15.0, (float(btc.get("rsi15", 50.0)) - 50.0) * 0.75))
    score = health * 0.50 + frame_score * 0.35 + max(0.0, min(100.0, momentum)) * 0.15
    if market.get("btc_flash_crash", False):
        score = min(score, 10.0)
    score = max(0.0, min(100.0, score))
    label = "ممتاز 🟢" if score >= 75 else "جيد 🟢" if score >= 60 else "حذر 🟡" if score >= 40 else "خطر 🟠" if score >= 20 else "خطر جدًا 🔴"
    size = "100%" if score >= 75 else "75%" if score >= 60 else "50%" if score >= 40 else "25%" if score >= 20 else "0–15%"
    return {"score": round(score, 1), "label": label, "suggested_size": size}


def market_permission(market: Dict) -> Dict:
    env = market_environment_score(market)
    health = float(market.get("btc_health_score", 50.0))
    frames = market.get("btc_health_frames", {}) or {}
    btc = market.get("btc", {}) or {}
    flash = bool(market.get("btc_flash_crash", False))
    score = float(env["score"])
    if flash or score < 20:
        risk, permission, allowed, decision = "مرتفع جدًا 🔴", "قوة استثنائية فقط", "A+ مستقلة جدًا", "تجنب الدخول العادي"
    elif score < 40:
        risk, permission, allowed, decision = "مرتفع 🟠", "بحذر شديد", "A+ أو استقلال قوي", "انتظر إلا للفرص الاستثنائية"
    elif score < 60:
        risk, permission, allowed, decision = "متوسط 🟡", "بحذر", "A وA+ بشروط قوية", "انتقِ أفضل الإشارات فقط"
    else:
        risk, permission, allowed, decision = "منخفض 🟢", "مسموح", "A وA+", "الدخول مسموح مع إدارة المخاطر"
    trend = "صاعد" if health >= 65 else "محايد" if health >= 45 else "هابط" if health >= 25 else "هابط بقوة"
    reasons=[]
    if float(frames.get("1h",50)) < 40: reasons.append("اتجاه BTC على 1H ضعيف")
    if float(frames.get("15m",50)) < 40: reasons.append("زخم BTC على 15M ضعيف")
    if float(btc.get("rsi15",50)) < 43: reasons.append("RSI BTC منخفض")
    if float(btc.get("rise_1h",0)) < -0.5: reasons.append("ضغط هبوط خلال الساعة")
    if float(frames.get("5m",50)) >= 70 and float(frames.get("15m",50)) < 40:
        reasons.append("ارتداد 5M لم يتحول بعد إلى اتجاه 15M")
    if not reasons: reasons.append("لا توجد إشارة خطر لحظية قوية")
    return {
        "health":health, "trend":trend, "risk":risk, "permission":permission,
        "allowed":allowed, "decision":decision, "reasons":reasons,
        "environment_score":env["score"], "environment_label":env["label"],
        "suggested_size":env["suggested_size"],
    }

def market_report_message() -> str:
    market = market_context()
    p = market_permission(market)
    btc = market.get("btc", {}) or {}
    frames = market.get("btc_health_frames", {}) or {}
    reasons = "\n".join(f"• {x}" for x in p["reasons"][:4])
    return f"""📊 تقرير السوق الآن

🌍 بيئة السوق
• الدرجة: {p['environment_score']:.1f}/100 — {p['environment_label']}
• حجم الصفقة المقترح: {p['suggested_size']}

🪙 BTC
• الصحة: {p['health']:.1f}/100 — {market.get('btc_health_label','غير متاح')}
• الاتجاه: {p['trend']}
• 4H/1H/15M/5M: {float(frames.get('4h',50)):.0f}/{float(frames.get('1h',50)):.0f}/{float(frames.get('15m',50)):.0f}/{float(frames.get('5m',50)):.0f}
• تغير 1H: {float(btc.get('rise_1h',0)):+.2f}%
• تغير 15M: {float(btc.get('rise_15m',0)):+.2f}%
• RSI 15M: {float(btc.get('rsi15',0)):.1f}

🛡️ قرار التداول
• المخاطرة: {p['risk']}
• السماح بالدخول: {p['permission']}
• الإشارات المسموحة: {p['allowed']}
• القرار النهائي: {p['decision']}

الأسباب:
{reasons}

⚠️ تحليل آلي وليس ضمانًا للربح."""


def debug_report_message(symbol: str) -> str:
    symbol = symbol.upper().strip()
    if symbol and not symbol.endswith("USDT"): symbol += "USDT"
    rows=[]
    try:
        if REJECTION_LOG_FILE.exists():
            with REJECTION_LOCK:
                lines=REJECTION_LOG_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()[-3000:]
            for line in reversed(lines):
                try:
                    row=json.loads(line)
                except Exception:
                    continue
                if not symbol or str(row.get("symbol","")).upper()==symbol:
                    rows.append(row)
                    if len(rows)>=DEBUG_MAX_ROWS: break
    except Exception as exc:
        return f"تعذر قراءة سجل الرفض: {exc}"
    if not rows:
        return f"🔎 لا توجد حالات رفض حديثة مسجلة لـ {symbol or 'الرمز المطلوب'}."
    chunks=[]
    for row in rows:
        details=row.get("details",{}) or {}
        compact=[]
        for k,v in list(details.items())[:8]: compact.append(f"• {k}: {v}")
        chunks.append(f"❌ {row.get('symbol','')} — {row.get('reason','غير معروف')}\n"+"\n".join(compact))
    return "🔎 آخر نتائج التشخيص\n\n"+"\n\n".join(chunks)


def open_trades_message() -> str:
    state = load_state()
    rows = state.get("open_signals", {}) or {}
    if not rows:
        return "📭 لا توجد صفقات مفتوحة يتابعها البوت حاليًا."
    chunks = []
    for symbol, s in list(rows.items())[:12]:
        entry = float(s.get("entry", 0))
        reached = int(s.get("reached", 0))
        chunks.append(
            f"• {symbol}: دخول {fmt(entry)} | آخر هدف TP{reached if reached else 0} | "
            f"النموذج {s.get('mode','غير معروف')}"
        )
    return "📌 الصفقات المفتوحة\n" + "\n".join(chunks)


def performance_message() -> str:
    state = load_state()
    stats = state.get("stats", {}) or {}
    tp1 = int(stats.get("tp1", 0)); tp2 = int(stats.get("tp2", 0))
    tp3 = int(stats.get("tp3", 0)); stop = int(stats.get("stop", 0))
    closed = tp3 + stop
    win_rate = (tp3 / closed * 100.0) if closed else 0.0
    return (
        "📊 إحصائيات المتابعة\n"
        f"• وصل TP1: {tp1}\n"
        f"• وصل TP2: {tp2}\n"
        f"• وصل TP3: {tp3}\n"
        f"• ضرب الوقف: {stop}\n"
        f"• نسبة اكتمال TP3 مقابل الوقف: {win_rate:.1f}%\n"
        "ملاحظة: TP1 وTP2 مراحل وليست صفقات مستقلة."
    )


def _load_command_offset() -> int:
    try: return int(json.loads(COMMAND_OFFSET_FILE.read_text(encoding="utf-8")).get("offset",0))
    except Exception: return 0


def _save_command_offset(offset: int) -> None:
    try: COMMAND_OFFSET_FILE.write_text(json.dumps({"offset":offset}),encoding="utf-8")
    except Exception as exc: log(f"Command offset save failed: {exc}")


def telegram_command_loop() -> None:
    if not COMMANDS_ENABLED: return
    offset=_load_command_offset()
    while True:
        try:
            r=SESSION.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates",params={"offset":offset,"timeout":20,"allowed_updates":["message"]},timeout=25)
            r.raise_for_status()
            for update in r.json().get("result",[]):
                offset=max(offset,int(update.get("update_id",0))+1); _save_command_offset(offset)
                msg=update.get("message",{}) or {}; chat=str((msg.get("chat",{}) or {}).get("id","")); text=str(msg.get("text","")).strip()
                if chat != str(CHAT_ID): continue
                cmd=text.split()[0].split("@")[0] if text else ""
                if cmd in ("/السوق","/market"):
                    send_message(market_report_message())
                elif cmd in ("/debug","/تشخيص"):
                    parts=text.split(maxsplit=1)
                    send_message(debug_report_message(parts[1] if len(parts)>1 else ""))
                elif cmd in ("/صفقات","/trades"):
                    send_message(open_trades_message())
                elif cmd in ("/إحصائيات","/stats"):
                    send_message(performance_message())
                elif cmd in ("/help","/مساعدة","/start"):
                    send_message("الأوامر المتاحة:\n/السوق — حالة السوق وقرار الدخول\n/debug DIA — أسباب الرفض\n/صفقات — الصفقات المفتوحة\n/إحصائيات — نتائج المتابعة")
        except Exception as exc:
            log(f"Telegram command loop: {exc}")
            time.sleep(max(2.0,COMMAND_POLL_SECONDS))


def send_message(text: str) -> bool:
    try:
        r=SESSION.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",json={"chat_id":CHAT_ID,"text":text,"disable_web_page_preview":True},timeout=20)
        r.raise_for_status()
        return True
    except Exception as exc:
        log(f"Telegram send failed: {exc}")
        return False


def get_json(path: str, params: Optional[Dict] = None, timeout: int = 20):
    r = SESSION.get(f"{BINANCE_BASE}{path}", params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_klines(symbol: str, interval: str, limit: int = 260) -> List[Dict]:
    raw = get_json("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    return [{"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),"close":float(x[4]),"volume":float(x[5]),"close_time":int(x[6]),"quote_volume":float(x[7]),"trades":int(x[8])} for x in raw]


def get_symbols() -> List[str]:
    now = time.time()
    with SYMBOL_CACHE_LOCK:
        if SYMBOL_CACHE["symbols"] and now - float(SYMBOL_CACHE["updated_at"]) < SYMBOL_REFRESH_MINUTES * 60:
            return list(SYMBOL_CACHE["symbols"])
    exchange = get_json("/api/v3/exchangeInfo", timeout=30)
    tickers = get_json("/api/v3/ticker/24hr", timeout=30)
    volume_map = {t.get("symbol",""): float(t.get("quoteVolume",0) or 0) for t in tickers}
    rows = []
    for item in exchange.get("symbols", []):
        symbol, base, quote = item.get("symbol",""), item.get("baseAsset","").upper(), item.get("quoteAsset","").upper()
        if item.get("status") != "TRADING" or quote != "USDT" or not item.get("isSpotTradingAllowed", True): continue
        if base in STABLE_BASES or base in EXCLUDED_MAJORS or base.endswith(LEVERAGED_SUFFIXES): continue
        qv = volume_map.get(symbol, 0.0)
        if qv >= MIN_DAILY_QUOTE_VOLUME: rows.append((symbol, qv))
    rows.sort(key=lambda x: x[1], reverse=True)
    fresh_symbols = [s for s,_ in rows]
    with SYMBOL_CACHE_LOCK:
        SYMBOL_CACHE["symbols"] = fresh_symbols
        SYMBOL_CACHE["updated_at"] = now
        return list(SYMBOL_CACHE["symbols"])


def ema(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period: return [None] * len(values)
    out = [None] * (period - 1)
    cur = mean(values[:period]); out.append(cur); k = 2 / (period + 1)
    for value in values[period:]: cur = (value - cur) * k + cur; out.append(cur)
    return out


def rsi(values: List[float], period: int = 14) -> List[Optional[float]]:
    if len(values) <= period: return [None] * len(values)
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
    ag, al = mean(gains[:period]), mean(losses[:period]); out = [None] * period
    calc = lambda g,l: 100.0 if l == 0 else 100 - 100/(1+g/l)
    out.append(calc(ag,al))
    for i in range(period, len(gains)):
        ag = ((ag*(period-1))+gains[i])/period; al = ((al*(period-1))+losses[i])/period; out.append(calc(ag,al))
    return out


def macd(values: List[float]):
    if len(values) < 35: return None,None,None
    f,s = ema(values,12), ema(values,26)
    series = [a-b for a,b in zip(f,s) if a is not None and b is not None]
    if len(series) < 9: return None,None,None
    sig = ema(series,9)[-1]
    if sig is None: return None,None,None
    return series[-1], sig, series[-1]-sig


def atr(candles: List[Dict], period: int = 14) -> float:
    tr=[]
    for i,c in enumerate(candles):
        tr.append(c["high"]-c["low"] if i == 0 else max(c["high"]-c["low"], abs(c["high"]-candles[i-1]["close"]), abs(c["low"]-candles[i-1]["close"])))
    return mean(tr[-period:]) if tr else 0.0


def adx(candles: List[Dict], period: int = 14) -> float:
    if len(candles) < period + 2: return 0.0
    trs,pdm,mdm=[],[],[]
    for i in range(1,len(candles)):
        c,p=candles[i],candles[i-1]; up=c["high"]-p["high"]; down=p["low"]-c["low"]
        pdm.append(up if up>down and up>0 else 0); mdm.append(down if down>up and down>0 else 0)
        trs.append(max(c["high"]-c["low"],abs(c["high"]-p["close"]),abs(c["low"]-p["close"])))
    dx=[]
    for end in range(period,len(trs)+1):
        ts=sum(trs[end-period:end])
        if ts<=0: continue
        pdi=100*sum(pdm[end-period:end])/ts; mdi=100*sum(mdm[end-period:end])/ts
        if pdi+mdi>0: dx.append(100*abs(pdi-mdi)/(pdi+mdi))
    return mean(dx[-period:]) if dx else 0.0


def vwap(candles: List[Dict], period: int = 20) -> float:
    w=candles[-period:]; tv=sum(c["volume"] for c in w)
    return w[-1]["close"] if tv<=0 else sum(((c["high"]+c["low"]+c["close"])/3)*c["volume"] for c in w)/tv


def obv(candles: List[Dict]) -> List[float]:
    out=[0.0]
    for i in range(1,len(candles)):
        out.append(out[-1]+candles[i]["volume"] if candles[i]["close"]>candles[i-1]["close"] else out[-1]-candles[i]["volume"] if candles[i]["close"]<candles[i-1]["close"] else out[-1])
    return out


def bollinger(values: List[float], period: int = 20, mult: float = 2.0):
    w=values[-period:]; mid=mean(w); sd=pstdev(w); up=mid+mult*sd; low=mid-mult*sd
    return mid,up,low,(up-low)/mid*100 if mid else 0


def pct_change(old: float, new: float) -> float: return ((new/old)-1)*100 if old else 0.0


def consecutive_green(candles: List[Dict], lookback: int = 6) -> int:
    n=0
    for c in reversed(candles[-lookback:]):
        if c["close"]>c["open"]: n+=1
        else: break
    return n


def accumulation_base(candles: List[Dict], lookback: int = ACCUMULATION_LOOKBACK_15M) -> Optional[Dict]:
    """يكشف قاعدة تجميع مضغوطة مع قيعان صاعدة وبناء تدريجي للحجم قبل الانطلاقة."""
    if len(candles) < lookback + 6:
        return None
    base = candles[-lookback-1:-1]
    if len(base) < lookback:
        return None
    highs = [float(c["high"]) for c in base]
    lows = [float(c["low"]) for c in base]
    opens = [float(c["open"]) for c in base]
    closes = [float(c["close"]) for c in base]
    vols = [float(c["volume"]) for c in base]
    base_low, base_high = min(lows), max(highs)
    range_pct = pct_change(base_low, base_high) if base_low > 0 else 999.0
    base_atr = atr(base)
    avg_body = mean(abs(c-o) for o,c in zip(opens, closes))
    avg_body_atr = avg_body / base_atr if base_atr > 0 else 999.0
    half = max(3, lookback // 2)
    first_low = min(lows[:half])
    second_low = min(lows[half:])
    higher_low_pct = pct_change(first_low, second_low) if first_low > 0 else -999.0
    prior_vol = mean(vols[:max(4, lookback-5)])
    recent_vol = mean(vols[-5:])
    volume_build = recent_vol / prior_vol if prior_vol > 0 else 0.0
    resistance = max(highs[-8:])
    return {
        "range_pct": range_pct,
        "avg_body_atr": avg_body_atr,
        "higher_low_pct": higher_low_pct,
        "volume_build": volume_build,
        "base_low": base_low,
        "resistance": resistance,
        "compressed": range_pct <= ACCUMULATION_MAX_BASE_RANGE_PCT and avg_body_atr <= ACCUMULATION_MAX_AVG_BODY_ATR,
        "higher_lows": higher_low_pct >= ACCUMULATION_MIN_HIGHER_LOW_PCT,
        "building_volume": volume_build >= ACCUMULATION_MIN_VOLUME_BUILD,
    }


def trend_ignition_structure(candles: List[Dict]) -> Optional[Dict]:
    """يكشف قاعدة طويلة مضغوطة، رف مقاومة متكرر، قيعان صاعدة، وبناء حجم قبل الانطلاقة."""
    closed = candles[:-1] if candles else []
    if len(closed) < TI_STRUCTURE_MIN_BARS + 3:
        return None

    lookback = min(TI_STRUCTURE_LOOKBACK, len(closed) - 1)
    base = closed[-lookback-1:-1]
    trigger = closed[-1]
    if len(base) < TI_STRUCTURE_MIN_BARS:
        return None

    highs = [float(c["high"]) for c in base]
    lows = [float(c["low"]) for c in base]
    closes = [float(c["close"]) for c in base]
    vols = [float(c["volume"]) for c in base]

    base_low = min(lows)
    base_high = max(highs)
    range_pct = pct_change(base_low, base_high) if base_low > 0 else 999.0

    split = max(4, len(base) // 2)
    first = base[:split]
    second = base[split:]
    first_range = max(float(c["high"]) for c in first) - min(float(c["low"]) for c in first)
    recent_range = max(float(c["high"]) for c in second) - min(float(c["low"]) for c in second)
    range_ratio = recent_range / first_range if first_range > 0 else 1.0

    first_low = min(float(c["low"]) for c in first)
    second_low = min(float(c["low"]) for c in second)
    higher_low_pct = pct_change(first_low, second_low) if first_low > 0 else -999.0

    # رف مقاومة: عدة قمم متقاربة قرب أعلى القاعدة، وليس ذيلًا واحدًا.
    shelf_seed = sorted(highs)[max(0, int(len(highs) * 0.70)):]
    shelf_level = mean(shelf_seed) if shelf_seed else base_high
    shelf_touches = sum(
        1 for h in highs
        if abs(h / shelf_level - 1) * 100 <= TI_STRUCTURE_SHELF_TOLERANCE_PCT
    )

    old_vol = mean(vols[:max(5, len(vols)//2)])
    recent_vol = mean(vols[-6:])
    volume_build = recent_vol / old_vol if old_vol > 0 else 0.0
    base_avg_vol = mean(vols[-20:]) if len(vols) >= 20 else mean(vols)
    breakout_volume = float(trigger["volume"]) / base_avg_vol if base_avg_vol > 0 else 0.0

    trigger_close = float(trigger["close"])
    extension_pct = pct_change(shelf_level, trigger_close) if shelf_level > 0 else 999.0
    trigger_close_location = close_location(trigger)
    trigger_green = trigger_close > float(trigger["open"])

    compressed = range_pct <= TI_STRUCTURE_MAX_RANGE_PCT
    contracting = range_ratio <= TI_STRUCTURE_MAX_RECENT_RANGE_RATIO
    higher_lows = higher_low_pct >= TI_STRUCTURE_MIN_HIGHER_LOW_PCT
    shelf_ready = shelf_touches >= TI_STRUCTURE_MIN_SHELF_TOUCHES
    volume_ready = volume_build >= TI_STRUCTURE_MIN_VOLUME_BUILD
    breakout_ready = (
        trigger_green
        and trigger_close >= shelf_level * 0.995
        and breakout_volume >= TI_STRUCTURE_MIN_BREAKOUT_VOLUME
        and extension_pct <= TI_STRUCTURE_MAX_BREAKOUT_EXTENSION_PCT
        and trigger_close_location >= TI_STRUCTURE_MIN_CLOSE_LOCATION
    )

    score = 0.0
    score += 18 if compressed else 0
    score += 16 if contracting else 0
    score += 16 if higher_lows else 0
    score += min(18.0, shelf_touches * 4.5)
    score += min(14.0, max(0.0, (volume_build - 1.0) * 35.0))
    score += min(18.0, max(0.0, (breakout_volume - 1.0) * 18.0))
    score += 8 if breakout_ready else 0
    score = min(100.0, score)

    return {
        "score": round(score, 1),
        "range_pct": range_pct,
        "range_ratio": range_ratio,
        "higher_low_pct": higher_low_pct,
        "shelf_level": shelf_level,
        "shelf_touches": shelf_touches,
        "volume_build": volume_build,
        "breakout_volume": breakout_volume,
        "extension_pct": extension_pct,
        "close_location": trigger_close_location,
        "base_low": base_low,
        "compressed": compressed,
        "contracting": contracting,
        "higher_lows": higher_lows,
        "shelf_ready": shelf_ready,
        "volume_ready": volume_ready,
        "breakout_ready": breakout_ready,
        "valid": bool(
            compressed and contracting and higher_lows and shelf_ready
            and volume_ready and breakout_ready and score >= TI_STRUCTURE_MIN_SCORE
        ),
    }


def adaptive_frame_snapshot(candles: List[Dict], min_bars: int = 35) -> Optional[Dict]:
    """لقطة اتجاه مرنة تدعم العملات الجديدة ولا تشترط EMA200."""
    closed = candles[:-1]
    if len(closed) < min_bars:
        return None
    closes = [c["close"] for c in closed]
    e20v = ema(closes, 20)
    e50v = ema(closes, 50) if len(closes) >= 50 else [None] * len(closes)
    e200v = ema(closes, 200) if len(closes) >= 200 else [None] * len(closes)
    e20 = e20v[-1]
    e50 = e50v[-1] if e50v else None
    e200 = e200v[-1] if e200v else None
    rr = rsi(closes)[-1]
    _, _, hist = macd(closes)
    if e20 is None or rr is None or hist is None:
        return None
    price = closes[-1]
    strongly_bearish = (
        price < e20
        and (e50 is None or e20 < e50)
        and (e200 is None or price < e200)
        and hist < 0
        and e20 <= e20v[-4]
    )
    return {
        "price": price, "e20": e20, "e50": e50, "e200": e200,
        "rsi": rr, "macd_hist": hist, "adx": adx(closed),
        "not_bearish": not strongly_bearish,
        "strongly_bearish": strongly_bearish,
        "bars": len(closed),
    }


def frame_snapshot(candles: List[Dict]) -> Optional[Dict]:
    return adaptive_frame_snapshot(candles, 210)


def compact_frame_snapshot(candles: List[Dict]) -> Optional[Dict]:
    return adaptive_frame_snapshot(candles, 60)


def frame_trend_score(snapshot: Dict) -> float:
    """درجة اتجاه 0..100 مع تجاهل EMA غير المتوفر بدل معاقبة العملة الجديدة."""
    price = float(snapshot["price"])
    e20 = float(snapshot["e20"])
    e50 = snapshot.get("e50")
    e200 = snapshot.get("e200")
    r = float(snapshot["rsi"])
    hist = float(snapshot["macd_hist"])

    score = 50.0
    score += 18 if price > e20 else -18
    if e50 is not None:
        score += 14 if e20 > float(e50) else -14
    if e200 is not None:
        score += 10 if price > float(e200) else -10
    score += 8 if hist > 0 else -8
    if 52 <= r <= 68:
        score += 8
    elif r < 42:
        score -= 8
    return max(0.0, min(100.0, score))


def multi_timeframe_alignment(frames: Dict[str, Optional[Dict]], base_weights: Optional[Dict[str, float]] = None) -> Dict:
    """يعيد توزيع الأوزان تلقائيًا على الفريمات المتوفرة فقط."""
    available = {k: v for k, v in frames.items() if v is not None}
    if not available:
        return {"score": 50.0, "adjustment": 0, "label": "غير متاح", "frames": {}, "weights": {}}

    source_weights = base_weights or MTF_WEIGHTS
    raw_weights = {k: source_weights[k] for k in available if k in source_weights}
    total = sum(raw_weights.values())
    weights = {k: raw_weights[k] * 100.0 / total for k in raw_weights}
    scores = {k: frame_trend_score(v) for k, v in available.items()}
    weighted = sum(scores[k] * weights[k] for k in scores) / 100.0
    if weighted >= 72:
        adjustment = MTF_STRONG_BONUS
    elif weighted >= 60:
        adjustment = MTF_GOOD_BONUS
    elif weighted >= 45:
        adjustment = 0
    else:
        adjustment = MTF_WEAK_PENALTY
    label = "متوافق بقوة" if weighted >= 72 else "متوافق" if weighted >= 60 else "محايد" if weighted >= 45 else "ضعيف" if weighted >= 32 else "هابط بقوة"
    return {
        "score": round(weighted, 1),
        "adjustment": int(adjustment),
        "label": label,
        "frames": {k: round(v, 1) for k, v in scores.items()},
        "weights": {k: round(v, 1) for k, v in weights.items()},
    }


def detect_launch_mode(
    mtf_frames: Dict[str, float],
    volume_ratio_15m: float,
    adx_15m: float,
    rsi_15m: float,
    breakout: bool,
    squeeze_break: bool,
    early_break: bool,
) -> bool:
    """يكتشف بداية انطلاقة حقيقية دون تجاوز فلاتر السوق أو المطاردة."""
    if not LAUNCH_MODE_ENABLED:
        return False
    return bool(
        mtf_frames.get("15m", 0) >= LAUNCH_MIN_15M_SCORE
        and mtf_frames.get("5m", 0) >= LAUNCH_MIN_5M_SCORE
        and mtf_frames.get("1h", 0) >= LAUNCH_MIN_1H_SCORE
        and volume_ratio_15m >= LAUNCH_MIN_VOLUME_15M
        and adx_15m >= LAUNCH_MIN_ADX_15M
        and LAUNCH_MIN_RSI_15M <= rsi_15m <= LAUNCH_MAX_RSI_15M
        and (breakout or squeeze_break or early_break)
    )


def signal_quality(
    score: int,
    mtf_score: float,
    mode: str,
    launch_mode: bool,
    volume_ratio_15m: float,
    mtf_frames: Optional[Dict[str, float]] = None,
) -> Tuple[str, str]:
    """يعيد تصنيف الجودة مع قيود خاصة للحجم وقوة الفريمات العليا."""
    if mode in ("trend_ignition", "surge_continuation"):
        # مسار الاندفاع يبدأ A؛ يرتقي إلى A+ فقط عند سكور استثنائي.
        if mode == "surge_continuation" and score >= 97 and mtf_score >= 60 and volume_ratio_15m >= 1.5:
            return "A+", "⭐⭐⭐⭐⭐"
        return "A", "⭐⭐⭐⭐"

    quality_points = int(score)
    if mtf_score >= 72:
        quality_points += 2
    if launch_mode:
        quality_points += 2
    if mode in ("momentum", "accumulation", "pullback"):
        quality_points += 1

    if quality_points >= 97:
        quality, stars = "A+", "⭐⭐⭐⭐⭐"
    else:
        quality, stars = "A", "⭐⭐⭐⭐"

    if mode == "pullback":
        if volume_ratio_15m < PULLBACK_A_MIN_VOLUME_15M:
            return "A", "⭐⭐⭐⭐"
        if volume_ratio_15m < PULLBACK_A_PLUS_MIN_VOLUME_15M and quality == "A+":
            return "A", "⭐⭐⭐⭐"

    # سياسة المستخدم تبقي A/A+ فقط؛ ضعف الفريمات العليا يمنع A+ ويثبت الجودة عند A.
    if mode == "reversal":
        frames = mtf_frames or {}
        score_4h = float(frames.get("4h", 0))
        score_1h = float(frames.get("1h", 0))
        reversal_grade_ready = (
            mtf_score >= REVERSAL_A_MIN_MTF_SCORE
            and score_4h >= REVERSAL_A_MIN_4H_SCORE
            and score_1h >= REVERSAL_A_MIN_1H_SCORE
        )
        if not reversal_grade_ready and quality in ("A+", "A"):
            return "A", "⭐⭐⭐⭐"

    return quality, stars


def market_context() -> Dict:
    """سياق السوق V19: صحة BTC على ستة فريمات مع حماية الهبوط اللحظي."""
    now = time.time()
    with MARKET_CACHE_LOCK:
        if MARKET_CACHE["data"] and now - float(MARKET_CACHE["updated_at"]) < MARKET_CACHE_SECONDS:
            return dict(MARKET_CACHE["data"])

    try:
        def short_snapshot(symbol: str) -> Dict:
            c5 = get_klines(symbol, "5m", 120)[:-1]
            c15 = get_klines(symbol, "15m", 120)[:-1]
            c1 = get_klines(symbol, "1h", 120)[:-1]
            c4 = get_klines(symbol, "4h", 120)[:-1]
            z = [c["close"] for c in c5]
            a = [c["close"] for c in c15]
            b = [c["close"] for c in c1]
            d = [c["close"] for c in c4]
            # V27: لا نقارن None بأرقام. هذا مهم إذا أعادت المنصة بيانات ناقصة
            # أو تم استخدام الدالة لاحقًا مع زوج حديث قليل الشموع.
            series_by_frame = {"5m": z, "15m": a, "1h": b, "4h": d}
            for frame, values in series_by_frame.items():
                if len(values) < 50:
                    raise ValueError(f"{symbol} {frame}: insufficient closed candles ({len(values)})")

            e20z, e50z = ema(z, 20)[-1], ema(z, 50)[-1]
            e20a, e50a = ema(a, 20)[-1], ema(a, 50)[-1]
            e20b, e50b = ema(b, 20)[-1], ema(b, 50)[-1]
            e20d, e50d = ema(d, 20)[-1], ema(d, 50)[-1]
            _, _, hz = macd(z); _, _, ha = macd(a)
            _, _, hb = macd(b); _, _, hd = macd(d)
            rz, ra, rb, rd = rsi(z)[-1], rsi(a)[-1], rsi(b)[-1], rsi(d)[-1]

            required = {
                "e20z": e20z, "e50z": e50z, "e20a": e20a, "e50a": e50a,
                "e20b": e20b, "e50b": e50b, "e20d": e20d, "e50d": e50d,
                "hz": hz, "ha": ha, "hb": hb, "hd": hd,
                "rz": rz, "ra": ra, "rb": rb, "rd": rd,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise ValueError(f"{symbol}: incomplete indicators: {', '.join(missing)}")

            return {
                "rise_5m": pct_change(z[-2], z[-1]),
                "rise_15m": pct_change(z[-4], z[-1]),
                "rise_1h": pct_change(a[-5], a[-1]),
                "rise_4h": pct_change(b[-5], b[-1]),
                "rsi5": rz, "rsi15": ra, "rsi1h": rb, "rsi4h": rd,
                "bull5": z[-1] > e20z > e50z and hz >= 0,
                "bear5": z[-1] < e20z and hz < 0,
                "bull15": a[-1] > e20a > e50a and ha >= 0,
                "bear15": a[-1] < e20a and ha < 0,
                "bull1h": b[-1] > e20b > e50b and hb >= 0,
                "bear1h": b[-1] < e20b and hb < 0,
                "bull4h": d[-1] > e20d > e50d and hd >= 0,
                "bear4h": d[-1] < e20d < e50d and hd < 0,
            }

        btc = short_snapshot("BTCUSDT")
        eth = short_snapshot("ETHUSDT")

        # تحليل مستقل لصحة BTC عبر جميع الفريمات.
        btc_health_frames = {}
        if BTC_HEALTH_ENABLED:
            btc_raw = {
                "5m": get_klines("BTCUSDT", "5m", 120),
                "15m": get_klines("BTCUSDT", "15m", 120),
                "1h": get_klines("BTCUSDT", "1h", 220),
                "4h": get_klines("BTCUSDT", "4h", 220),
                "1d": get_klines("BTCUSDT", "1d", 220),
                "1w": get_klines("BTCUSDT", "1w", 220),
            }
            btc_health_input = {
                "1w": adaptive_frame_snapshot(btc_raw["1w"], 35),
                "1d": adaptive_frame_snapshot(btc_raw["1d"], 50),
                "4h": adaptive_frame_snapshot(btc_raw["4h"], 120),
                "1h": adaptive_frame_snapshot(btc_raw["1h"], 120),
                "15m": adaptive_frame_snapshot(btc_raw["15m"], 60),
                "5m": adaptive_frame_snapshot(btc_raw["5m"], 60),
            }
            btc_health = multi_timeframe_alignment(btc_health_input, BTC_HEALTH_WEIGHTS)
            btc_health_score = float(btc_health["score"])
            btc_health_frames = dict(btc_health.get("frames", {}))
            btc_health_label = btc_health.get("label", "غير متاح")
        else:
            btc_health_score = 50.0
            btc_health_label = "معطل"

        points = (
            (2 if btc["bull4h"] else -2 if btc["bear4h"] else 0)
            + (2 if btc["bull1h"] else -2 if btc["bear1h"] else 0)
            + (1 if btc["bull15"] else -1 if btc["bear15"] else 0)
            + (1 if eth["bull1h"] else -1 if eth["bear1h"] else 0)
            + (1 if eth["bull15"] else -1 if eth["bear15"] else 0)
        )
        sudden_drop = (
            btc["rise_5m"] <= -BTC_5M_DROP_BLOCK_PCT
            and btc["bear5"]
            and btc["rsi15"] < BTC_15M_RSI_BLOCK
        )
        trend_break = (
            btc["rise_1h"] <= -BTC_1H_DROP_BLOCK_PCT
            and btc["bear15"]
            and btc["bear1h"]
        )
        weak_pressure = bool(
            SMART_MARKET_FILTER
            and (
                btc_health_score < BTC_HEALTH_WEAK_SCORE
                or (
                    btc["rsi15"] < BTC_WEAK_RSI_15M
                    and (btc["rsi5"] < BTC_WEAK_RSI_5M or btc["rise_15m"] <= -BTC_WEAK_15M_DROP_PCT)
                    and (btc["bear5"] or btc["bear15"])
                )
            )
        )
        severe = bool(
            btc["rise_1h"] <= -1.4
            or btc["rise_4h"] <= -3
            or sudden_drop
            or trend_break
            or (BTC_HEALTH_ENABLED and btc_health_score < BTC_HEALTH_HARD_BLOCK_SCORE)
        )
        hard_block = bool(
            SMART_MARKET_FILTER
            and (
                sudden_drop
                or trend_break
                or (btc["bear4h"] and btc["bear1h"] and btc["bear15"])
                or (BTC_HEALTH_ENABLED and btc_health_score < BTC_HEALTH_HARD_BLOCK_SCORE)
            )
        )
        regime = (
            "ضعيف جدًا" if severe or points <= -4
            else "ضعيف" if points <= -2 or weak_pressure
            else "إيجابي" if points >= 3 and btc_health_score >= BTC_HEALTH_WEAK_SCORE
            else "محايد"
        )
        bonus = 12 if regime == "ضعيف جدًا" else 7 if regime == "ضعيف" else 0 if regime == "إيجابي" else 3
        if BTC_HEALTH_ENABLED and BTC_HEALTH_HARD_BLOCK_SCORE <= btc_health_score < BTC_HEALTH_WEAK_SCORE:
            bonus += BTC_HEALTH_WEAK_SCORE_BONUS

        data = {
            "regime": regime,
            "btc": btc,
            "eth": eth,
            "btc_health_score": round(btc_health_score, 1),
            "btc_health_label": btc_health_label,
            "btc_health_frames": btc_health_frames,
            "required_score": MIN_SCORE + bonus,
            "severe_drop": severe,
            "hard_block": hard_block,
            "weak_pressure": weak_pressure,
        }
    except Exception as exc:
        log(f"Market filter error: {exc}")
        data = {
            "regime": "غير متاح",
            "btc": {"rise_1h": 0, "rise_4h": 0, "rise_15m": 0, "rise_5m": 0, "rsi5": 50, "rsi15": 50},
            "eth": {"rise_1h": 0, "rise_4h": 0},
            "btc_health_score": 50.0,
            "btc_health_label": "غير متاح",
            "btc_health_frames": {},
            "required_score": MIN_SCORE + 3,
            "severe_drop": False,
            "hard_block": False,
            "weak_pressure": False,
        }
    with MARKET_CACHE_LOCK:
        MARKET_CACHE["data"], MARKET_CACHE["updated_at"] = data, now
    return dict(data)


def prefilter_symbol(symbol: str) -> Optional[Tuple[str,float]]:
    try:
        c=get_klines(symbol,"5m",80)[:-1]
        if len(c)<55: return None
        closes=[x["close"] for x in c]; vols=[x["volume"] for x in c]
        av=mean(vols[-21:-1]); vr=vols[-1]/av if av else 0; e20=ema(closes,20)[-1]
        resistance=max(x["high"] for x in c[-21:-1]); proximity=closes[-1]/resistance if resistance else 0
        short_rise = pct_change(closes[-7], closes[-1])
        score=vr*34+max(short_rise,0)*5+max(proximity-0.965,0)*230
        # إبقاء العملات ذات الحجم الاستثنائي والحركة المستقلة ضمن القائمة المختصرة.
        if EXCEPTIONAL_STRENGTH_ENABLED and vr >= 2.0 and short_rise >= 1.5:
            score += 30
        # V31: لا تسقط اندفاعات الفريم القصير من قائمة الـ60 قبل التحليل الكامل.
        if SURGE_CONTINUATION_ENABLED and short_rise >= 1.0 and vr >= 1.10 and proximity >= 0.985:
            score += 38
        # لا نهمل العملات التي بدأت ارتدادًا من هبوط واضح.
        dd=recent_drawdown_pct(c, 24)
        rv=rsi(closes)
        if REVERSAL_ENABLED and dd>=REVERSAL_MIN_DRAWDOWN_PCT and rv[-1] and rv[-4] and rv[-1]>rv[-4]: score+=35
        # لا نهمل القواعد الهادئة التي تبني حجمًا وقيعانًا صاعدة قبل الاختراق.
        if ACCUMULATION_ENABLED:
            base = accumulation_base(c, min(16, len(c)-6))
            if base and base["compressed"] and base["higher_lows"]:
                score += 24
                if base["building_volume"]: score += 14
                if closes[-1] >= base["resistance"] * 0.995: score += 12
        # V24: إبقاء القواعد الطويلة المضغوطة ذات رف المقاومة ضمن القائمة المختصرة.
        if TREND_IGNITION_ENABLED:
            ti5 = trend_ignition_structure(c + [{"open":closes[-1],"high":closes[-1],"low":closes[-1],"close":closes[-1],"volume":0.0,"close_time":0,"quote_volume":0.0,"trades":0}])
            if ti5:
                score += ti5["score"] * 0.35
                if ti5["shelf_ready"]: score += 12
                if ti5["contracting"]: score += 10

        # لا نهمل التصحيح الهادئ قرب EMA20 إذا بدأ ارتداد 5m.
        if PULLBACK_ENABLED and e20 and e20 * 0.985 <= closes[-1] <= e20 * 1.012:
            if rv[-1] is not None and rv[-2] is not None and rv[-1] >= rv[-2]:
                score += 28
        if e20 and closes[-1]<e20*0.985: score-=20
        return symbol,score
    except Exception as exc:
        log(f"Prefilter {symbol}: {exc}"); return None


def analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    try:
        c5,c15,c1h,c4h=[get_klines(symbol,x,260) for x in ("5m","15m","1h","4h")]
        c1d=get_klines(symbol,"1d",220)
        c1w=get_klines(symbol,"1w",220)
        closed5,closed15=c5[:-1],c15[:-1]
        live5,live15=c5[-1],c15[-1]
        if min(len(closed5),len(closed15),len(c1h)-1,len(c4h)-1)<210:
            return None
        closes5=[c["close"] for c in closed5]; vols5=[c["volume"] for c in closed5]; price=closes5[-1]; candle5=closed5[-1]
        e20=ema(closes5,20)[-1]; r5v=rsi(closes5); r5,r5p=r5v[-1],r5v[-2]; m5,s5,h5=macd(closes5)
        if None in (e20,r5,r5p,m5,s5,h5): return None
        atr5=atr(closed5); vw5=vwap(closed5); obv5=obv(closed5); av5=mean(vols5[-21:-1]); vr5=vols5[-1]/av5 if av5 else 0
        dist=abs(price/e20-1)*100 if e20 else 999; rise15=pct_change(closed5[-4]["close"],price); rise1=pct_change(closed5[-13]["close"],price); rise4=pct_change(closed5[-49]["close"],price)
        body=abs(candle5["close"]-candle5["open"])/candle5["open"]*100 if candle5["open"] else 0; greens=consecutive_green(closed5)
        if not (price>e20 and price>vw5 and h5>=0): return None

        closes15=[c["close"] for c in closed15]; vols15=[c["volume"] for c in closed15]; trades15=[c["trades"] for c in closed15]; candle15,prev15=closed15[-1],closed15[-2]
        e20v,e50v=ema(closes15,20),ema(closes15,50); e20_15,e50_15=e20v[-1],e50v[-1]; r15v=rsi(closes15); r15=r15v[-1]; m15,s15,h15=macd(closes15); a15=adx(closed15)
        if None in (e20_15,e50_15,r15,m15,s15,h15): return None
        av15=mean(vols15[-21:-1]); vr15=vols15[-1]/av15 if av15 else 0; at15=mean(trades15[-21:-1]); tr15=trades15[-1]/at15 if at15 else 0
        live_vr15=projected_volume_ratio(live15, av15, 15*60)
        live_vr5=projected_volume_ratio(live5, av5, 5*60)
        live_price=float(live5["close"] or price)
        resistance=max(c["high"] for c in closed15[-22:-2]); support=min(c["low"] for c in closed15[-12:-1])
        atr15=atr(closed15)
        breakout_range=candle15["high"]-candle15["low"]
        breakout_body=abs(candle15["close"]-candle15["open"])
        breakout=candle15["close"]>resistance and candle15["close"]>candle15["open"] and vr15>=MIN_15M_VOLUME_RATIO
        retest=prev15["close"]>resistance and candle15["low"]<=resistance*1.004 and candle15["low"]>=resistance*0.990 and candle15["close"]>resistance and candle15["close"]>candle15["open"]
        widths=[bollinger(closes15[:-i])[3] for i in range(40,0,-1) if len(closes15[:-i])>=20]; _,bbup,_,bbw=bollinger(closes15)
        squeeze=bool(widths) and bbw<=sorted(widths)[max(0,int(len(widths)*0.30)-1)]
        squeeze_break=squeeze and candle15["close"]>=bbup*0.995 and vr15>=MIN_15M_VOLUME_RATIO
        s1 = frame_snapshot(c1h)
        s4 = frame_snapshot(c4h)
        s1d = adaptive_frame_snapshot(c1d, 50) if len(c1d) - 1 >= 50 else None
        s1w = adaptive_frame_snapshot(c1w, 35) if len(c1w) - 1 >= 35 else None
        s15 = compact_frame_snapshot(c15)
        s5 = compact_frame_snapshot(c5)
        if not all((s1, s4, s15, s5)):
            return None
        mtf_frames_input = {"1w": s1w, "1d": s1d, "4h": s4, "1h": s1, "15m": s15, "5m": s5}
        mtf = multi_timeframe_alignment(mtf_frames_input) if MTF_ENABLED else {"score":50.0,"adjustment":0,"label":"معطل","frames":{},"weights":{}}

        accumulation_info = accumulation_base(closed15, ACCUMULATION_LOOKBACK_15M) if ACCUMULATION_ENABLED else None
        early_level = float(accumulation_info["resistance"]) if accumulation_info else 0.0
        closed_early_break = bool(accumulation_info and candle15["close"] >= early_level * 0.998 and candle15["close"] > candle15["open"])
        live_break_extension = pct_change(early_level, live_price) if early_level > 0 else 999.0
        live_early_break = bool(
            EARLY_WAVE_ENABLED and accumulation_info and early_level > 0
            and live_price >= early_level * 0.998
            and live15["close"] > live15["open"]
            and close_location(live15) >= EARLY_WAVE_MIN_CLOSE_LOCATION
            and live_vr15 >= EARLY_WAVE_MIN_PROJECTED_VOLUME_15M
            and max(vr5, live_vr5) >= EARLY_WAVE_MIN_VOLUME_5M
            and live_break_extension <= EARLY_WAVE_MAX_BREAKOUT_EXTENSION_PCT
        )
        early_break = closed_early_break or live_early_break

        # V24: Trend Ignition Structure Engine — بنية السعر أولًا، والمؤشرات كفلاتر أمان.
        ti_structure = trend_ignition_structure(c15)
        closes_above_e20 = sum(1 for close, e in zip(closes15[-5:], e20v[-5:]) if e is not None and close > e)
        ema20_flat_or_rising = bool(e20_15 >= e20v[-4] * 0.998)
        structure_ready = bool(ti_structure and (ti_structure["valid"] or (float(ti_structure.get("score", 0)) >= TI_STRUCTURE_MIN_SCORE - 8)))
        shelf_level = float(ti_structure["shelf_level"]) if ti_structure else 0.0
        live_structure_extension = pct_change(shelf_level, live_price) if shelf_level > 0 else 999.0
        live_structure_break = bool(
            EARLY_WAVE_ENABLED and ti_structure and shelf_level > 0
            and live_price >= shelf_level * 0.998
            and live15["close"] > live15["open"]
            and close_location(live15) >= EARLY_WAVE_MIN_CLOSE_LOCATION
            and live_vr15 >= EARLY_WAVE_MIN_PROJECTED_VOLUME_15M
            and max(vr5, live_vr5) >= EARLY_WAVE_MIN_VOLUME_5M
            and live_structure_extension <= EARLY_WAVE_MAX_BREAKOUT_EXTENSION_PCT
        )
        early_structure_break = bool(
            ti_structure and (
                (candle15["close"] >= shelf_level * 0.995 and ti_structure["extension_pct"] <= TI_STRUCTURE_MAX_BREAKOUT_EXTENSION_PCT)
                or live_structure_break
            )
        )

        trend_ignition = bool(
            TREND_IGNITION_ENABLED
            and structure_ready
            and early_structure_break
            and ema20_flat_or_rising
            and closes_above_e20 >= 2
            and (h15 >= 0 or live_structure_break) and (h5 >= 0 or live_vr5 >= EARLY_WAVE_MIN_VOLUME_5M)
            and live_price > vw5
            and rise15 <= max(TREND_IGNITION_MAX_15M_RISE, TI_STRUCTURE_MAX_BREAKOUT_EXTENSION_PCT)
            and rise1 <= TREND_IGNITION_MAX_1H_RISE
            and not s1["strongly_bearish"] and not s4["strongly_bearish"]
            and mtf["score"] >= TREND_IGNITION_MIN_MTF
        )



        launch_mode = detect_launch_mode(
            mtf.get("frames", {}), vr15, a15, r15,
            breakout, squeeze_break, early_break
        )
        if launch_mode and MTF_ENABLED:
            mtf = multi_timeframe_alignment(mtf_frames_input, LAUNCH_MTF_WEIGHTS)

        # V32: القوة النسبية يجب أن تُحسب قبل أي مسار يستخدمها.
        # في V31 كان مسار surge_continuation يقرأ rel قبل تعريفه، ما كان يؤدي
        # إلى Analyze <symbol>: local variable 'rel' referenced before assignment
        # ويمنع جميع المرشحين من الوصول إلى مرحلة الإرسال.
        rel = rise1 - float(market.get("btc", {}).get("rise_1h", 0.0))

        # النماذج المرسلة كثيرًا ما تكون بعد أول شمعة انفجار؛ عندها ينخفض حجم
        # الشمعة الحالية رغم أن آخر عدة شمعات تحمل اندفاعًا واضحًا. لذلك نحتفظ
        # بأعلى نسبة حجم حديثة بدل اشتراط أن تكون الشمعة الأخيرة وحدها ضخمة.
        recent_n = max(2, SURGE_RECENT_VOLUME_LOOKBACK)
        base15 = mean(vols15[-(24 + recent_n):-recent_n]) if len(vols15) >= 24 + recent_n else av15
        base5 = mean(vols5[-(24 + recent_n):-recent_n]) if len(vols5) >= 24 + recent_n else av5
        recent_vr15 = max(vols15[-recent_n:]) / base15 if base15 else vr15
        recent_vr5 = max(vols5[-recent_n:]) / base5 if base5 else vr5

        # V31: حالة BTC المبكرة حتى تستطيع فلاتر RSI/الشمعة التمييز بين
        # انهيار السوق الحقيقي وبين ضعف BTC العادي مع عملة مستقلة قوية.
        btc5_move_early = float(market.get("btc", {}).get("rise_5m", 0.0))
        btc15_move_early = float(market.get("btc", {}).get("rise_15m", 0.0))
        btc1h_move_early = float(market.get("btc", {}).get("rise_1h", 0.0))
        btc_flash_crash_early = bool(
            btc5_move_early <= -BTC_FLASH_CRASH_5M_DROP_PCT
            or btc15_move_early <= -BTC_FLASH_CRASH_15M_DROP_PCT
            or btc1h_move_early <= -BTC_FLASH_CRASH_1H_DROP_PCT
        )

        # V32: مسار اندفاع مستقل معاير على النماذج المرسلة. يقبل حالتين:
        # 1) اختراق القمة الآن، أو 2) استمرار قريب جدًا من القمة بعد انفجار سابق
        # مع بقاء الهيكل والحجم الحديث صاعدين. لا يعتمد على ضعف/قوة BTC إلا
        # في منع Flash Crash الحقيقي.
        surge_reference = max(c["high"] for c in closed15[-18:-2])
        surge_break = candle15["close"] >= surge_reference * 0.995 and candle15["close"] > candle15["open"]
        near_recent_high = candle15["close"] >= surge_reference * (1 - SURGE_NEAR_HIGH_TOLERANCE_PCT / 100)
        rising_closes = sum(
            1 for a, b in zip(closes15[-7:-1], closes15[-6:]) if b > a
        )
        continuation_structure = bool(
            near_recent_high
            and rising_closes >= SURGE_MIN_RISING_CLOSES
            and closes_above_e20 >= 4
            and candle15["close"] >= e20_15
        )
        surge_extension_atr = (candle15["close"] - e20_15) / atr15 if atr15 > 0 else 999.0
        surge_volume_ready = bool(
            (vr15 >= SURGE_MIN_VOLUME_15M and vr5 >= SURGE_MIN_VOLUME_5M)
            or (recent_vr15 >= SURGE_MIN_RECENT_VOLUME_15M and recent_vr5 >= SURGE_MIN_RECENT_VOLUME_5M)
        )
        live_surge_break = bool(
            EARLY_WAVE_ENABLED
            and live_price >= surge_reference * 0.998
            and live15["close"] > live15["open"]
            and close_location(live15) >= EARLY_WAVE_MIN_CLOSE_LOCATION
            and live_vr15 >= EARLY_WAVE_MIN_PROJECTED_VOLUME_15M
            and max(vr5, live_vr5) >= EARLY_WAVE_MIN_VOLUME_5M
            and pct_change(surge_reference, live_price) <= EARLY_WAVE_MAX_BREAKOUT_EXTENSION_PCT
        )
        breakout_extension_pct = pct_change(surge_reference, live_price) if surge_reference > 0 else 999.0
        ema_extension_atr = (live_price - e20_15) / atr15 if atr15 > 0 else 999.0
        chased_move = bool(
            breakout_extension_pct > CHASE_MAX_BREAKOUT_EXTENSION_PCT
            or ema_extension_atr > CHASE_MAX_EMA20_DISTANCE_ATR
            or rise15 > CHASE_MAX_15M_RISE_PCT
            or rise1 > CHASE_MAX_1H_RISE_PCT
            or (r15 > CHASE_MAX_RSI_15M and not retest)
        )
        wave_stage = wave_stage_context(closed15, live_price, e20_15, atr15)
        late_wave = bool(
            LATE_BREAKOUT_FILTER_ENABLED
            and not (live_early_break or live_structure_break)
            and (
                wave_stage["progress"] > LATE_MAX_WAVE_PROGRESS
                or wave_stage["green_15m"] > LATE_MAX_GREEN_15M
                or wave_stage["base_distance_atr"] > LATE_MAX_BASE_DISTANCE_ATR
                or wave_stage["base_distance_pct"] > LATE_MAX_BASE_DISTANCE_PCT
                or wave_stage["rebreak_without_reset"]
                or (wave_stage["blowoff"] and wave_stage["volume_decay"])
            )
        )
        first_leg = first_leg_timing_context(
            closed15, live15, live_price, atr15, max(live_vr15, vr15)
        )
        first_leg_rsi_hot = bool(r15 > FIRST_LEG_MAX_RSI_15M and not retest)
        first_leg_late = bool(
            FIRST_LEG_ONLY_ENABLED
            and first_leg.get("detected", False)
            and (first_leg.get("late", False) or first_leg_rsi_hot)
            and not (live_early_break or live_structure_break or live_surge_break)
        )
        if first_leg_late:
            log_rejection(symbol, "انتهت نافذة أول الموجة — ممنوع مطاردة الاختراق", {
                "age_bars": int(first_leg.get("age_bars", -1)),
                "max_age": FIRST_LEG_MAX_CLOSED_BARS,
                "extension_pct": round(float(first_leg.get("extension_pct", 0)), 2),
                "extension_atr": round(float(first_leg.get("extension_atr", 0)), 2),
                "trigger_range_atr": round(float(first_leg.get("trigger_range_atr", 0)), 2),
                "trigger_body_atr": round(float(first_leg.get("trigger_body_atr", 0)), 2),
                "volume_retention": round(float(first_leg.get("volume_retention", 0)), 2),
                "reset_depth_pct": round(float(first_leg.get("reset_depth_pct", 0)), 2),
                "rsi15": round(float(r15), 2),
                "exhaustion": bool(first_leg.get("exhaustion", False)),
            })
            return None

        surge_continuation = bool(
            SURGE_CONTINUATION_ENABLED
            and (live_surge_break or surge_break or continuation_structure)
            and price > e20 and price > vw5 and candle15["close"] > e20_15
            and e20_15 >= e20v[-4] * 0.995
            and (h15 >= 0 or h5 >= 0 or e20_15 > e20v[-3])
            and a15 >= SURGE_MIN_ADX_15M
            and surge_volume_ready
            and rel >= SURGE_MIN_RELATIVE_STRENGTH
            and mtf["score"] >= SURGE_MIN_MTF_SCORE
            and r15 <= SURGE_MAX_RSI_15M
            and surge_extension_atr <= SURGE_MAX_EXTENSION_ATR
            and (live_surge_break or not chased_move)
            and (not FIRST_LEG_ONLY_ENABLED or not first_leg.get("detected", False) or first_leg.get("age_bars", 99) <= FIRST_LEG_MAX_CLOSED_BARS)
            and not late_wave
            and (close_location(live15) if live_surge_break else close_location(candle15)) >= 0.45
            and not btc_flash_crash_early
        )

        if late_wave:
            log_rejection(symbol, "مرحلة متأخرة من الموجة/إعادة كسر بلا قاعدة جديدة", {
                "wave_progress": round(float(wave_stage["progress"]), 3),
                "green_15m": int(wave_stage["green_15m"]),
                "base_distance_pct": round(float(wave_stage["base_distance_pct"]), 2),
                "base_distance_atr": round(float(wave_stage["base_distance_atr"]), 2),
                "blowoff": bool(wave_stage["blowoff"]),
                "volume_decay": bool(wave_stage["volume_decay"]),
                "rebreak_without_reset": bool(wave_stage["rebreak_without_reset"]),
                "reset_depth_pct": round(float(wave_stage.get("reset_depth_pct", 0)), 2),
            })

        if chased_move and not (retest or live_early_break or live_structure_break or live_surge_break):
            log_rejection(symbol, "مطاردة حركة ممتدة بعد بداية الموجة", {
                "breakout_extension_pct": round(breakout_extension_pct, 2),
                "ema_extension_atr": round(ema_extension_atr, 2),
                "rise15": round(rise15, 2),
                "rise1h": round(rise1, 2),
                "rsi15": round(r15, 2),
            })
            return None

        # حماية من مطاردة شمعة اندفاع: مسار V31 يعالج الاندفاع المؤكد كحالة مستقلة.
        hot_rsi = r15 >= HOT_RSI_15M
        calm_candle = atr15 > 0 and breakout_body <= HOT_RSI_MAX_BODY_ATR * atr15 and close_location(candle15) < 0.80
        if r15 > MAX_RSI_HARD and not (retest or calm_candle or surge_continuation):
            log_rejection(symbol, "RSI مرتفع بلا إعادة اختبار أو تهدئة", {
                "rsi15": round(r15, 2),
                "hard_limit": MAX_RSI_HARD,
                "hot_limit": HOT_RSI_15M,
                "retest": retest,
                "calm_candle": calm_candle,
            })
            return None

        # شمعة اختراق مفرطة غالبًا تكون استنزافًا لا بداية آمنة.
        if breakout and not surge_continuation and atr15 > 0 and (breakout_range > MAX_BREAKOUT_RANGE_ATR * atr15 or breakout_body > MAX_BREAKOUT_BODY_ATR * atr15):
            log_rejection(symbol, "شمعة اختراق استنزافية", {
                "range_atr": round(breakout_range / atr15, 2),
                "body_atr": round(breakout_body / atr15, 2),
            })
            return None

        # V30: تأكيد 5m مرن للاختراق.
        # الشمعة الحمراء الصغيرة فوق المقاومة قد تكون إعادة اختبار سليمة، وليست فشلًا.
        # القوة النسبية حُسبت مبكرًا قبل مسارات الاندفاع والتأكيد.
        if BREAKOUT_CONFIRM_5M and breakout:
            confirm_after_breakout = candle5["close_time"] >= candle15["close_time"]
            held_level = (
                candle5["close"] > resistance
                and candle5["low"] >= resistance * (1 - CONFIRM_MAX_DIP_PCT / 100)
            )
            close_loc_5m = close_location(candle5)
            healthy_close = candle5["close"] >= candle5["open"] and close_loc_5m >= MIN_CONFIRM_CLOSE_LOCATION
            normal_confirmation = held_level and healthy_close and h5 >= 0 and confirm_after_breakout

            hist_floor = -abs(h15) * TI_CONFIRM_MAX_NEGATIVE_HIST_RATIO
            ignition_confirmation = bool(
                TI_CONFIRM_ALLOW_FLAT_MACD
                and trend_ignition
                and held_level and healthy_close
                and h5 >= hist_floor
            )

            # إعادة اختبار هادئة: يسمح بشمعة حمراء صغيرة ما دام السعر حافظ على المقاومة،
            # الإغلاق ليس قرب القاع، والزخم ليس منكسرًا بوضوح، مع حجم أو قوة نسبية داعمة.
            candle_body_pct_5m = abs(candle5["close"] - candle5["open"]) / candle5["open"] * 100 if candle5["open"] else 999.0
            retest_confirmation = bool(
                confirm_after_breakout
                and held_level
                and close_loc_5m >= 0.30
                and candle_body_pct_5m <= 1.20
                and h5 >= hist_floor
                and (vr5 >= 1.0 or rel >= 1.5)
            )

            surge_confirmation = bool(
                surge_continuation
                and held_level
                and close_loc_5m >= 0.25
                and h5 >= hist_floor
            )
            confirmation_valid = normal_confirmation or ignition_confirmation or retest_confirmation or surge_confirmation
            if not confirmation_valid:
                log_rejection(symbol, "فشل تأكيد الاختراق 5m", {
                    "after_breakout": confirm_after_breakout,
                    "trend_ignition_override": ignition_confirmation,
                    "retest_override": retest_confirmation,
                    "surge_override": surge_confirmation,
                    "held_level": held_level,
                    "healthy_close": healthy_close,
                    "close_location": round(float(close_loc_5m), 3),
                    "body_pct": round(float(candle_body_pct_5m), 3),
                    "h5": round(float(h5), 8),
                    "hist_floor": round(float(hist_floor), 8),
                    "relative_strength": round(float(rel), 2),
                    "volume5": round(float(vr5), 2),
                    "confirm_close": candle5["close"],
                    "resistance": resistance,
                })
                return None

        drawdown15=recent_drawdown_pct(closed15, 18)
        recent_rsi_low=min(x for x in r15v[-10:] if x is not None)
        structure_shift=higher_low_structure(closed5, 18)
        reclaimed_15=candle15["close"]>candle15["open"] and candle15["close"]>=prev15["high"]*0.997 and candle15["close"]>e20_15

        # قوة استثنائية: العملة تتفوق بوضوح على BTC مع حجم واختراق حقيقي.
        # لا تتجاوز الإيقاف الكامل، ولا تسمح بمطاردة RSI أو شمعة استنزافية.
        exceptional_strength = bool(
            EXCEPTIONAL_STRENGTH_ENABLED
            and rel >= EXCEPTIONAL_MIN_RELATIVE_STRENGTH
            and mtf["score"] >= EXCEPTIONAL_MIN_MTF_SCORE
            and float(mtf.get("frames", {}).get("15m", 0)) >= EXCEPTIONAL_MIN_15M_SCORE
            and vr15 >= EXCEPTIONAL_MIN_VOLUME_15M
            and vr5 >= EXCEPTIONAL_MIN_VOLUME_5M
            and a15 >= EXCEPTIONAL_MIN_ADX_15M
            and r15 <= EXCEPTIONAL_MAX_RSI_15M
            and h15 > 0 and h5 >= 0
            and price > e20 and price > vw5
            and (breakout or squeeze_break or early_break)
            and not late_wave
        )

        _, _, previous_h15 = macd(closes15[:-1])
        macd_turn = previous_h15 is not None and h15 > previous_h15

        # V29: ارتداد قوي مبكر مستقل. لا يشترط اكتمال الاتجاه على الفريمات العليا،
        # لكنه يشترط استعادة بنية وحجم وقوة نسبية واضحة، ولا يتجاوز Flash Crash لاحقًا.
        strong_reclaim = bool(
            STRONG_RECLAIM_ENABLED
            and drawdown15 >= STRONG_RECLAIM_MIN_DRAWDOWN_PCT
            and structure_shift and reclaimed_15
            and candle15["close"] > e20_15
            and price > e20 and price > vw5
            and STRONG_RECLAIM_MIN_RSI_15M <= r15 <= STRONG_RECLAIM_MAX_RSI_15M
            and r15 > r15v[-3]
            and vr15 >= STRONG_RECLAIM_MIN_VOLUME_15M
            and vr5 >= STRONG_RECLAIM_MIN_VOLUME_5M
            and a15 >= STRONG_RECLAIM_MIN_ADX_15M
            and mtf["score"] >= STRONG_RECLAIM_MIN_MTF_SCORE
            and rel >= STRONG_RECLAIM_MIN_RELATIVE_STRENGTH
            and h5 >= 0 and macd_turn
            and dist <= STRONG_RECLAIM_MAX_EMA20_DISTANCE
            and close_location(candle15) >= 0.55
            and not s4["strongly_bearish"]
        )

        reversal = REVERSAL_ENABLED and drawdown15>=REVERSAL_MIN_DRAWDOWN_PCT and recent_rsi_low<=REVERSAL_MAX_RSI_RECENT_LOW and REVERSAL_MIN_RSI_NOW<=r15<=REVERSAL_MAX_RSI_NOW and r15>r15v[-3] and reclaimed_15 and structure_shift and vr15>=REVERSAL_MIN_VOLUME_15M and vr5>=REVERSAL_MIN_VOLUME_5M and a15>=REVERSAL_MIN_ADX_15M and h5>=0 and not s1["strongly_bearish"] and not s4["strongly_bearish"] and not market.get("hard_block",False)

        momentum = MOMENTUM_ENABLED and not late_wave and candle15["close"]>resistance and candle15["close"]>candle15["open"] and vr15>=MOMENTUM_MIN_VOLUME_15M and vr5>=MOMENTUM_MIN_VOLUME_5M and a15>=MOMENTUM_MIN_ADX_15M and MOMENTUM_MIN_RSI_15M<=r15<=MOMENTUM_MAX_RSI_15M and h15>0 and h5>=0 and e20_15>e20v[-4] and not s1["strongly_bearish"] and not s4["strongly_bearish"] and not market.get("severe_drop",False) and not market.get("hard_block",False) and rise15<=MOMENTUM_MAX_15M_RISE and rise1<=MOMENTUM_MAX_1H_RISE and dist<=MOMENTUM_MAX_EMA20_DISTANCE and greens<=MOMENTUM_MAX_GREEN and body<=4.0

        accumulation = bool(
            ACCUMULATION_ENABLED and not late_wave and accumulation_info
            and accumulation_info["compressed"] and accumulation_info["higher_lows"] and accumulation_info["building_volume"]
            and vols15[-1] >= mean(vols15[-4:-1]) * 0.90
            and early_break
            and vr15 >= ACCUMULATION_MIN_VOLUME_15M and vr5 >= ACCUMULATION_MIN_VOLUME_5M
            and ACCUMULATION_MIN_RSI_15M <= r15 <= ACCUMULATION_MAX_RSI_15M
            and a15 >= ACCUMULATION_MIN_ADX_15M and h15 >= 0 and h5 >= 0
            and e20_15 >= e20v[-4] and price > e20 and price > vw5
            and rise15 <= ACCUMULATION_MAX_15M_RISE and rise1 <= ACCUMULATION_MAX_1H_RISE
            and dist <= ACCUMULATION_MAX_EMA20_DISTANCE
            and not s1["strongly_bearish"] and not s4["strongly_bearish"]
            and not market.get("severe_drop", False) and not market.get("hard_block", False)
        )


        # المسار الخامس V18: Trend Pullback بعد تصحيح صحي داخل اتجاه صاعد.
        recent_pullback_window = closed15[-8:-1]
        recent_pullback_low = min(c["low"] for c in recent_pullback_window)
        touched_ema20 = recent_pullback_low <= e20_15 * 1.010
        held_ema50 = min(c["close"] for c in recent_pullback_window) >= e50_15 * 0.992
        bullish_1h = bool(
            s1.get("e50") is not None
            and s1["price"] > s1["e20"] > s1["e50"]
            and s1["macd_hist"] >= 0
        )
        bullish_4h = bool(
            s4.get("e50") is not None
            and s4["price"] > s4["e20"] > s4["e50"]
            and not s4["strongly_bearish"]
        )
        green_reclaim = bool(
            candle15["close"] > candle15["open"]
            and candle15["close"] > e20_15
            and close_location(candle15) >= 0.55
        )
        volume_rebound = bool(
            vr5 >= PULLBACK_MIN_VOLUME_5M
            and vols5[-1] >= mean(vols5[-6:-1])
        )
        pullback = bool(
            PULLBACK_ENABLED
            and bullish_1h and bullish_4h
            and touched_ema20 and held_ema50 and green_reclaim
            and PULLBACK_MIN_RSI_15M <= r15 <= PULLBACK_MAX_RSI_15M
            and a15 >= PULLBACK_MIN_ADX_15M
            and volume_rebound and h5 >= 0 and macd_turn
            and dist <= PULLBACK_MAX_EMA20_DISTANCE_PCT
            and not market.get("severe_drop", False)
            and not market.get("hard_block", False)
        )

        # V34: الحركة المتأخرة لا تمر كاختراق جديد. يسمح فقط بإعادة تصنيفها
        # إلى Strong Reclaim أو Pullback إذا تكوّن تصحيح حقيقي وقاعدة جديدة.
        if late_wave and not (strong_reclaim or pullback):
            return None

        # V19: لا نرسل Trend Pullback أثناء ضعف صحة BTC أو موجة بيع 15m.
        btc_health_score = float(market.get("btc_health_score", 50.0))
        btc_15m_move = float(market.get("btc", {}).get("rise_15m", 0.0))
        if pullback and BTC_HEALTH_ENABLED and not exceptional_strength and (
            btc_health_score < BTC_PULLBACK_MIN_HEALTH_SCORE
            or btc_15m_move <= -BTC_PULLBACK_MAX_15M_DROP_PCT
        ):
            log_rejection(symbol, "Trend Pullback مرفوض بسبب ضعف BTC متعدد الفريمات", {
                "btc_health_score": round(btc_health_score, 1),
                "required_health": BTC_PULLBACK_MIN_HEALTH_SCORE,
                "btc_15m": round(btc_15m_move, 3),
                "max_15m_drop": -BTC_PULLBACK_MAX_15M_DROP_PCT,
            })
            return None

        # V25: لا نغلق الباب على العملات المستقلة لمجرد ضعف BTC المتوسط.
        # التجاوز مسموح فقط لمسار Trend Ignition أو Exceptional Strength، وبشروط قوة واضحة.
        btc5_move = float(market.get("btc", {}).get("rise_5m", 0.0))
        btc15_move = float(market.get("btc", {}).get("rise_15m", 0.0))
        btc1h_move = float(market.get("btc", {}).get("rise_1h", 0.0))
        btc_catastrophic = bool(
            btc5_move <= -BTC_CATASTROPHIC_5M_DROP_PCT
            or btc15_move <= -BTC_CATASTROPHIC_15M_DROP_PCT
            or btc1h_move <= -BTC_CATASTROPHIC_1H_DROP_PCT
        )
        btc_flash_crash = bool(
            btc5_move <= -BTC_FLASH_CRASH_5M_DROP_PCT
            or btc15_move <= -BTC_FLASH_CRASH_15M_DROP_PCT
            or btc1h_move <= -BTC_FLASH_CRASH_1H_DROP_PCT
        )
        btc_override = bool(
            BTC_OVERRIDE_ENABLED
            and market.get("hard_block", False)
            and not btc_catastrophic
            and (surge_continuation or trend_ignition or exceptional_strength or strong_reclaim)
            and rel >= BTC_OVERRIDE_MIN_RELATIVE_STRENGTH
            and vr15 >= BTC_OVERRIDE_MIN_VOLUME_15M
            and vr5 >= BTC_OVERRIDE_MIN_VOLUME_5M
            and mtf["score"] >= BTC_OVERRIDE_MIN_MTF_SCORE
            and h15 >= 0 and h5 >= 0
            and price > e20 and price > vw5
            and r15 <= EXCEPTIONAL_MAX_RSI_15M
        )
        btc_extreme_override = bool(
            BTC_EXTREME_OVERRIDE_ENABLED
            and market.get("hard_block", False)
            and btc_catastrophic
            and not btc_flash_crash
            and (surge_continuation or trend_ignition or exceptional_strength or strong_reclaim)
            and rel >= BTC_EXTREME_MIN_RELATIVE_STRENGTH
            and vr15 >= BTC_EXTREME_MIN_VOLUME_15M
            and vr5 >= BTC_EXTREME_MIN_VOLUME_5M
            and mtf["score"] >= BTC_EXTREME_MIN_MTF_SCORE
            and h15 >= 0
            and price > e20 and price > vw5
            and r15 <= EXCEPTIONAL_MAX_RSI_15M
            and close_location(candle15) >= 0.55
        )
        btc_override = btc_override or btc_extreme_override

        # V31: Hard Block الصحي لم يعد إيقافًا مطلقًا لكل العملات. الإيقاف المطلق
        # فقط عند Flash Crash؛ أما العملة المستقلة فتُقيّم بمسارها وقوتها النسبية.
        if btc_flash_crash and not btc_override:
            log_rejection(symbol, "إيقاف كامل بسبب هبوط BTC", {
                "btc5": round(btc5_move, 3),
                "btc15": round(btc15_move, 3),
                "btc1h": round(btc1h_move, 3),
                "btc_rsi15": round(float(market.get("btc",{}).get("rsi15",0)), 2),
                "btc_catastrophic": btc_catastrophic,
                "btc_flash_crash": btc_flash_crash,
                "surge_continuation": surge_continuation,
                "trend_ignition": trend_ignition,
                "exceptional_strength": exceptional_strength,
                "relative_strength": round(rel, 2),
                "volume15": round(vr15, 2),
                "volume5": round(vr5, 2),
                "mtf_score": round(float(mtf["score"]), 1),
            })
            return None
        elif btc_override:
            override_kind = "extreme" if btc_extreme_override else "smart"
            log(f"BTC override accepted {symbol} [{override_kind}] | rel={rel:.2f}% | vr15={vr15:.2f} | vr5={vr5:.2f} | mtf={mtf['score']:.1f}")

        # في ضعف BTC المتوسط لا نسمح إلا بعملة تتفوق عليه بوضوح.
        if market.get("weak_pressure",False) and not (surge_continuation or exceptional_strength or strong_reclaim) and rel < BTC_WEAK_MIN_RELATIVE_STRENGTH:
            log_rejection(symbol, "ضعف BTC والقوة النسبية غير كافية", {
                "relative_strength": round(rel, 2),
                "required_relative_strength": BTC_WEAK_MIN_RELATIVE_STRENGTH,
                "btc15": round(float(market.get("btc",{}).get("rise_15m",0)), 3),
                "btc_rsi5": round(float(market.get("btc",{}).get("rsi5",0)), 2),
                "btc_rsi15": round(float(market.get("btc",{}).get("rsi15",0)), 2),
            })
            return None

        # مقاومة أعلى قريبة: لا نعتمد أي ذيل منفرد؛ نستخدم منطقة قمم محورية متكررة على الساعة.
        overhead = nearest_overhead_resistance(c1h[:-1], price, 100)
        if overhead:
            overhead_pct = float(overhead["distance_pct"])
            required_room = (
                MOMENTUM_MIN_NEXT_RESISTANCE_PCT if surge_continuation
                else ACCUMULATION_MIN_NEXT_RESISTANCE_PCT if accumulation
                else ACCUMULATION_MIN_NEXT_RESISTANCE_PCT if trend_ignition
                else PULLBACK_MIN_NEXT_RESISTANCE_PCT if pullback
                else MOMENTUM_MIN_NEXT_RESISTANCE_PCT if (momentum or reversal or strong_reclaim)
                else MIN_NEXT_RESISTANCE_PCT
            )
            if overhead_pct < required_room:
                log_rejection(symbol, "مقاومة قريبة مؤكدة", {
                    "distance_pct": round(overhead_pct, 2),
                    "required_pct": required_room,
                    "resistance": round(float(overhead["level"]), 10),
                    "touches": int(overhead["touches"]),
                })
                return None

        # V34: مساحة كافية حتى أقرب مقاومة يومية/4H موثوقة؛ تمنع الدخول تحت القمة مباشرة.
        higher_overhead = nearest_overhead_resistance(c4h[:-1], price, 140) or nearest_overhead_resistance(c1d[:-1], price, 140)
        if higher_overhead and float(higher_overhead["distance_pct"]) < LATE_MIN_ROOM_DAILY_PCT and not (strong_reclaim or pullback):
            log_rejection(symbol, "قمة عليا قريبة — هامش الربح غير كاف", {
                "distance_pct": round(float(higher_overhead["distance_pct"]), 2),
                "required_pct": LATE_MIN_ROOM_DAILY_PCT,
                "resistance": round(float(higher_overhead["level"]), 10),
                "touches": int(higher_overhead["touches"]),
            })
            return None

        # V27: تدقيق تعارض المسارات دون تغيير القرار. إذا تحققت عدة أنماط،
        # نُسجلها ويستمر الاختيار حسب MODE_PRIORITY المطابق لترتيب elif الحالي.
        matched_modes = [
            name for name, matched in (
                ("surge_continuation", surge_continuation),
                ("trend_ignition", trend_ignition),
                ("accumulation", accumulation),
                ("strong_reclaim", strong_reclaim),
                ("reversal", reversal),
                ("pullback", pullback),
                ("momentum", momentum),
            ) if matched
        ]
        if len(matched_modes) > 1:
            selected_by_priority = next(name for name in MODE_PRIORITY if name in matched_modes)
            log(
                f"Mode overlap {symbol}: matched={matched_modes} | "
                f"selected={selected_by_priority} | priority={MODE_PRIORITY}"
            )

        early_wave_triggered = bool(live_surge_break or live_structure_break or live_early_break)
        signal_price = live_price if early_wave_triggered else price

        if surge_continuation:
            score = 72
            reasons = [
                "⚡ اندفاع متكيف مستقل عن BTC مع اختراق قمة حديثة",
                f"قوة نسبية أمام BTC {rel:+.2f}%",
                f"حجم حالي 15m ×{vr15:.1f}/5m ×{vr5:.1f}، وأعلى حجم حديث ×{recent_vr15:.1f}/×{recent_vr5:.1f}",
                f"RSI زخم {r15:.1f} مع إغلاق صحي عند {close_location(candle15)*100:.0f}% من مدى الشمعة",
                f"ADX 15m {a15:.0f} وEMA20 صاعد/مستقر",
                f"توافق الفريمات {mtf['score']:.0f}/100",
                "السعر فوق EMA20 وVWAP",
            ]
            if live_surge_break:
                score += 10
                reasons.insert(0, "🚀 دخول مبكر أثناء تكوّن الاختراق قبل إغلاق 15m")
            elif breakout or squeeze_break: score += 8
            if max(vr15, recent_vr15) >= 1.5: score += 6
            if max(vr5, recent_vr5) >= 1.3: score += 4
            if rel >= 4.0: score += 6
            if obv5[-1] > obv5[-5]: score += 5
            if mtf["score"] >= 60: score += 5
            mode, setup = "surge_continuation", "Adaptive Surge / Multi-frame Breakout"
            # المسار مستقل عن مزاج BTC؛ لا نرفع حده بسبب weak market bonus.
            threshold = SURGE_MIN_SCORE
            recent_low = min(c["low"] for c in closed5[-8:-1])
            stop_candidates = [recent_low, e20 * 0.995, price - 1.25 * atr5]
            stop = max(x for x in stop_candidates if 0 < x < price)

        elif trend_ignition:
            score = int(ti_structure["score"])
            reasons = [
                "🚀 Trend Ignition V3: خروج مبكر من قاعدة هيكلية قبل الانفجار",
                f"📦 قاعدة {ti_structure['range_pct']:.1f}% وانكماش تذبذب إلى ×{ti_structure['range_ratio']:.2f}",
                f"🧱 رف مقاومة مؤكد عند {fmt(ti_structure['shelf_level'])} بعدد {ti_structure['shelf_touches']} لمسات",
                f"📈 قاع أحدث أعلى {ti_structure['higher_low_pct']:+.2f}%",
                f"🌊 بناء حجم ×{ti_structure['volume_build']:.2f} وحجم اختراق ×{ti_structure['breakout_volume']:.2f}",
                f"🎯 امتداد عن الرف {ti_structure['extension_pct']:+.2f}% وإغلاق الشمعة عند {ti_structure['close_location']*100:.0f}% من مداها",
                f"EMA20 مستقر/صاعد و{closes_above_e20}/5 إغلاقات فوقه",
                f"توافق الفريمات {mtf['score']:.0f}/100 وقوة نسبية {rel:+.2f}%",
            ]
            if live_structure_break:
                score += 8
                reasons.insert(0, "🚀 كسر حي للرف مع حجم متوقع قوي — بداية الموجة")
            if obv5[-1] > obv5[-5]: score += 5
            if tr15 >= 1.15: score += 4
            if rel >= 0.35: score += 5
            if market.get("regime") in ("إيجابي", "محايد"): score += 3
            mode, setup = "trend_ignition", "Trend Ignition V3 — Structure Break"
            threshold = max(TREND_IGNITION_MIN_SCORE, int(market.get("required_score", MIN_SCORE)))
            base_low = float(ti_structure["base_low"])
            recent_low = min(c["low"] for c in closed5[-12:-1])
            stop = min(base_low, recent_low, price-1.20*atr5)

        elif accumulation:
            score = 20+16+14+12+10+8+6
            reasons = [
                f"قاعدة تجميع مضغوطة {accumulation_info['range_pct']:.1f}%",
                f"قيعان صاعدة {accumulation_info['higher_low_pct']:+.2f}%",
                f"بناء تدريجي للحجم ×{accumulation_info['volume_build']:.2f}",
                "اختراق مبكر لقمة القاعدة",
                f"حجم 15m ×{vr15:.1f} وتأكيد 5m ×{vr5:.1f}",
                f"RSI مبكر غير متشبع {r15:.1f}",
                "MACD وEMA20 يتحسنان قبل الانطلاقة",
                "السعر فوق VWAP",
            ]
            if obv5[-1] > obv5[-5]: score += 6
            if tr15 >= 1.2: score += 5
            if rel >= 0.35: score += 6
            if market.get("regime") in ("إيجابي", "محايد"): score += 4
            mode, setup = "accumulation", "تجميع مبكر قبل الانطلاقة"
            threshold = max(ACCUMULATION_MIN_SCORE, int(market.get("required_score", MIN_SCORE)))
            base_low = float(accumulation_info["base_low"])
            recent_low = min(c["low"] for c in closed5[-10:-1])
            stop = min(base_low, recent_low, price-1.20*atr5)

        elif strong_reclaim:
            score = 22+16+14+12+10+8+6
            reasons = [
                f"⚡ Strong Reclaim بعد هبوط {drawdown15:.1f}%",
                "تحول هيكل 5m إلى قاع أعلى",
                "استعادة EMA20 على 15m بإغلاق صحي",
                f"قوة نسبية أمام BTC {rel:+.2f}%",
                f"حجم ارتداد 15m ×{vr15:.1f} وتأكيد 5m ×{vr5:.1f}",
                f"RSI استعادة غير متشبع {r15:.1f}",
                "MACD بدأ ينعطف والسعر فوق VWAP",
            ]
            if obv5[-1] > obv5[-5]: score += 6
            if rel >= 3.5: score += 5
            if vr15 >= 1.8: score += 4
            if market.get("regime") in ("إيجابي", "محايد"): score += 3
            mode, setup = "strong_reclaim", "Strong Reclaim / Early Reversal"
            threshold = max(STRONG_RECLAIM_MIN_SCORE, int(market.get("required_score", MIN_SCORE)))
            swing_low = min(c["low"] for c in closed15[-12:-1])
            recent_low = min(c["low"] for c in closed5[-12:-1])
            stop = min(swing_low, recent_low, price-1.25*atr5)

        elif reversal:
            score = 22+16+12+10+8+8+6
            reasons = [
                f"ارتداد بعد هبوط {drawdown15:.1f}%", "خروج RSI من الضعف",
                "تحول هيكل 5m إلى قاع أعلى", "استعادة EMA20 على 15m",
                f"حجم ارتداد 15m ×{vr15:.1f}", f"تأكيد حجم 5m ×{vr5:.1f}",
                "السعر فوق VWAP",
            ]
            reversal_4h_score = float(mtf.get("frames", {}).get("4h", 0))
            reversal_1h_score = float(mtf.get("frames", {}).get("1h", 0))
            if (
                mtf["score"] < REVERSAL_A_MIN_MTF_SCORE
                or reversal_4h_score < REVERSAL_A_MIN_4H_SCORE
                or reversal_1h_score < REVERSAL_A_MIN_1H_SCORE
            ):
                reasons.append(
                    f"تنبيه: الفريمات العليا غير مكتملة — MTF {mtf['score']:.0f}، 4H {reversal_4h_score:.0f}، 1H {reversal_1h_score:.0f}; لا يسمح بتصنيف A+"
                )
            if obv5[-1] > obv5[-5]: score += 6
            if rel >= 0.5: score += 6
            if market.get("regime") in ("إيجابي", "محايد"): score += 4
            mode, setup = "reversal", "ارتداد ذكي بعد هبوط"
            threshold = max(REVERSAL_MIN_SCORE, int(market.get("required_score", MIN_SCORE)))
            swing_low = min(c["low"] for c in closed15[-10:-1])
            stop = min(swing_low, price-1.35*atr5)

        elif pullback:
            score = 22+18+14+12+10+8+6
            reasons = [
                "اتجاه 4H و1H صاعد",
                "تصحيح صحي لملامسة EMA20 على 15m",
                "التصحيح حافظ على EMA50",
                "شمعة استعادة صاعدة فوق EMA20",
                f"RSI بعد التهدئة {r15:.1f}",
                f"عودة حجم الشراء على 5m ×{vr5:.1f}",
                f"ADX 15m {a15:.0f}",
                "MACD بدأ ينعطف صعودًا",
            ]
            if vr15 < PULLBACK_A_MIN_VOLUME_15M:
                reasons.append(f"تنبيه: حجم 15m ضعيف ×{vr15:.1f} — تم خفض الجودة")
            elif vr15 < PULLBACK_A_PLUS_MIN_VOLUME_15M:
                reasons.append(f"حجم 15m متوسط ×{vr15:.1f} — لا يسمح بتصنيف A+")
            if obv5[-1] > obv5[-5]: score += 6
            if rel >= 0.25: score += 5
            if market.get("regime") in ("إيجابي", "محايد"): score += 4
            mode, setup = "pullback", "Trend Pullback داخل اتجاه صاعد"
            threshold = max(PULLBACK_MIN_SCORE, int(market.get("required_score", MIN_SCORE)))
            recent = min(c["low"] for c in closed5[-12:-1])
            stop = min(recent, recent_pullback_low, price-1.20*atr5)

        elif momentum:
            score = 20+18+10+10+10+7+7+4+4
            reasons = [
                "اختراق قمة 15m بإغلاق واضح", "تأكيد 5m حافظ على مستوى الاختراق",
                f"حجم انفجاري 15m ×{vr15:.1f}", f"تأكيد حجم 5m ×{vr5:.1f}",
                f"قوة اتجاه ADX {a15:.0f}", f"RSI زخم مناسب {r15:.1f}",
                "MACD إيجابي على 15m و5m", "السعر فوق VWAP",
                "الساعة و4 ساعات لا يعاكسان الزخم",
            ]
            if obv5[-1] > obv5[-5]: score += 6
            if tr15 >= 1.4: score += 6
            if rel >= 0.25: score += 5
            if market.get("regime") == "إيجابي": score += 4
            mode, setup = "momentum", "زخم قوي واختراق 15m"
            threshold = max(MOMENTUM_MIN_SCORE, int(market.get("required_score", MIN_SCORE)))
            recent = min(c["low"] for c in closed5[-10:-1])
            stop = min(recent, resistance*0.992, price-1.10*atr5)

        else:
            if rise15 > MAX_15M_RISE_PCT or rise1 > MAX_1H_RISE_PCT or rise4 > MAX_4H_RISE_PCT or body > MAX_CANDLE_BODY_PCT or dist > MAX_EMA20_DISTANCE_PCT or greens > MAX_CONSECUTIVE_GREEN:
                return None
            if not (breakout or retest or squeeze_break) or not (52 <= r15 <= 65) or a15 < 22 or vr15 < MIN_15M_VOLUME_RATIO:
                return None
            if not (e20_15 > e50_15 and e20_15 > e20v[-4] and e50_15 >= e50v[-4]):
                return None
            if not (s1["e20"] > s1["e50"] and s1["macd_hist"] > 0 and 50 <= s1["rsi"] <= 68 and s1["adx"] >= 18) or not s4["not_bearish"]:
                return None
            score = (
                (18 if breakout else 0)+(20 if retest else 0)+(14 if squeeze_break else 0)
                +(12 if vr15 >= 2 else 0)+(6 if tr15 >= 1.4 else 0)
                +(8 if 52 <= r15 <= 62 else 0)+(8 if a15 >= 25 else 0)
                +8+10+5+(8 if dist <= 0.6 else 0)
                +(6 if m5 > s5 and r5 >= r5p else 0)
                +(5 if obv5[-1] > obv5[-5] else 0)
                +(7 if rel >= 0.8 else 0)
                +(4 if market.get("regime") == "إيجابي" else 0)
            )
            reasons = []
            if breakout: reasons += ["اختراق مؤكد على 15 دقيقة", "تأكيد 5m حافظ على مستوى الاختراق"]
            if retest: reasons.append("إعادة اختبار ناجحة على 15 دقيقة")
            if squeeze_break: reasons.append("خروج من انضغاط Bollinger")
            reasons += [
                f"حجم 15 دقيقة ×{vr15:.1f}", f"RSI 15m مناسب {r15:.1f}",
                f"ADX 15m {a15:.0f}", "تأكيد صاعد على الساعة",
                "4 ساعات لا يعاكس الصفقة",
            ]
            mode = "balanced"
            setup = "إعادة اختبار 15m" if retest else "انضغاط واختراق 15m" if squeeze_break else "اختراق 15m"
            threshold = max(82, int(market.get("required_score", MIN_SCORE)))
            recent = min(c["low"] for c in closed5[-12:-1])
            stop = min(recent, support, price-1.25*atr5)

        if exceptional_strength:
            score += EXCEPTIONAL_SCORE_BONUS
            reasons.append(
                f"قوة استثنائية مقابل BTC: تفوق {rel:+.2f}%، حجم 15m ×{vr15:.1f}، MTF {mtf['score']:.0f}"
            )

        score += int(mtf["adjustment"])
        reasons.append(f"توافق الفريمات {mtf['label']} ({mtf['score']:.0f}/100، تعديل {mtf['adjustment']:+d})")
        if launch_mode:
            score += LAUNCH_SCORE_BONUS
            threshold = max(MIN_SCORE, threshold - LAUNCH_THRESHOLD_RELIEF)
            reasons.append(
                f"وضع الانطلاقة المبكرة: توافق 1H/15M/5M مع حجم ×{vr15:.1f} وADX {a15:.0f}"
            )
        # V28: تأهيل A المبكر لحالات مثل COTI وDIA.
        # لا يخلق نمطًا جديدًا ولا يتجاوز فلاتر الأمان السابقة؛ فقط يمنع رفض
        # Trend Ignition / Exceptional Strength بسبب نقص محدود في السكور النهائي.
        score_gap = max(0, int(threshold) - int(score))
        strong_ignition_for_a = bool(
            mode == "trend_ignition"
            and trend_ignition
            and ti_structure
            and float(ti_structure.get("score", 0)) >= EARLY_A_MIN_STRUCTURE_SCORE
            and bool(ti_structure.get("breakout_ready", False))
        )
        strong_exceptional_for_a = bool(exceptional_strength)
        early_a_qualified = bool(
            EARLY_A_ENABLED
            and (strong_ignition_for_a or strong_exceptional_for_a)
            and score_gap <= EARLY_A_MAX_SCORE_GAP
            and float(mtf["score"]) >= EARLY_A_MIN_MTF_SCORE
            and rel >= EARLY_A_MIN_RELATIVE_STRENGTH
            and vr15 >= EARLY_A_MIN_VOLUME_15M
            and vr5 >= EARLY_A_MIN_VOLUME_5M
            and r15 <= EARLY_A_MAX_RSI_15M
            and h15 >= 0
            and price > e20 and price > vw5
            and not btc_flash_crash
        )

        if score < threshold and early_a_qualified:
            original_score = int(score)
            score = int(threshold)
            reasons.append(
                f"تأهيل A مبكر: النمط قوي والسكور كان أقل من الحد بـ {score_gap} نقاط فقط"
            )
            log(
                f"Early A accepted {symbol} | mode={mode} | "
                f"score={original_score}->{score} | gap={score_gap} | "
                f"rel={rel:.2f}% | vr15={vr15:.2f} | mtf={mtf['score']:.1f}"
            )
        elif score < threshold:
            log_rejection(symbol, "السكور أقل من حد المسار", {
                "mode": mode,
                "score": int(score),
                "threshold": int(threshold),
                "score_gap": score_gap,
                "early_a_qualified": early_a_qualified,
                "strong_ignition_for_a": strong_ignition_for_a,
                "strong_exceptional_for_a": strong_exceptional_for_a,
                "mtf_score": round(float(mtf["score"]), 1),
                "mtf_adjustment": int(mtf["adjustment"]),
            })
            return None
        # عند الإشارة الحية تُحسب الصفقة من السعر الحي لا من آخر إغلاق 5m.
        price = signal_price
        risk=price-stop
        max_risk = (
            SURGE_MAX_RISK_PCT if mode=="surge_continuation"
            else TREND_IGNITION_MAX_RISK_PCT if mode=="trend_ignition"
            else ACCUMULATION_MAX_RISK_PCT if mode=="accumulation"
            else PULLBACK_MAX_RISK_PCT if mode=="pullback"
            else STRONG_RECLAIM_MAX_RISK_PCT if mode=="strong_reclaim"
            else REVERSAL_MAX_RISK_PCT if mode=="reversal"
            else MAX_RISK_PCT
        )
        if risk <= 0 or risk / price * 100 > max_risk:
            log_rejection(symbol, "المخاطرة خارج الحد", {
                "mode": mode,
                "risk_pct": round((risk / price * 100) if price > 0 else 999.0, 3),
                "max_risk_pct": max_risk,
                "entry": price,
                "stop": stop,
            })
            return None
        final_score = min(score, 99)
        quality, quality_stars = signal_quality(final_score, mtf["score"], mode, launch_mode, vr15, mtf.get("frames", {}))
        if early_a_qualified or mode == "strong_reclaim":
            quality, quality_stars = "A", "⭐⭐⭐⭐"
        return {"symbol":symbol,"entry":price,"stop":stop,"tp1":price+1.5*risk,"tp2":price+2.2*risk,"tp3":price+3*risk,"risk_pct":risk/price*100,"score":final_score,"quality":quality,"quality_stars":quality_stars,"volume_ratio":vr15,"recent_volume_ratio_15m":recent_vr15,"recent_volume_ratio_5m":recent_vr5,"rsi":r15,"adx":a15,"setup":setup,"mode":mode,"reasons":reasons[:8],"candle_close":candle5["close_time"],"market_regime":market.get("regime","غير متاح"),"market_environment":market_environment_score(market),"btc_1h":float(market.get("btc",{}).get("rise_1h",0)),"btc_15m":float(market.get("btc",{}).get("rise_15m",0)),"btc_rsi15":float(market.get("btc",{}).get("rsi15",0)),"btc_health_score":float(market.get("btc_health_score",50)),"btc_health_label":market.get("btc_health_label","غير متاح"),"btc_health_frames":market.get("btc_health_frames",{}),"relative_strength":rel,"mtf_score":mtf["score"],"mtf_label":mtf["label"],"mtf_adjustment":mtf["adjustment"],"mtf_frames":mtf["frames"],"mtf_weights":mtf.get("weights",{}),"launch_mode":launch_mode,"exceptional_strength":exceptional_strength,"surge_continuation":surge_continuation,"trend_ignition":trend_ignition,"strong_reclaim":strong_reclaim,"btc_override":btc_override,"btc_extreme_override":btc_extreme_override,"btc_flash_crash":btc_flash_crash,"early_a_qualified":early_a_qualified,"ti_structure":ti_structure or {},"early_wave_triggered":early_wave_triggered,"live_volume_ratio_15m":live_vr15,"live_volume_ratio_5m":live_vr5,"breakout_extension_pct":breakout_extension_pct,"ema_extension_atr":ema_extension_atr,"wave_progress":float(wave_stage.get("progress",0.0)),"late_wave":late_wave,"wave_stage":wave_stage,"independence_score":coin_independence_score(rel, vr15, float(mtf["score"]), float(market.get("btc_health_score",50)), exceptional_strength),"first_leg":first_leg,"first_leg_late":first_leg_late}
    except Exception as exc:
        log(f"Analyze {symbol}: {exc}"); return None


def fmt(v: float) -> str:
    d=2 if v>=1000 else 4 if v>=1 else 5 if v>=0.01 else 8
    return f"{v:.{d}f}"


def signal_message(r: Dict) -> str:
    reasons="\n".join(f"• {x}" for x in r["reasons"])
    kind="اندفاع متكيف ⚡" if r.get("mode")=="surge_continuation" else "Trend Ignition 🚀" if r.get("mode")=="trend_ignition" else "تجميع قبل الانطلاقة" if r.get("mode")=="accumulation" else "Strong Reclaim ⚡" if r.get("mode")=="strong_reclaim" else "Trend Pullback" if r.get("mode")=="pullback" else "انطلاقة قوية" if r.get("mode")=="momentum" else "ارتداد ذكي" if r.get("mode")=="reversal" else "دخول متوازن"
    title = "⚡ إشارة اندفاع مستقلة" if r.get("mode")=="surge_continuation" else "🚀 إشارة Trend Ignition مستقلة" if r.get("mode")=="trend_ignition" else "🟢 إشارة شراء سبوت"
    return f"""{title} — {r['symbol']}

📈 التحليل متعدد الفريمات
• 1W: {r.get('mtf_frames',{}).get('1w',50):.0f}/100 — وزن {r.get('mtf_weights',{}).get('1w',0):.0f}%
• 1D: {r.get('mtf_frames',{}).get('1d',50):.0f}/100 — وزن {r.get('mtf_weights',{}).get('1d',0):.0f}%
• 4H: {r.get('mtf_frames',{}).get('4h',50):.0f}/100 — وزن {r.get('mtf_weights',{}).get('4h',0):.0f}%
• 1H: {r.get('mtf_frames',{}).get('1h',50):.0f}/100 — وزن {r.get('mtf_weights',{}).get('1h',0):.0f}%
• 15M: {r.get('mtf_frames',{}).get('15m',50):.0f}/100 — وزن {r.get('mtf_weights',{}).get('15m',0):.0f}%
• 5M: {r.get('mtf_frames',{}).get('5m',50):.0f}/100 — وزن {r.get('mtf_weights',{}).get('5m',0):.0f}%
• التوافق العام: {r.get('mtf_score',50):.1f}/100 — {r.get('mtf_label','محايد')}
• تعديل التقييم: {r.get('mtf_adjustment',0):+d}
• وضع الانطلاقة: {'مفعل 🚀' if r.get('launch_mode') else 'غير مفعل'}
• القوة الاستثنائية: {'مفعلة ⚡' if r.get('exceptional_strength') else 'غير مفعلة'}
• تأهيل A المبكر: {'مفعل 🚀' if r.get('early_a_qualified') else 'غير مفعل'}
• تجاوز فلتر BTC: {'استثنائي 🚀' if r.get('btc_extreme_override') else 'نعم 🛡️' if r.get('btc_override') else 'لا'}

النموذج: {r['setup']}
نوع الإشارة: {kind}
قوة الإشارة: {r['score']}%
🏆 جودة الإشارة: {r.get('quality','A')} {r.get('quality_stars','⭐⭐⭐⭐')}

🌍 حالة السوق
• السوق: {r['market_regime']}
• بيئة السوق: {r.get('market_environment',{}).get('score',50):.1f}/100 — {r.get('market_environment',{}).get('label','محايد')}
• حجم الصفقة المقترح: {r.get('market_environment',{}).get('suggested_size','50%')}
• صحة BTC: {r.get('btc_health_score',50):.1f}/100 — {r.get('btc_health_label','غير متاح')}
• BTC 4H/1H/15M/5M: {r.get('btc_health_frames',{}).get('4h',50):.0f}/{r.get('btc_health_frames',{}).get('1h',50):.0f}/{r.get('btc_health_frames',{}).get('15m',50):.0f}/{r.get('btc_health_frames',{}).get('5m',50):.0f}
• BTC 1H: {r['btc_1h']:+.2f}%
• BTC 15M: {r['btc_15m']:+.2f}%
• RSI BTC: {r['btc_rsi15']:.1f}
• القوة النسبية: {r['relative_strength']:+.2f}%
• استقلال العملة عن BTC: {r.get('independence_score',0)}/100

🎯 مستويات الصفقة
• دخول: {fmt(r['entry'])}
• وقف: {fmt(r['stop'])} ({r['risk_pct']:.2f}%)
• TP1: {fmt(r['tp1'])}
• TP2: {fmt(r['tp2'])}
• TP3: {fmt(r['tp3'])}

📊 المؤشرات
• RSI: {r['rsi']:.1f}
• ADX: {r['adx']:.1f}
• الحجم: ×{r['volume_ratio']:.1f}

✅ أسباب الإشارة
{reasons}

⚠️ تحليل فني آلي وليس ضمانًا للربح."""
def load_state():
    try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception: return {"alerts":{}}


def save_state(s): STATE_FILE.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")

def cooled(s,r): return r["candle_close"]-int(s.get("alerts",{}).get(r["symbol"],0))>=COOLDOWN_MINUTES*60*1000


def track_open_signals(state: Dict) -> None:
    if not TRACK_RESULTS: return
    op=state.setdefault("open_signals",{})
    stats=state.setdefault("stats",{"tp1":0,"tp2":0,"tp3":0,"stop":0})
    now=int(time.time()*1000)
    for symbol,signal in list(op.items()):
        try:
            candles=get_klines(symbol,"5m",100)[:-1]
            last=int(signal.get("last_checked",signal["time"]))
            rel=[c for c in candles if c["close_time"]>last]
            if not rel:
                if now-int(signal["time"])>48*60*60*1000: del op[symbol]
                continue
            reached=int(signal.get("reached",0)); closed=False
            entry=float(signal["entry"]); stop=float(signal["stop"])
            for c in rel:
                if c["low"]<=stop:
                    stats["stop"]+=1
                    if TRACKER_NOTIFICATIONS_ENABLED and TRACKER_NOTIFY_STOP:
                        send_message(f"🛑 متابعة {symbol}\nتم لمس وقف الخسارة عند {fmt(stop)}.\nالدخول: {fmt(entry)}")
                    del op[symbol]; closed=True; break
                if reached<1 and c["high"]>=float(signal["tp1"]):
                    stats["tp1"]+=1; reached=1
                    if TRACKER_NOTIFICATIONS_ENABLED and TRACKER_NOTIFY_TP:
                        send_message(f"✅ متابعة {symbol}\nتم تحقيق TP1 عند {fmt(float(signal['tp1']))}.")
                if reached<2 and c["high"]>=float(signal["tp2"]):
                    stats["tp2"]+=1; reached=2
                    if TRACKER_NOTIFICATIONS_ENABLED and TRACKER_NOTIFY_TP:
                        send_message(f"✅ متابعة {symbol}\nتم تحقيق TP2 عند {fmt(float(signal['tp2']))}.")
                if reached<3 and c["high"]>=float(signal["tp3"]):
                    stats["tp3"]+=1
                    if TRACKER_NOTIFICATIONS_ENABLED and TRACKER_NOTIFY_TP:
                        send_message(f"🏆 متابعة {symbol}\nتم تحقيق TP3 عند {fmt(float(signal['tp3']))}.")
                    del op[symbol]; closed=True; break
                signal["last_checked"]=c["close_time"]
            if not closed:
                signal["reached"]=reached
                signal["last_checked"]=rel[-1]["close_time"]
        except Exception as exc:
            log(f"Track {symbol}: {exc}")


def scan(state: Dict) -> None:
    track_open_signals(state); market=market_context(); symbols=get_symbols(); ranked=[]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for f in as_completed([pool.submit(prefilter_symbol,s) for s in symbols]):
            x=f.result()
            if x: ranked.append(x)
    ranked.sort(key=lambda x:x[1],reverse=True); shortlist=[s for s,_ in ranked[:PREFILTER_LIMIT]]; results=[]
    with ThreadPoolExecutor(max_workers=max(4,MAX_WORKERS//2)) as pool:
        for f in as_completed([pool.submit(analyze_symbol,s,market) for s in shortlist]):
            x=f.result()
            if x and cooled(state,x): results.append(x)
    results.sort(key=lambda x:(7 if x.get("mode")=="surge_continuation" else 6 if x.get("mode")=="trend_ignition" else 5 if x.get("mode")=="strong_reclaim" else 4 if x.get("mode")=="pullback" else 3 if x.get("mode")=="accumulation" else 2 if x.get("mode")=="momentum" else 1 if x.get("mode")=="reversal" else 0,x["score"],x["volume_ratio"]),reverse=True)
    sent=0
    for r in results[:MAX_ALERTS_PER_SCAN]:
        if not send_message(signal_message(r)):
            continue
        state.setdefault("alerts",{})[r["symbol"]]=r["candle_close"]
        state.setdefault("open_signals",{})[r["symbol"]]={"time":r["candle_close"],"last_checked":r["candle_close"],"entry":r["entry"],"stop":r["stop"],"tp1":r["tp1"],"tp2":r["tp2"],"tp3":r["tp3"],"reached":0,"mode":r.get("mode","balanced")}
        sent+=1; time.sleep(0.3)
    save_state(state); log(f"Scan finished | market={market.get('regime')} | universe={len(symbols)} | shortlist={len(shortlist)} | candidates={len(results)} | sent={sent}")


def main() -> None:
    state=load_state();
    if COMMANDS_ENABLED:
        Thread(target=telegram_command_loop, daemon=True, name="telegram-commands").start()
    try:
        send_message("✅ تم تشغيل بوت إشارات الشراء للسبوت V38 Smart Market Manager.\nالمسار الأول: دخول متوازن وإعادة اختبار.\nالمسار الثاني: زخم قوي لالتقاط الانطلاقات.\nالمسار الثالث: ارتداد ذكي بعد الهبوط.\nالمسار الرابع: تجميع مبكر قبل الانطلاقة.\nالمسار الخامس: Trend Pullback داخل اتجاه صاعد.\nتم تفعيل جودة A+/A فقط مع محرك بنية السعر Trend Ignition V3.\nتم ربط جودة Trend Pullback بحجم 15m لمنع A+ عند ضعف السيولة.\nتم تفعيل صحة BTC متعددة الفريمات ومنع Pullback أثناء ضعف السوق.\nتم تفعيل استثناء القوة الاستثنائية لالتقاط العملات المستقلة عن BTC دون تجاوز Hard Block.\nتم الإبقاء على حماية BTC متعددة الفريمات وجميع مسارات V17.\nتم تفعيل /السوق و /debug و /صفقات و /إحصائيات، ودرجة بيئة السوق واستقلال العملة عن BTC.\nإشارات فقط — بدون تداول تلقائي وبدون شورت وبدون WATCH.")
    except Exception as exc:
        log(f"Startup message failed: {exc}")
    while True:
        started=time.time()
        try: scan(state)
        except Exception as exc: log(f"Scan error: {exc}")
        time.sleep(max(5,SCAN_MINUTES*60-(time.time()-started)))





if __name__ == "__main__":
    main()


