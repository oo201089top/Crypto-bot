import os
import time
import json
import math
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple

import requests

# ============================================================
# Smart Scalping Signal Bot
# مصدر البيانات: Binance Spot
# يرسل إشارات فنية فقط ولا ينفذ صفقات.
# ============================================================

BINANCE_BASE = "https://data-api.binance.vision"
BYBIT_BASE = "https://api.bybit.com"
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

ENTRY_TIMEFRAME = os.getenv("ENTRY_TIMEFRAME", "5m")
CONFIRM_TIMEFRAME = os.getenv("CONFIRM_TIMEFRAME", "15m")
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")

SCAN_MINUTES = int(os.getenv("SCAN_MINUTES", "3"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "68"))
WATCH_SCORE = int(os.getenv("WATCH_SCORE", "58"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "90"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
STATUS_EVERY_SCANS = int(os.getenv("STATUS_EVERY_SCANS", "20"))
MIN_RR = float(os.getenv("MIN_RR", "1.5"))
MAX_RISK_PCT = float(os.getenv("MAX_RISK_PCT", "3.5"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME", "1000000"))
MAX_QUOTE_VOLUME = float(os.getenv("MAX_QUOTE_VOLUME", "250000000"))
MAX_SYMBOLS_PER_EXCHANGE = int(os.getenv("MAX_SYMBOLS_PER_EXCHANGE", "45"))

# سبوت فقط: نستبعد العملات الكبيرة والعملات المستقرة والرموز ذات الرافعة.
EXCLUDED_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "AVAX",
    "LINK", "SUI", "TON", "DOT", "LTC", "BCH", "SHIB", "HBAR", "XLM",
    "UNI", "AAVE", "ETC", "NEAR", "APT", "PEPE", "TAO", "ICP", "FIL",
    "USDC", "USDT", "FDUSD", "TUSD", "DAI", "USDE", "PYUSD", "EUR",
}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")


STATE_FILE = Path("smart_signal_state.json")
SESSION = requests.Session()


# =========================
# الاتصال
# =========================
def send_message(text: str) -> None:
    response = SESSION.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={
            "chat_id": CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    response.raise_for_status()


def _bybit_interval(interval: str) -> str:
    mapping = {
        "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
        "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
        "1d": "D", "1w": "W", "1M": "M",
    }
    if interval not in mapping:
        raise ValueError(f"Bybit does not support timeframe: {interval}")
    return mapping[interval]


def get_klines(exchange: str, symbol: str, interval: str, limit: int = 260) -> List[Dict]:
    if exchange == "BINANCE":
        response = SESSION.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=20,
        )
        response.raise_for_status()
        rows = response.json()
        return [{
            "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
            "close": float(x[4]), "volume": float(x[5]),
            "close_time": int(x[6]), "quote_volume": float(x[7]),
        } for x in rows]

    response = SESSION.get(
        f"{BYBIT_BASE}/v5/market/kline",
        params={
            "category": "spot", "symbol": symbol,
            "interval": _bybit_interval(interval), "limit": limit,
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("retCode") != 0:
        raise RuntimeError(payload.get("retMsg", "Bybit kline error"))

    # Bybit returns newest first; indicators need oldest first.
    rows = list(reversed(payload["result"]["list"]))
    interval_ms = {
        "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
        "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
        "4h": 14_400_000, "6h": 21_600_000, "12h": 43_200_000,
        "1d": 86_400_000, "1w": 604_800_000,
    }.get(interval, 0)
    return [{
        "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
        "close": float(x[4]), "volume": float(x[5]),
        "quote_volume": float(x[6]),
        "close_time": int(x[0]) + interval_ms - 1,
    } for x in rows]


def _valid_small_mid_symbol(symbol: str) -> bool:
    if not symbol.endswith("USDT"):
        return False
    base = symbol[:-4]
    if base in EXCLUDED_BASES or base.endswith(LEVERAGED_SUFFIXES):
        return False
    return bool(base) and base.isalnum()


def discover_binance_symbols() -> List[Dict]:
    response = SESSION.get(f"{BINANCE_BASE}/api/v3/ticker/24hr", timeout=25)
    response.raise_for_status()
    candidates = []
    for item in response.json():
        symbol = item.get("symbol", "")
        if not _valid_small_mid_symbol(symbol):
            continue
        quote_volume = float(item.get("quoteVolume", 0) or 0)
        change = abs(float(item.get("priceChangePercent", 0) or 0))
        if MIN_QUOTE_VOLUME <= quote_volume <= MAX_QUOTE_VOLUME:
            candidates.append({
                "exchange": "BINANCE", "symbol": symbol,
                "quote_volume": quote_volume, "change": change,
            })
    candidates.sort(key=lambda x: (x["change"], x["quote_volume"]), reverse=True)
    return candidates[:MAX_SYMBOLS_PER_EXCHANGE]


def discover_bybit_symbols() -> List[Dict]:
    response = SESSION.get(
        f"{BYBIT_BASE}/v5/market/tickers", params={"category": "spot"}, timeout=25
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("retCode") != 0:
        raise RuntimeError(payload.get("retMsg", "Bybit tickers error"))

    candidates = []
    for item in payload["result"]["list"]:
        symbol = item.get("symbol", "")
        if not _valid_small_mid_symbol(symbol):
            continue
        quote_volume = float(item.get("turnover24h", 0) or 0)
        change = abs(float(item.get("price24hPcnt", 0) or 0)) * 100
        if MIN_QUOTE_VOLUME <= quote_volume <= MAX_QUOTE_VOLUME:
            candidates.append({
                "exchange": "BYBIT", "symbol": symbol,
                "quote_volume": quote_volume, "change": change,
            })
    candidates.sort(key=lambda x: (x["change"], x["quote_volume"]), reverse=True)
    return candidates[:MAX_SYMBOLS_PER_EXCHANGE]


def discover_symbols() -> List[Dict]:
    combined: List[Dict] = []
    for finder in (discover_binance_symbols, discover_bybit_symbols):
        try:
            combined.extend(finder())
        except Exception as exc:
            print(f"Symbol discovery error: {exc}", flush=True)
    return combined


# =========================
# المؤشرات الفنية
# =========================
def ema(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period:
        return [None] * len(values)

    multiplier = 2 / (period + 1)
    result: List[Optional[float]] = [None] * (period - 1)
    current = mean(values[:period])
    result.append(current)

    for value in values[period:]:
        current = (value - current) * multiplier + current
        result.append(current)

    return result


def rsi(values: List[float], period: int = 14) -> List[Optional[float]]:
    if len(values) <= period:
        return [None] * len(values)

    gains: List[float] = []
    losses: List[float] = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])
    output: List[Optional[float]] = [None] * period

    def calc(gain: float, loss: float) -> float:
        if loss == 0:
            return 100.0
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    output.append(calc(avg_gain, avg_loss))

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        output.append(calc(avg_gain, avg_loss))

    return output


def atr(candles: List[Dict], period: int = 14) -> float:
    values: List[float] = []

    for i, candle in enumerate(candles):
        if i == 0:
            values.append(candle["high"] - candle["low"])
        else:
            previous_close = candles[i - 1]["close"]
            values.append(max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            ))

    return mean(values[-period:])


def macd(values: List[float]) -> Tuple[float, float, float]:
    fast = ema(values, 12)
    slow = ema(values, 26)

    series: List[float] = []
    for fast_value, slow_value in zip(fast, slow):
        if fast_value is not None and slow_value is not None:
            series.append(fast_value - slow_value)

    signal = ema(series, 9)
    macd_now = series[-1]
    signal_now = signal[-1]
    return macd_now, signal_now, macd_now - signal_now


def bollinger(values: List[float], period: int = 20, multiplier: float = 2.0) -> Tuple[float, float, float]:
    window = values[-period:]
    middle = mean(window)
    deviation = pstdev(window)
    return middle, middle + multiplier * deviation, middle - multiplier * deviation


def adx(candles: List[Dict], period: int = 14) -> float:
    if len(candles) < period + 2:
        return 0.0

    tr_values: List[float] = []
    plus_dm: List[float] = []
    minus_dm: List[float] = []

    for i in range(1, len(candles)):
        current = candles[i]
        previous = candles[i - 1]

        up_move = current["high"] - previous["high"]
        down_move = previous["low"] - current["low"]

        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)

        tr_values.append(max(
            current["high"] - current["low"],
            abs(current["high"] - previous["close"]),
            abs(current["low"] - previous["close"]),
        ))

    dx_values: List[float] = []

    for end in range(period, len(tr_values) + 1):
        tr_sum = sum(tr_values[end - period:end])
        if tr_sum == 0:
            continue

        plus_di = 100 * sum(plus_dm[end - period:end]) / tr_sum
        minus_di = 100 * sum(minus_dm[end - period:end]) / tr_sum
        denominator = plus_di + minus_di

        if denominator:
            dx_values.append(100 * abs(plus_di - minus_di) / denominator)

    return mean(dx_values[-period:]) if dx_values else 0.0


def vwap(candles: List[Dict], period: int = 20) -> float:
    window = candles[-period:]
    volume_sum = sum(c["volume"] for c in window)

    if volume_sum == 0:
        return window[-1]["close"]

    total = sum(
        ((c["high"] + c["low"] + c["close"]) / 3) * c["volume"]
        for c in window
    )
    return total / volume_sum


def obv(candles: List[Dict]) -> List[float]:
    output = [0.0]

    for i in range(1, len(candles)):
        if candles[i]["close"] > candles[i - 1]["close"]:
            output.append(output[-1] + candles[i]["volume"])
        elif candles[i]["close"] < candles[i - 1]["close"]:
            output.append(output[-1] - candles[i]["volume"])
        else:
            output.append(output[-1])

    return output


# =========================
# الحالة والتعلم البسيط
# =========================
def load_state() -> Dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {
            "alerts": {},
            "performance": {
                "LONG": {"wins": 0, "losses": 0},
                "SHORT": {"wins": 0, "losses": 0},
            },
            "scan_count": 0,
        }


def save_state(state: Dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2)
    )


def adaptive_bonus(state: Dict, side: str) -> int:
    stats = state.get("performance", {}).get(side, {"wins": 0, "losses": 0})
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    total = wins + losses

    if total < 5:
        return 0

    win_rate = wins / total
    if win_rate >= 0.65:
        return 4
    if win_rate <= 0.35:
        return -4
    return 0


# =========================
# تحليل اتجاه الفريم
# =========================
def trend_snapshot(candles: List[Dict]) -> Dict:
    closed = candles[:-1]
    closes = [c["close"] for c in closed]

    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1]
    rsi_now = rsi(closes)[-1]
    _, _, histogram = macd(closes)

    price = closes[-1]

    return {
        "bullish": price > e20 > e50 and price > e200 and histogram > 0,
        "bearish": price < e20 < e50 and price < e200 and histogram < 0,
        "neutral": not (
            price > e20 > e50 and price > e200 and histogram > 0
        ) and not (
            price < e20 < e50 and price < e200 and histogram < 0
        ),
        "rsi": rsi_now,
    }


def market_regime(candles: List[Dict]) -> str:
    closed = candles[:-1]
    closes = [c["close"] for c in closed]
    current_atr = atr(closed)
    atr_pct = current_atr / closes[-1] * 100
    current_adx = adx(closed)

    if current_adx >= 25 and atr_pct >= 0.35:
        return "TRENDING"
    if current_adx < 18:
        return "RANGING"
    return "MIXED"


# =========================
# التحليل الذكي
# =========================
def analyze_symbol(exchange: str, symbol: str, state: Dict, btc_filter: Dict) -> Optional[Dict]:
    entry_candles = get_klines(exchange, symbol, ENTRY_TIMEFRAME)
    confirm_candles = get_klines(exchange, symbol, CONFIRM_TIMEFRAME)
    trend_candles = get_klines(exchange, symbol, TREND_TIMEFRAME)

    closed = entry_candles[:-1]
    closes = [c["close"] for c in closed]
    volumes = [c["volume"] for c in closed]
    quote_volumes = [c["quote_volume"] for c in closed]

    price = closes[-1]
    candle = closed[-1]

    e9 = ema(closes, 9)[-1]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]

    rsi_values = rsi(closes)
    rsi_now = rsi_values[-1]
    rsi_prev = rsi_values[-2]

    macd_now, macd_signal, macd_hist = macd(closes)
    current_atr = atr(closed)
    adx_now = adx(closed)
    bb_mid, bb_upper, bb_lower = bollinger(closes)
    current_vwap = vwap(closed)
    obv_values = obv(closed)

    avg_volume = mean(volumes[-21:-1])
    volume_ratio = volumes[-1] / avg_volume if avg_volume else 0.0
    avg_quote_volume = mean(quote_volumes[-12:])

    resistance = max(c["high"] for c in closed[-21:-1])
    support = min(c["low"] for c in closed[-21:-1])

    confirm = trend_snapshot(confirm_candles)
    trend = trend_snapshot(trend_candles)
    regime = market_regime(entry_candles)

    candle_range = max(candle["high"] - candle["low"], 1e-12)
    body = abs(candle["close"] - candle["open"])
    body_ratio = body / candle_range
    upper_wick = candle["high"] - max(candle["open"], candle["close"])
    lower_wick = min(candle["open"], candle["close"]) - candle["low"]
    upper_wick_ratio = upper_wick / candle_range
    lower_wick_ratio = lower_wick / candle_range

    atr_pct = current_atr / price * 100
    distance_ema20 = abs(price - e20) / price * 100

    long_score = 0
    short_score = 0
    long_reasons: List[str] = []
    short_reasons: List[str] = []
    warnings: List[str] = []

    def add_long(condition: bool, points: int, reason: str) -> None:
        nonlocal long_score
        if condition:
            long_score += points
            long_reasons.append(reason)

    def add_short(condition: bool, points: int, reason: str) -> None:
        nonlocal short_score
        if condition:
            short_score += points
            short_reasons.append(reason)

    # الزخم السريع
    add_long(price > e9 > e20, 10, "EMA9 وEMA20 إيجابيان")
    add_short(price < e9 < e20, 10, "EMA9 وEMA20 سلبيان")

    add_long(e20 > e50, 8, "الاتجاه القصير صاعد")
    add_short(e20 < e50, 8, "الاتجاه القصير هابط")

    add_long(48 <= rsi_now <= 70 and rsi_now > rsi_prev, 9, f"RSI صاعد {rsi_now:.1f}")
    add_short(30 <= rsi_now <= 52 and rsi_now < rsi_prev, 9, f"RSI هابط {rsi_now:.1f}")

    add_long(macd_now > macd_signal and macd_hist > 0, 9, "MACD إيجابي")
    add_short(macd_now < macd_signal and macd_hist < 0, 9, "MACD سلبي")

    # السيولة والحركة
    add_long(volume_ratio >= 1.20, 9, f"الفوليوم ×{volume_ratio:.1f}")
    add_short(volume_ratio >= 1.20, 9, f"الفوليوم ×{volume_ratio:.1f}")

    add_long(adx_now >= 18, 6, f"ADX {adx_now:.0f}")
    add_short(adx_now >= 18, 6, f"ADX {adx_now:.0f}")

    add_long(price > current_vwap, 6, "السعر فوق VWAP")
    add_short(price < current_vwap, 6, "السعر تحت VWAP")

    add_long(obv_values[-1] > obv_values[-4], 5, "OBV يدعم الشراء")
    add_short(obv_values[-1] < obv_values[-4], 5, "OBV يدعم البيع")

    # شمعة الدخول
    add_long(body_ratio >= 0.50 and candle["close"] > candle["open"], 7, "شمعة شرائية قوية")
    add_short(body_ratio >= 0.50 and candle["close"] < candle["open"], 7, "شمعة بيعية قوية")

    # الاختراق أو الارتداد
    breakout_long = price > resistance and volume_ratio >= 1.15
    breakout_short = price < support and volume_ratio >= 1.15

    pullback_long = (
        e9 > e20 > e50
        and candle["low"] <= e20 * 1.003
        and price > e20
        and candle["close"] > candle["open"]
    )

    pullback_short = (
        e9 < e20 < e50
        and candle["high"] >= e20 * 0.997
        and price < e20
        and candle["close"] < candle["open"]
    )

    add_long(breakout_long, 12, "اختراق مقاومة")
    add_short(breakout_short, 12, "كسر دعم")
    add_long(pullback_long, 10, "ارتداد من EMA20")
    add_short(pullback_short, 10, "رفض من EMA20")

    # تأكيد الفريمات
    add_long(confirm["bullish"], 8, f"تأكيد {CONFIRM_TIMEFRAME}")
    add_short(confirm["bearish"], 8, f"تأكيد {CONFIRM_TIMEFRAME}")

    add_long(trend["bullish"], 8, f"اتجاه {TREND_TIMEFRAME} صاعد")
    add_short(trend["bearish"], 8, f"اتجاه {TREND_TIMEFRAME} هابط")

    # فلتر بيتكوين
    add_long(btc_filter["bullish"], 5, "بيتكوين داعم")
    if btc_filter["bearish"]:
        long_score -= 7
        warnings.append("بيتكوين ضاغط على السوق")

    # تكيف مع حالة السوق
    if regime == "TRENDING":
        long_score += 3
        short_score += 3
    elif regime == "RANGING":
        if breakout_long:
            long_score -= 5
            warnings.append("السوق عرضي: احتمال اختراق وهمي")
        if breakout_short:
            short_score -= 5
            warnings.append("السوق عرضي: احتمال كسر وهمي")

    # عقوبات المطاردة والاختراق الوهمي
    if distance_ema20 > 2.2:
        long_score -= 7
        short_score -= 7
        warnings.append("السعر بعيد عن EMA20")

    if upper_wick_ratio > 0.45:
        long_score -= 7
        warnings.append("ذيل علوي كبير")

    if lower_wick_ratio > 0.45:
        short_score -= 7
        warnings.append("ذيل سفلي كبير")

    if price > bb_upper and rsi_now > 72:
        long_score -= 6
        warnings.append("تشبع شرائي")

    if price < bb_lower and rsi_now < 28:
        short_score -= 6
        warnings.append("تشبع بيعي")

    if avg_quote_volume < MIN_QUOTE_VOLUME:
        return None

    # التعلم البسيط من النتائج المخزنة
    long_score += adaptive_bonus(state, "LONG")
    short_score += adaptive_bonus(state, "SHORT")

    # سبوت فقط: لا نرسل إشارات شورت.
    side = "LONG"
    score = long_score
    reasons = long_reasons

    if side == "LONG":
        stop = min(
            support,
            price - 1.25 * current_atr
        )
        risk = price - stop
        target1 = price + 1.5 * risk
        target2 = price + 2.2 * risk
        target3 = price + 3.0 * risk
    else:
        stop = max(
            resistance,
            price + 1.25 * current_atr
        )
        risk = stop - price
        target1 = price - 1.5 * risk
        target2 = price - 2.2 * risk
        target3 = price - 3.0 * risk

    if risk <= 0:
        return None

    risk_pct = risk / price * 100
    rr = 1.5

    if risk_pct > MAX_RISK_PCT or rr < MIN_RR:
        return None

    signal_type = "ENTRY" if score >= MIN_SCORE else "WATCH"
    if score < WATCH_SCORE:
        return None

    confidence = min(95, max(50, score))
    setup = "اختراق" if (breakout_long if side == "LONG" else breakout_short) else "ارتداد"

    return {
        "exchange": exchange,
        "symbol": symbol,
        "side": side,
        "score": score,
        "confidence": confidence,
        "signal_type": signal_type,
        "setup": setup,
        "entry": price,
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "target3": target3,
        "risk_pct": risk_pct,
        "rsi": rsi_now,
        "adx": adx_now,
        "volume_ratio": volume_ratio,
        "atr_pct": atr_pct,
        "regime": regime,
        "reasons": reasons[:7],
        "warnings": list(dict.fromkeys(warnings))[:3],
        "candle_close": candle["close_time"],
    }


# =========================
# التنسيق
# =========================
def fmt(value: float) -> str:
    if value >= 1000:
        digits = 2
    elif value >= 1:
        digits = 4
    elif value >= 0.01:
        digits = 5
    else:
        digits = 7
    return f"{value:.{digits}f}"


def signal_message(result: Dict) -> str:
    type_ar = "🟢 إشارة دخول" if result["signal_type"] == "ENTRY" else "🟡 تحت المراقبة"
    reasons = "\n".join(f"• {reason}" for reason in result["reasons"])
    warnings = ""
    if result["warnings"]:
        warnings = "\n\nتحذيرات:\n" + "\n".join(
            f"• {warning}" for warning in result["warnings"]
        )

    header = (
        f"{type_ar} — {result['symbol']}\n"
        f"المنصة: {result['exchange']} SPOT\n\n"
        f"الاتجاه: شراء سبوت\n"
        f"النموذج: {result['setup']}\n"
        f"الثقة الذكية: {result['confidence']}%\n"
        f"التقييم: {result['score']}\n"
        f"الفريم: {ENTRY_TIMEFRAME}\n"
        f"حالة السوق: {result['regime']}\n\n"
    )

    metrics = (
        f"RSI: {result['rsi']:.1f}\n"
        f"ADX: {result['adx']:.1f}\n"
        f"الفوليوم: ×{result['volume_ratio']:.1f}\n"
        f"ATR: {result['atr_pct']:.2f}%\n\n"
        f"الأسباب:\n{reasons}{warnings}"
    )

    if result["signal_type"] == "WATCH":
        return (
            header + metrics +
            "\n\n⏳ انتظر تأكيد الدخول، لا تدخل الآن."
            "\n\n⚠️ تحليل فني آلي وليس ضمانًا للربح."
        )

    levels = (
        f"الدخول التقريبي: {fmt(result['entry'])}\n"
        f"وقف الخسارة: {fmt(result['stop'])} ({result['risk_pct']:.2f}%)\n"
        f"الهدف الأول: {fmt(result['target1'])}\n"
        f"الهدف الثاني: {fmt(result['target2'])}\n"
        f"الهدف الثالث: {fmt(result['target3'])}\n\n"
    )
    return (
        header + levels + metrics +
        "\n\n⚠️ تحليل فني آلي وليس ضمانًا للربح. استخدم وقف الخسارة."
    )


# =========================
# الفحص الدوري
# =========================
def is_cooled(state: Dict, result: Dict) -> bool:
    key = f"{result['exchange']}:{result['symbol']}:{result['side']}:{result['signal_type']}"
    previous = int(state.get("alerts", {}).get(key, 0))
    elapsed = result["candle_close"] - previous
    return elapsed >= COOLDOWN_MINUTES * 60 * 1000


def remember_alert(state: Dict, result: Dict) -> None:
    key = f"{result['exchange']}:{result['symbol']}:{result['side']}:{result['signal_type']}"
    state.setdefault("alerts", {})[key] = result["candle_close"]


def scan(state: Dict) -> None:
    btc_candles = get_klines("BINANCE", "BTCUSDT", CONFIRM_TIMEFRAME)
    btc_filter = trend_snapshot(btc_candles)
    symbols = discover_symbols()

    results: List[Dict] = []
    for item in symbols:
        exchange = item["exchange"]
        symbol = item["symbol"]
        try:
            result = analyze_symbol(exchange, symbol, state, btc_filter)
            if result and is_cooled(state, result):
                results.append(result)
            time.sleep(0.12)
        except Exception as exc:
            print(f"{exchange}:{symbol}: {exc}", flush=True)

    results.sort(
        key=lambda item: (
            item["signal_type"] == "ENTRY",
            item["score"],
            item["volume_ratio"],
        ),
        reverse=True,
    )

    sent = 0
    for result in results:
        if sent >= MAX_ALERTS_PER_SCAN:
            break
        send_message(signal_message(result))
        remember_alert(state, result)
        sent += 1
        time.sleep(0.3)

    state["scan_count"] = int(state.get("scan_count", 0)) + 1
    if STATUS_EVERY_SCANS > 0 and state["scan_count"] % STATUS_EVERY_SCANS == 0:
        send_message(
            "🤖 البوت يعمل ويواصل الفحص.\n"
            f"عدد الأزواج المفحوصة: {len(symbols)}\n"
            "الأسواق: Binance Spot + Bybit Spot\n"
            "التركيز: العملات الصغيرة والمتوسطة فقط\n"
            f"الفريمات: {ENTRY_TIMEFRAME} / {CONFIRM_TIMEFRAME} / {TREND_TIMEFRAME}\n"
            f"آخر فحص: تم إرسال {sent} تنبيه."
        )

    save_state(state)
    print(
        f"Scan finished | symbols={len(symbols)} | candidates={len(results)} | sent={sent}",
        flush=True,
    )


def main() -> None:
    state = load_state()

    send_message(
        "✅ تم تشغيل بوت صيد العملات الصغيرة والمتوسطة.\n"
        "الأسواق: Binance Spot + Bybit Spot\n"
        "النوع: شراء سبوت فقط — بدون شورت أو رافعة\n"
        f"فريم الدخول: {ENTRY_TIMEFRAME}\n"
        f"التأكيد: {CONFIRM_TIMEFRAME} و{TREND_TIMEFRAME}\n"
        "يستبعد العملات الكبيرة والمستقرة والعملات ضعيفة السيولة، "
        "ويركز على الفوليوم والزخم والاختراقات والارتدادات."
    )

    while True:
        try:
            scan(state)
        except Exception as exc:
            print(f"Scan error: {exc}", flush=True)

        time.sleep(SCAN_MINUTES * 60)


if __name__ == "__main__":
    main()
