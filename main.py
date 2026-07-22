import os
import time
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
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
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

ENTRY_TIMEFRAME = os.getenv("ENTRY_TIMEFRAME", "5m")
CONFIRM_TIMEFRAME = os.getenv("CONFIRM_TIMEFRAME", "15m")
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")

SCAN_MINUTES = int(os.getenv("SCAN_MINUTES", "1"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "68"))
WATCH_SCORE = int(os.getenv("WATCH_SCORE", "58"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "120"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
STATUS_EVERY_SCANS = int(os.getenv("STATUS_EVERY_SCANS", "20"))
MIN_RR = float(os.getenv("MIN_RR", "1.5"))
MAX_RISK_PCT = float(os.getenv("MAX_RISK_PCT", "3.5"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME", "500000"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "120"))
SYMBOL_REFRESH_MINUTES = int(os.getenv("SYMBOL_REFRESH_MINUTES", "30"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
MAX_PRE_PUMP_PCT = float(os.getenv("MAX_PRE_PUMP_PCT", "18"))

EXCLUDED_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "AVAX",
    "LINK", "DOT", "LTC", "BCH", "SUI", "TON", "SHIB", "PEPE",
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "TRY", "BRL"
}

LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
SYMBOL_CACHE: Dict[str, object] = {"symbols": [], "updated_at": 0.0}


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


def get_klines(symbol: str, interval: str, limit: int = 260) -> List[Dict]:
    response = SESSION.get(
        f"{BINANCE_BASE}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=20,
    )
    response.raise_for_status()

    candles: List[Dict] = []
    for x in response.json():
        candles.append({
            "open": float(x[1]),
            "high": float(x[2]),
            "low": float(x[3]),
            "close": float(x[4]),
            "volume": float(x[5]),
            "close_time": int(x[6]),
            "quote_volume": float(x[7]),
        })
    return candles


def get_dynamic_symbols() -> List[str]:
    now = time.time()
    cached = SYMBOL_CACHE.get("symbols", [])
    updated_at = float(SYMBOL_CACHE.get("updated_at", 0.0))
    if cached and now - updated_at < SYMBOL_REFRESH_MINUTES * 60:
        return list(cached)

    exchange = SESSION.get(f"{BINANCE_BASE}/api/v3/exchangeInfo", timeout=25)
    exchange.raise_for_status()
    ticker = SESSION.get(f"{BINANCE_BASE}/api/v3/ticker/24hr", timeout=25)
    ticker.raise_for_status()

    quote_volume = {
        item.get("symbol", ""): float(item.get("quoteVolume", 0.0) or 0.0)
        for item in ticker.json()
    }

    candidates: List[Tuple[str, float]] = []
    for item in exchange.json().get("symbols", []):
        symbol = item.get("symbol", "")
        base = item.get("baseAsset", "")
        quote = item.get("quoteAsset", "")

        if item.get("status") != "TRADING" or quote != "USDT":
            continue
        if not item.get("isSpotTradingAllowed", True):
            continue
        if base in EXCLUDED_BASES:
            continue
        if base.endswith(LEVERAGED_SUFFIXES):
            continue

        volume = quote_volume.get(symbol, 0.0)
        if volume < MIN_QUOTE_VOLUME:
            continue
        candidates.append((symbol, volume))

    candidates.sort(key=lambda x: x[1], reverse=True)
    symbols = [symbol for symbol, _ in candidates[:MAX_SYMBOLS]]
    SYMBOL_CACHE["symbols"] = symbols
    SYMBOL_CACHE["updated_at"] = now
    return symbols


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


def macd(values: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    # MACD يحتاج 26 شمعة على الأقل، وخط الإشارة يحتاج 9 قيم MACD إضافية.
    if len(values) < 35:
        return None, None, None

    fast = ema(values, 12)
    slow = ema(values, 26)

    series: List[float] = []
    for fast_value, slow_value in zip(fast, slow):
        if fast_value is not None and slow_value is not None:
            series.append(fast_value - slow_value)

    if len(series) < 9:
        return None, None, None

    signal = ema(series, 9)
    macd_now = series[-1]
    signal_now = signal[-1]
    if signal_now is None:
        return None, None, None

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

    # العملات الحديثة قد لا تملك 200 شمعة على فريم الساعة.
    # في هذه الحالة نعيد اتجاهًا محايدًا بدل حدوث خطأ NoneType.
    if len(closes) < 200:
        return {"bullish": False, "bearish": False, "neutral": True, "rsi": None}

    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1]
    rsi_now = rsi(closes)[-1]
    _, _, histogram = macd(closes)

    required = (e20, e50, e200, rsi_now, histogram)
    if any(value is None for value in required):
        return {"bullish": False, "bearish": False, "neutral": True, "rsi": rsi_now}

    price = closes[-1]
    bullish = price > e20 > e50 and price > e200 and histogram > 0
    bearish = price < e20 < e50 and price < e200 and histogram < 0

    return {
        "bullish": bullish,
        "bearish": bearish,
        "neutral": not bullish and not bearish,
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
def analyze_symbol(symbol: str, state: Dict, btc_filter: Dict) -> Optional[Dict]:
    entry_candles = get_klines(symbol, ENTRY_TIMEFRAME)
    confirm_candles = get_klines(symbol, CONFIRM_TIMEFRAME)
    trend_candles = get_klines(symbol, TREND_TIMEFRAME)

    closed = entry_candles[:-1]
    closes = [c["close"] for c in closed]
    volumes = [c["volume"] for c in closed]
    quote_volumes = [c["quote_volume"] for c in closed]

    # تجاهل العملات الجديدة أو البيانات غير المكتملة بأمان.
    if len(closed) < 60 or len(confirm_candles) < 35 or len(trend_candles) < 35:
        return None

    price = closes[-1]
    candle = closed[-1]

    e9 = ema(closes, 9)[-1]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]

    rsi_values = rsi(closes)
    rsi_now = rsi_values[-1]
    rsi_prev = rsi_values[-2]

    macd_now, macd_signal, macd_hist = macd(closes)

    required_indicators = (e9, e20, e50, rsi_now, rsi_prev, macd_now, macd_signal, macd_hist)
    if any(value is None for value in required_indicators):
        return None

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
    lookback_price = closes[-13] if len(closes) >= 13 else closes[0]
    recent_change_pct = ((price / lookback_price) - 1) * 100 if lookback_price else 0.0

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
    if symbol != "BTCUSDT":
        add_long(btc_filter["bullish"], 5, "بيتكوين داعم")
        add_short(btc_filter["bearish"], 5, "بيتكوين ضاغط")

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

    if recent_change_pct >= MAX_PRE_PUMP_PCT:
        long_score -= 20
        warnings.append(f"ارتفاع سابق {recent_change_pct:.1f}%: احتمال دخول متأخر")

    if volume_ratio < 1.10:
        long_score -= 8
        short_score -= 8

    if avg_quote_volume < MIN_QUOTE_VOLUME:
        return None

    # التعلم البسيط من النتائج المخزنة
    long_score += adaptive_bonus(state, "LONG")
    short_score += adaptive_bonus(state, "SHORT")

    side = "LONG" if long_score >= short_score else "SHORT"
    score = max(long_score, short_score)
    reasons = long_reasons if side == "LONG" else short_reasons

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
        "recent_change_pct": recent_change_pct,
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
    side_icon = "🟢" if result["side"] == "LONG" else "🔴"
    side_ar = "لونغ" if result["side"] == "LONG" else "شورت"
    reasons = "\n".join(f"• {reason}" for reason in result["reasons"])

    warnings = ""
    if result["warnings"]:
        warnings = "\n\nتحذيرات:\n" + "\n".join(
            f"• {warning}" for warning in result["warnings"]
        )

    header = (
        f"{side_icon} {'إشارة دخول' if result['signal_type'] == 'ENTRY' else 'تحت المراقبة'} — {result['symbol']}\n\n"
        f"الاتجاه: {side_ar}\n"
        f"النموذج: {result['setup']}\n"
        f"الثقة الذكية: {result['confidence']}%\n"
        f"التقييم: {result['score']}\n"
        f"الفريم: {ENTRY_TIMEFRAME}\n"
        f"حالة السوق: {result['regime']}\n"
        f"الفوليوم: ×{result['volume_ratio']:.1f}\n"
        f"الحركة الأخيرة: {result['recent_change_pct']:.1f}%\n"
    )

    if result["signal_type"] == "WATCH":
        return (
            header
            + f"\nالأسباب:\n{reasons}"
            + warnings
            + "\n\n⏳ مراقبة فقط، لا يوجد دخول مؤكد حتى الآن."
        )

    return (
        header
        + f"\nالدخول التقريبي: {fmt(result['entry'])}\n"
        + f"وقف الخسارة: {fmt(result['stop'])} ({result['risk_pct']:.2f}%)\n"
        + f"الهدف الأول: {fmt(result['target1'])}\n"
        + f"الهدف الثاني: {fmt(result['target2'])}\n"
        + f"الهدف الثالث: {fmt(result['target3'])}\n\n"
        + f"RSI: {result['rsi']:.1f}\n"
        + f"ADX: {result['adx']:.1f}\n"
        + f"ATR: {result['atr_pct']:.2f}%\n\n"
        + f"الأسباب:\n{reasons}"
        + warnings
        + "\n\n⚠️ تحليل فني آلي وليس ضمانًا للربح. استخدم وقف الخسارة."
    )


# =========================
# الفحص الدوري
# =========================
def is_cooled(state: Dict, result: Dict) -> bool:
    key = f"{result['symbol']}:{result['side']}:{result['signal_type']}"
    previous = int(state.get("alerts", {}).get(key, 0))
    elapsed = result["candle_close"] - previous
    return elapsed >= COOLDOWN_MINUTES * 60 * 1000


def remember_alert(state: Dict, result: Dict) -> None:
    key = f"{result['symbol']}:{result['side']}:{result['signal_type']}"
    state.setdefault("alerts", {})[key] = result["candle_close"]


def scan(state: Dict) -> None:
    btc_candles = get_klines("BTCUSDT", CONFIRM_TIMEFRAME)
    btc_filter = trend_snapshot(btc_candles)

    results: List[Dict] = []
    symbols = get_dynamic_symbols()

    def worker(symbol: str) -> Optional[Dict]:
        try:
            return analyze_symbol(symbol, state, btc_filter)
        except Exception as exc:
            print(f"{symbol}: {exc}", flush=True)
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            result = future.result()
            if result and is_cooled(state, result):
                results.append(result)

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
            f"عدد العملات: {len(symbols)}\n"
            f"الفريمات: {ENTRY_TIMEFRAME} / {CONFIRM_TIMEFRAME} / {TREND_TIMEFRAME}\n"
            f"آخر فحص: تم العثور على {sent} تنبيه."
        )

    save_state(state)
    print(
        f"Scan finished | candidates={len(results)} | sent={sent}",
        flush=True,
    )


def main() -> None:
    state = load_state()

    send_message(
        "✅ تم تشغيل بوت المضاربة الذكي V2.1.\n"
        f"فريم الدخول: {ENTRY_TIMEFRAME}\n"
        f"التأكيد: {CONFIRM_TIMEFRAME} و{TREND_TIMEFRAME}\n"
        "المميزات: اكتشاف تلقائي لعملات Binance Spot، استبعاد العملات الكبرى، "
        "لونغ وشورت، اختراق وارتداد، فلتر بيتكوين، "
        "فوليوم، MACD، ADX، VWAP، OBV، ATR، Bollinger، "
        "وفلترة الاختراق الوهمي."
    )

    while True:
        try:
            scan(state)
        except Exception as exc:
            print(f"Scan error: {exc}", flush=True)

        time.sleep(SCAN_MINUTES * 60)


if __name__ == "__main__":
    main()
