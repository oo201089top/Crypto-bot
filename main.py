import json
import math
import os
import time
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple

import requests

BINANCE_BASE = "https://api.binance.com"
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# إعدادات قابلة للتغيير من Railway Variables
SCAN_MINUTES = int(os.getenv("SCAN_MINUTES", "5"))
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", "6"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "78"))
PRE_BREAKOUT_SCORE = int(os.getenv("PRE_BREAKOUT_SCORE", "72"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "5000000"))

DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "LINKUSDT", "AVAXUSDT", "RIFUSDT",
]
SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")
    if s.strip()
]

STATE_FILE = Path("signal_state.json")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-breakout-alert-bot/2.0"})


def http_get(path: str, params: Optional[dict] = None):
    response = SESSION.get(f"{BINANCE_BASE}{path}", params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def get_klines(symbol: str, interval: str, limit: int = 260) -> List[dict]:
    rows = http_get(
        "/api/v3/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    return [
        {
            "open": float(x[1]),
            "high": float(x[2]),
            "low": float(x[3]),
            "close": float(x[4]),
            "volume": float(x[5]),
            "close_time": int(x[6]),
            "quote_volume": float(x[7]),
            "trades": int(x[8]),
            "taker_buy_base": float(x[9]),
        }
        for x in rows
    ]


def get_24h(symbol: str) -> dict:
    return http_get("/api/v3/ticker/24hr", {"symbol": symbol})


def sma(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0:
        return out
    running = 0.0
    for i, value in enumerate(values):
        running += value
        if i >= period:
            running -= values[i - period]
        if i >= period - 1:
            out[i] = running / period
    return out


def ema(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period:
        return [None] * len(values)
    k = 2 / (period + 1)
    out: List[Optional[float]] = [None] * (period - 1)
    current = mean(values[:period])
    out.append(current)
    for value in values[period:]:
        current = (value - current) * k + current
        out.append(current)
    return out


def rsi(values: List[float], period: int = 14) -> List[Optional[float]]:
    if len(values) <= period:
        return [None] * len(values)
    gains, losses = [], []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    result: List[Optional[float]] = [None] * period
    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])

    def calc(gain: float, loss: float) -> float:
        if loss == 0:
            return 100.0
        rs = gain / loss
        return 100 - 100 / (1 + rs)

    result.append(calc(avg_gain, avg_loss))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        result.append(calc(avg_gain, avg_loss))
    return result


def true_ranges(candles: List[dict]) -> List[float]:
    out = []
    for i, candle in enumerate(candles):
        if i == 0:
            out.append(candle["high"] - candle["low"])
        else:
            prev_close = candles[i - 1]["close"]
            out.append(max(
                candle["high"] - candle["low"],
                abs(candle["high"] - prev_close),
                abs(candle["low"] - prev_close),
            ))
    return out


def atr(candles: List[dict], period: int = 14) -> float:
    tr = true_ranges(candles)
    if len(tr) < period:
        return mean(tr)
    return mean(tr[-period:])


def macd(values: List[float]) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    fast = ema(values, 12)
    slow = ema(values, 26)
    macd_line: List[Optional[float]] = []
    for f, s in zip(fast, slow):
        macd_line.append(None if f is None or s is None else f - s)

    clean = [x for x in macd_line if x is not None]
    signal_clean = ema(clean, 9)
    signal: List[Optional[float]] = [None] * (len(macd_line) - len(signal_clean)) + signal_clean
    hist = [
        None if m is None or s is None else m - s
        for m, s in zip(macd_line, signal)
    ]
    return macd_line, signal, hist


def bollinger(values: List[float], period: int = 20, mult: float = 2.0):
    middle = sma(values, period)
    upper: List[Optional[float]] = [None] * len(values)
    lower: List[Optional[float]] = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        sd = pstdev(window)
        upper[i] = middle[i] + mult * sd if middle[i] is not None else None
        lower[i] = middle[i] - mult * sd if middle[i] is not None else None
    return middle, upper, lower


def adx(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period * 2 + 2:
        return None

    plus_dm, minus_dm, tr = [], [], []
    for i in range(1, len(candles)):
        up_move = candles[i]["high"] - candles[i - 1]["high"]
        down_move = candles[i - 1]["low"] - candles[i]["low"]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        tr.append(max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i - 1]["close"]),
            abs(candles[i]["low"] - candles[i - 1]["close"]),
        ))

    dx_values = []
    for i in range(period - 1, len(tr)):
        tr_sum = sum(tr[i - period + 1:i + 1])
        if tr_sum == 0:
            continue
        plus_di = 100 * sum(plus_dm[i - period + 1:i + 1]) / tr_sum
        minus_di = 100 * sum(minus_dm[i - period + 1:i + 1]) / tr_sum
        denom = plus_di + minus_di
        if denom:
            dx_values.append(100 * abs(plus_di - minus_di) / denom)

    if len(dx_values) < period:
        return None
    return mean(dx_values[-period:])


def obv(candles: List[dict]) -> List[float]:
    values = [0.0]
    for i in range(1, len(candles)):
        if candles[i]["close"] > candles[i - 1]["close"]:
            values.append(values[-1] + candles[i]["volume"])
        elif candles[i]["close"] < candles[i - 1]["close"]:
            values.append(values[-1] - candles[i]["volume"])
        else:
            values.append(values[-1])
    return values


def rolling_vwap(candles: List[dict], period: int = 20) -> float:
    window = candles[-period:]
    pv = 0.0
    volume = 0.0
    for candle in window:
        typical = (candle["high"] + candle["low"] + candle["close"]) / 3
        pv += typical * candle["volume"]
        volume += candle["volume"]
    return pv / volume if volume else window[-1]["close"]


def stochastic_rsi(rsi_values: List[Optional[float]], period: int = 14) -> Optional[float]:
    clean = [x for x in rsi_values if x is not None]
    if len(clean) < period:
        return None
    window = clean[-period:]
    low, high = min(window), max(window)
    if high == low:
        return 50.0
    return 100 * (window[-1] - low) / (high - low)


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


def send_message(text: str):
    response = SESSION.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text},
        timeout=20,
    )
    response.raise_for_status()


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False))


def trend_snapshot(symbol: str, interval: str) -> dict:
    candles = get_klines(symbol, interval, 230)[:-1]
    closes = [c["close"] for c in candles]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1]
    price = closes[-1]
    return {
        "price": price,
        "bullish": bool(e20 and e50 and e200 and price > e20 > e50 > e200),
        "above_200": bool(e200 and price > e200),
    }


def btc_market_filter() -> dict:
    try:
        one_hour = trend_snapshot("BTCUSDT", "1h")
        four_hour = trend_snapshot("BTCUSDT", "4h")
        healthy = one_hour["above_200"] and four_hour["above_200"]
        strong = one_hour["bullish"] and four_hour["bullish"]
        return {"healthy": healthy, "strong": strong}
    except Exception as exc:
        print(f"BTC filter error: {exc}", flush=True)
        return {"healthy": True, "strong": False}


def analyze(symbol: str, btc_filter: dict) -> Optional[dict]:
    candles15 = get_klines(symbol, "15m", 260)[:-1]
    candles1h = get_klines(symbol, "1h", 260)[:-1]
    candles4h = get_klines(symbol, "4h", 230)[:-1]

    if len(candles15) < 220 or len(candles1h) < 220 or len(candles4h) < 205:
        return None

    ticker = get_24h(symbol)
    quote_volume_24h = float(ticker.get("quoteVolume", 0))
    if quote_volume_24h < MIN_QUOTE_VOLUME_USDT:
        return None

    closes = [c["close"] for c in candles15]
    volumes = [c["volume"] for c in candles15]
    price = closes[-1]
    previous_close = closes[-2]

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    rsis = rsi(closes, 14)
    macd_line, macd_signal, macd_hist = macd(closes)
    bb_mid, bb_upper, _ = bollinger(closes)

    ema20, ema50, ema200 = e20[-1], e50[-1], e200[-1]
    rsi_now, rsi_prev = rsis[-1], rsis[-2]
    macd_now, signal_now = macd_line[-1], macd_signal[-1]
    hist_now, hist_prev = macd_hist[-1], macd_hist[-2]
    upper_now = bb_upper[-1]

    resistance = max(c["high"] for c in candles15[-25:-1])
    support = min(c["low"] for c in candles15[-12:-1])
    avg_volume = mean(volumes[-21:-1])
    volume_ratio = volumes[-1] / avg_volume if avg_volume else 0.0

    candle = candles15[-1]
    candle_range = max(candle["high"] - candle["low"], 1e-12)
    body_ratio = abs(candle["close"] - candle["open"]) / candle_range
    upper_wick_ratio = (candle["high"] - max(candle["open"], candle["close"])) / candle_range
    close_position = (candle["close"] - candle["low"]) / candle_range

    vwap20 = rolling_vwap(candles15, 20)
    adx_now = adx(candles15, 14)
    obv_values = obv(candles15)
    obv_ma = mean(obv_values[-20:])
    stoch_rsi_now = stochastic_rsi(rsis)

    trend1h = trend_snapshot(symbol, "1h")
    trend4h = trend_snapshot(symbol, "4h")

    confirmed_breakout = (
        price > resistance
        and previous_close <= resistance
        and volume_ratio >= 1.6
        and body_ratio >= 0.55
        and close_position >= 0.72
        and upper_wick_ratio <= 0.25
    )
    pre_breakout = (
        price <= resistance
        and price >= resistance * 0.992
        and volume_ratio >= 1.25
        and ema20 is not None
        and price > ema20
    )

    score = 0
    reasons = []

    def add(condition: bool, points: int, reason: str):
        nonlocal score
        if condition:
            score += points
            reasons.append(reason)

    add(bool(ema20 and ema50 and ema200 and price > ema20 > ema50 > ema200), 14, "ترتيب EMA صاعد على 15 دقيقة")
    add(trend1h["above_200"], 8, "اتجاه الساعة فوق EMA200")
    add(trend4h["above_200"], 8, "اتجاه 4 ساعات فوق EMA200")
    add(trend1h["bullish"], 5, "اتجاه الساعة صاعد بالكامل")
    add(48 <= rsi_now <= 68 and rsi_now > rsi_prev, 8, f"RSI مناسب وصاعد ({rsi_now:.1f})")
    add(bool(macd_now is not None and signal_now is not None and macd_now > signal_now), 7, "MACD إيجابي")
    add(bool(hist_now is not None and hist_prev is not None and hist_now > hist_prev), 4, "زخم MACD يتسارع")
    add(bool(adx_now is not None and adx_now >= 23), 8, f"ADX يؤكد قوة الاتجاه ({adx_now:.1f})")
    add(volume_ratio >= 1.6, 12, f"فوليوم الاختراق ×{volume_ratio:.1f}")
    add(price > vwap20, 5, "السعر فوق VWAP")
    add(obv_values[-1] > obv_ma, 5, "OBV يؤكد دخول سيولة")
    add(bool(upper_now is not None and price > upper_now), 4, "اختراق الحد العلوي لبولينجر")
    add(confirmed_breakout, 12, "إغلاق اختراق حقيقي فوق المقاومة")
    add(body_ratio >= 0.55 and upper_wick_ratio <= 0.25, 5, "شمعة قوية بدون ذيل علوي مزعج")
    add(btc_filter["healthy"] or symbol == "BTCUSDT", 5, "سوق بيتكوين غير سلبي")

    # خصومات تقلل الاختراقات الوهمية والمطاردة بعد الارتفاع
    penalties = []
    if rsi_now is not None and rsi_now > 74:
        score -= 12
        penalties.append("RSI متشبع")
    if stoch_rsi_now is not None and stoch_rsi_now > 92:
        score -= 6
        penalties.append("Stoch RSI مرتفع جدًا")
    if upper_wick_ratio > 0.35:
        score -= 10
        penalties.append("ذيل علوي كبير")
    if price > resistance * 1.035:
        score -= 8
        penalties.append("السعر ابتعد عن نقطة الاختراق")
    if not btc_filter["healthy"] and symbol != "BTCUSDT":
        score -= 10
        penalties.append("اتجاه بيتكوين غير داعم")

    score = max(0, min(100, score))
    current_atr = atr(candles15, 14)
    stop = max(support, price - 1.35 * current_atr)
    if stop >= price:
        stop = price - 1.35 * current_atr
    risk = price - stop
    if risk <= 0:
        return None

    signal_type = "breakout" if confirmed_breakout else "watch" if pre_breakout else "none"
    if signal_type == "none":
        return None

    return {
        "symbol": symbol,
        "signal_type": signal_type,
        "score": score,
        "entry": price,
        "entry_low": max(resistance, price - 0.25 * current_atr) if confirmed_breakout else resistance * 0.997,
        "entry_high": price + 0.20 * current_atr if confirmed_breakout else resistance * 1.003,
        "stop": stop,
        "target1": price + 1.5 * risk,
        "target2": price + 2.3 * risk,
        "target3": price + 3.2 * risk,
        "resistance": resistance,
        "rsi": rsi_now,
        "stoch_rsi": stoch_rsi_now,
        "adx": adx_now,
        "volume_ratio": volume_ratio,
        "quote_volume_24h": quote_volume_24h,
        "reasons": reasons,
        "penalties": penalties,
        "candle_close": candle["close_time"],
    }


def build_message(a: dict) -> str:
    risk_pct = (a["entry"] - a["stop"]) / a["entry"] * 100
    reasons = "\n".join(f"• {x}" for x in a["reasons"][:8])
    warnings = ""
    if a["penalties"]:
        warnings = "\nملاحظات:\n" + "\n".join(f"• {x}" for x in a["penalties"])

    if a["signal_type"] == "breakout":
        header = "🚀 اختراق مؤكد"
        action = "منطقة دخول بعد ثبات السعر"
    else:
        header = "👀 استعداد قبل الاختراق"
        action = "راقب الإغلاق ولا تدخل قبل التأكيد"

    return (
        f"{header} — {a['symbol']}\n\n"
        f"التقييم الفني: {a['score']}/100\n"
        f"{action}: {fmt(a['entry_low'])} — {fmt(a['entry_high'])}\n"
        f"المقاومة: {fmt(a['resistance'])}\n"
        f"وقف الخسارة: {fmt(a['stop'])} ({risk_pct:.1f}%)\n"
        f"الهدف 1: {fmt(a['target1'])}\n"
        f"الهدف 2: {fmt(a['target2'])}\n"
        f"الهدف 3: {fmt(a['target3'])}\n\n"
        f"RSI: {a['rsi']:.1f}\n"
        f"ADX: {a['adx']:.1f}\n" if a["adx"] is not None else ""
    ) + (
        f"الفوليوم: ×{a['volume_ratio']:.1f}\n"
        f"حجم 24 ساعة: ${a['quote_volume_24h']:,.0f}\n\n"
        f"الأسباب:\n{reasons}{warnings}\n\n"
        "⚠️ إشارة فنية وليست ضمانًا. لا تطارد شمعة مرتفعة، والتزم بوقف الخسارة."
    )


def scan():
    state = load_state()
    alerts = []
    btc_filter = btc_market_filter()

    for symbol in SYMBOLS:
        try:
            result = analyze(symbol, btc_filter)
            if not result:
                time.sleep(0.25)
                continue

            state_key = f"{symbol}:{result['signal_type']}"
            last = int(state.get(state_key, 0))
            cooled = result["candle_close"] - last >= COOLDOWN_HOURS * 3600000

            threshold = MIN_SCORE if result["signal_type"] == "breakout" else PRE_BREAKOUT_SCORE
            if result["score"] >= threshold and cooled:
                alerts.append(result)
                state[state_key] = result["candle_close"]

            time.sleep(0.25)
        except Exception as exc:
            print(f"{symbol}: {exc}", flush=True)

    alerts.sort(key=lambda x: (x["signal_type"] == "breakout", x["score"]), reverse=True)
    for alert in alerts[:MAX_ALERTS_PER_SCAN]:
        send_message(build_message(alert))

    save_state(state)
    print(f"Scan finished. Alerts: {len(alerts[:MAX_ALERTS_PER_SCAN])}", flush=True)


if __name__ == "__main__":
    send_message(
        "✅ تم تشغيل بوت الاختراقات الفنية.\n"
        "يراقب 15 دقيقة مع تأكيد اتجاه الساعة و4 ساعات وفوليوم وMACD وADX وVWAP وOBV."
    )
    while True:
        try:
            scan()
        except Exception as exc:
            print(f"Scan error: {exc}", flush=True)
        time.sleep(SCAN_MINUTES * 60)
