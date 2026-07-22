import os
import time
import json
from pathlib import Path
from statistics import mean
import requests

BINANCE_BASE = "https://api.binance.com"
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

TIMEFRAME = os.getenv("TIMEFRAME", "1h")
MIN_SCORE = int(os.getenv("MIN_SCORE", "85"))
SCAN_MINUTES = int(os.getenv("SCAN_MINUTES", "10"))
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", "12"))

DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "LINKUSDT", "AVAXUSDT", "RIFUSDT"
]
SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")
    if s.strip()
]

STATE_FILE = Path("signal_state.json")


def get_klines(symbol: str, limit: int = 220):
    r = requests.get(
        f"{BINANCE_BASE}/api/v3/klines",
        params={"symbol": symbol, "interval": TIMEFRAME, "limit": limit},
        timeout=20,
    )
    r.raise_for_status()
    return [
        {
            "high": float(x[2]),
            "low": float(x[3]),
            "close": float(x[4]),
            "volume": float(x[5]),
            "close_time": int(x[6]),
        }
        for x in r.json()
    ]


def ema(values, period):
    if len(values) < period:
        return [None] * len(values)
    k = 2 / (period + 1)
    out = [None] * (period - 1)
    current = mean(values[:period])
    out.append(current)
    for value in values[period:]:
        current = (value - current) * k + current
        out.append(current)
    return out


def rsi(values, period=14):
    if len(values) <= period:
        return [None] * len(values)
    gains, losses = [], []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    result = [None] * period
    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])

    def value(gain, loss):
        if loss == 0:
            return 100.0
        rs = gain / loss
        return 100 - 100 / (1 + rs)

    result.append(value(avg_gain, avg_loss))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        result.append(value(avg_gain, avg_loss))
    return result


def atr(candles, period=14):
    tr = []
    for i, candle in enumerate(candles):
        if i == 0:
            tr.append(candle["high"] - candle["low"])
        else:
            prev = candles[i - 1]["close"]
            tr.append(max(
                candle["high"] - candle["low"],
                abs(candle["high"] - prev),
                abs(candle["low"] - prev),
            ))
    return mean(tr[-period:])


def analyze(symbol, candles):
    closed = candles[:-1]
    closes = [x["close"] for x in closed]
    volumes = [x["volume"] for x in closed]

    e20, e50, e200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)
    rsis = rsi(closes)

    price = closes[-1]
    ema20, ema50, ema200 = e20[-1], e50[-1], e200[-1]
    rsi_now, rsi_prev = rsis[-1], rsis[-2]

    avg_volume = mean(volumes[-21:-1])
    volume_ratio = volumes[-1] / avg_volume if avg_volume else 0
    resistance = max(x["high"] for x in closed[-21:-1])
    support = min(x["low"] for x in closed[-21:-1])

    score = 0
    reasons = []

    checks = [
        (price > ema20, 15, "السعر فوق EMA20"),
        (price > ema50, 15, "السعر فوق EMA50"),
        (price > ema200, 20, "الاتجاه العام فوق EMA200"),
        (ema20 > ema50, 15, "EMA20 أعلى من EMA50"),
        (45 <= rsi_now <= 65 and rsi_now > rsi_prev, 15, f"RSI صاعد ومناسب ({rsi_now:.1f})"),
        (volume_ratio >= 1.5, 10, f"الحجم أعلى من المتوسط ×{volume_ratio:.1f}"),
        (price > resistance, 10, "اختراق أعلى 20 شمعة"),
    ]
    for ok, points, reason in checks:
        if ok:
            score += points
            reasons.append(reason)

    current_atr = atr(closed)
    stop = max(support, price - 1.5 * current_atr)
    if stop >= price:
        stop = price - 1.5 * current_atr
    risk = price - stop

    return {
        "symbol": symbol,
        "score": score,
        "entry": price,
        "stop": stop,
        "target1": price + 1.5 * risk,
        "target2": price + 2.5 * risk,
        "rsi": rsi_now,
        "volume_ratio": volume_ratio,
        "reasons": reasons,
        "candle_close": closed[-1]["close_time"],
    }


def fmt(value):
    if value >= 1000:
        digits = 2
    elif value >= 1:
        digits = 4
    elif value >= 0.01:
        digits = 5
    else:
        digits = 7
    return f"{value:.{digits}f}"


def send_message(text):
    r = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text},
        timeout=20,
    )
    r.raise_for_status()


def load_state():
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state))


def scan():
    state = load_state()
    alerts = []

    for symbol in SYMBOLS:
        try:
            result = analyze(symbol, get_klines(symbol))
            last = int(state.get(symbol, 0))
            cooled = result["candle_close"] - last >= COOLDOWN_HOURS * 3600000
            if result["score"] >= MIN_SCORE and cooled:
                alerts.append(result)
                state[symbol] = result["candle_close"]
            time.sleep(0.2)
        except Exception as exc:
            print(f"{symbol}: {exc}", flush=True)

    alerts.sort(key=lambda x: x["score"], reverse=True)
    for a in alerts[:5]:
        risk_pct = (a["entry"] - a["stop"]) / a["entry"] * 100
        reasons = "\n".join(f"• {x}" for x in a["reasons"])
        send_message(
            f"🟢 إشارة مراقبة قوية — {a['symbol']}\n\n"
            f"التقييم: {a['score']}/100\n"
            f"الفريم: {TIMEFRAME}\n"
            f"دخول تقريبي: {fmt(a['entry'])}\n"
            f"وقف الخسارة: {fmt(a['stop'])} ({risk_pct:.1f}%)\n"
            f"الهدف الأول: {fmt(a['target1'])}\n"
            f"الهدف الثاني: {fmt(a['target2'])}\n"
            f"RSI: {a['rsi']:.1f}\n"
            f"الحجم: ×{a['volume_ratio']:.1f}\n\n"
            f"الأسباب:\n{reasons}\n\n"
            "⚠️ تنبيه فني وليس ضمانًا للربح. لا تدخل بكامل السيولة."
        )

    save_state(state)
    print(f"Scan finished. Alerts: {len(alerts[:5])}", flush=True)


if __name__ == "__main__":
    send_message("✅ تم تشغيل بوت التنبيهات بنجاح.")
    while True:
        try:
            scan()
        except Exception as exc:
            print(f"Scan error: {exc}", flush=True)
        time.sleep(SCAN_MINUTES * 60)
