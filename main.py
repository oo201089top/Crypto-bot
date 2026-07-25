import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple

import requests

# ============================================================
# Binance Spot Signal Bot — إشارات شراء سبوت فقط
# لا ينفذ صفقات، لا شورت، لا WATCH، ولا رسائل صفر تنبيه.
# الفريمات: 5m للدخول، 15m للتجهيز، 1h للتأكيد، 4h للاتجاه العام.
# ============================================================

BINANCE_BASE = "https://data-api.binance.vision"
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

SCAN_MINUTES = int(os.getenv("SCAN_MINUTES", "1"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "120"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
SYMBOL_REFRESH_MINUTES = int(os.getenv("SYMBOL_REFRESH_MINUTES", "30"))
MIN_DAILY_QUOTE_VOLUME = float(os.getenv("MIN_DAILY_QUOTE_VOLUME", "500000"))
PREFILTER_LIMIT = int(os.getenv("PREFILTER_LIMIT", "45"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "76"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.45"))
MIN_ADX = float(os.getenv("MIN_ADX", "20"))
MAX_1H_RISE_PCT = float(os.getenv("MAX_1H_RISE_PCT", "8"))
MAX_4H_RISE_PCT = float(os.getenv("MAX_4H_RISE_PCT", "14"))
MAX_RISK_PCT = float(os.getenv("MAX_RISK_PCT", "4"))
MAX_CANDLE_BODY_PCT = float(os.getenv("MAX_CANDLE_BODY_PCT", "2.8"))
MAX_EMA20_DISTANCE_PCT = float(os.getenv("MAX_EMA20_DISTANCE_PCT", "2.5"))
MAX_CONSECUTIVE_GREEN = int(os.getenv("MAX_CONSECUTIVE_GREEN", "3"))
MARKET_CACHE_SECONDS = int(os.getenv("MARKET_CACHE_SECONDS", "55"))
TRACK_RESULTS = os.getenv("TRACK_RESULTS", "1") == "1"

STATE_FILE = Path("spot_signal_state.json")
SESSION = requests.Session()
SYMBOL_CACHE: Dict[str, object] = {"symbols": [], "updated_at": 0.0}
MARKET_CACHE: Dict[str, object] = {"data": None, "updated_at": 0.0}

# العملات المستقرة والعملات المرتبطة بعملات ورقية
STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USD1", "BUSD", "USDS",
    "EUR", "AEUR", "EURT", "TRY", "BRL", "GBP", "AUD", "BIDR", "IDRT",
    "UAH", "RUB", "NGN", "VAI", "PAX", "UST", "USTC"
}

# لا يتم استبعاد العملات الكبيرة افتراضيًا، ويمكن تحديد المستبعد من Railway.
# احذف أي رمز من المتغير EXCLUDED_MAJORS في Railway إذا أردت إدخاله لاحقًا.
DEFAULT_EXCLUDED_MAJORS = set()
EXCLUDED_MAJORS = {
    x.strip().upper()
    for x in os.getenv("EXCLUDED_MAJORS", ",".join(sorted(DEFAULT_EXCLUDED_MAJORS))).split(",")
    if x.strip()
}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")


def send_message(text: str) -> None:
    response = SESSION.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True},
        timeout=20,
    )
    response.raise_for_status()


def get_json(path: str, params: Optional[Dict] = None, timeout: int = 20):
    response = SESSION.get(f"{BINANCE_BASE}{path}", params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_klines(symbol: str, interval: str, limit: int = 260) -> List[Dict]:
    raw = get_json("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    return [
        {
            "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
            "close": float(x[4]), "volume": float(x[5]),
            "close_time": int(x[6]), "quote_volume": float(x[7]),
            "trades": int(x[8]),
        }
        for x in raw
    ]


def get_symbols() -> List[str]:
    now = time.time()
    cached = SYMBOL_CACHE.get("symbols", [])
    updated_at = float(SYMBOL_CACHE.get("updated_at", 0.0))
    if cached and now - updated_at < SYMBOL_REFRESH_MINUTES * 60:
        return list(cached)

    exchange = get_json("/api/v3/exchangeInfo", timeout=30)
    tickers = get_json("/api/v3/ticker/24hr", timeout=30)
    volume_map = {
        t.get("symbol", ""): float(t.get("quoteVolume", 0.0) or 0.0)
        for t in tickers
    }

    symbols: List[Tuple[str, float]] = []
    for item in exchange.get("symbols", []):
        symbol = item.get("symbol", "")
        base = item.get("baseAsset", "").upper()
        quote = item.get("quoteAsset", "").upper()
        if item.get("status") != "TRADING" or quote != "USDT":
            continue
        if not item.get("isSpotTradingAllowed", True):
            continue
        if base in STABLE_BASES or base in EXCLUDED_MAJORS:
            continue
        if base.endswith(LEVERAGED_SUFFIXES):
            continue
        qv = volume_map.get(symbol, 0.0)
        if qv < MIN_DAILY_QUOTE_VOLUME:
            continue
        symbols.append((symbol, qv))

    symbols.sort(key=lambda x: x[1], reverse=True)
    result = [s for s, _ in symbols]
    SYMBOL_CACHE["symbols"] = result
    SYMBOL_CACHE["updated_at"] = now
    return result


def ema(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period:
        return [None] * len(values)
    out: List[Optional[float]] = [None] * (period - 1)
    current = mean(values[:period])
    out.append(current)
    k = 2 / (period + 1)
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
    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])
    out: List[Optional[float]] = [None] * period

    def calc(g: float, l: float) -> float:
        if l == 0:
            return 100.0
        rs = g / l
        return 100 - 100 / (1 + rs)

    out.append(calc(avg_gain, avg_loss))
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        out.append(calc(avg_gain, avg_loss))
    return out


def macd(values: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if len(values) < 35:
        return None, None, None
    fast, slow = ema(values, 12), ema(values, 26)
    series = [a - b for a, b in zip(fast, slow) if a is not None and b is not None]
    if len(series) < 9:
        return None, None, None
    sig = ema(series, 9)[-1]
    if sig is None:
        return None, None, None
    now = series[-1]
    return now, sig, now - sig


def atr(candles: List[Dict], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    tr = []
    for i, c in enumerate(candles):
        if i == 0:
            tr.append(c["high"] - c["low"])
        else:
            pc = candles[i - 1]["close"]
            tr.append(max(c["high"] - c["low"], abs(c["high"] - pc), abs(c["low"] - pc)))
    return mean(tr[-period:])


def adx(candles: List[Dict], period: int = 14) -> float:
    if len(candles) < period + 2:
        return 0.0
    trs, pdm, mdm = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        up, down = c["high"] - p["high"], p["low"] - c["low"]
        pdm.append(up if up > down and up > 0 else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(c["high"] - c["low"], abs(c["high"] - p["close"]), abs(c["low"] - p["close"])))
    dx = []
    for end in range(period, len(trs) + 1):
        tr_sum = sum(trs[end - period:end])
        if tr_sum <= 0:
            continue
        pdi = 100 * sum(pdm[end - period:end]) / tr_sum
        mdi = 100 * sum(mdm[end - period:end]) / tr_sum
        if pdi + mdi > 0:
            dx.append(100 * abs(pdi - mdi) / (pdi + mdi))
    return mean(dx[-period:]) if dx else 0.0


def vwap(candles: List[Dict], period: int = 20) -> float:
    window = candles[-period:]
    total_volume = sum(c["volume"] for c in window)
    if total_volume <= 0:
        return window[-1]["close"]
    return sum(((c["high"] + c["low"] + c["close"]) / 3) * c["volume"] for c in window) / total_volume


def obv(candles: List[Dict]) -> List[float]:
    out = [0.0]
    for i in range(1, len(candles)):
        if candles[i]["close"] > candles[i - 1]["close"]:
            out.append(out[-1] + candles[i]["volume"])
        elif candles[i]["close"] < candles[i - 1]["close"]:
            out.append(out[-1] - candles[i]["volume"])
        else:
            out.append(out[-1])
    return out


def bollinger(values: List[float], period: int = 20, mult: float = 2.0) -> Tuple[float, float, float, float]:
    window = values[-period:]
    mid = mean(window)
    sd = pstdev(window)
    upper, lower = mid + mult * sd, mid - mult * sd
    width = (upper - lower) / mid * 100 if mid else 0.0
    return mid, upper, lower, width


def supertrend(candles: List[Dict], period: int = 10, multiplier: float = 3.0) -> Optional[bool]:
    if len(candles) < period + 3:
        return None
    atr_value = atr(candles, period)
    if atr_value <= 0:
        return None
    closes = [c["close"] for c in candles]
    hl2 = [(c["high"] + c["low"]) / 2 for c in candles]
    upper = [x + multiplier * atr_value for x in hl2]
    lower = [x - multiplier * atr_value for x in hl2]
    trend_up = True
    final_upper, final_lower = upper[0], lower[0]
    for i in range(1, len(candles)):
        final_upper = upper[i] if upper[i] < final_upper or closes[i - 1] > final_upper else final_upper
        final_lower = lower[i] if lower[i] > final_lower or closes[i - 1] < final_lower else final_lower
        if closes[i] > final_upper:
            trend_up = True
        elif closes[i] < final_lower:
            trend_up = False
        if trend_up:
            final_upper = upper[i]
        else:
            final_lower = lower[i]
    return trend_up


def pct_change(old: float, new: float) -> float:
    return ((new / old) - 1) * 100 if old else 0.0




def consecutive_green(candles: List[Dict], lookback: int = 6) -> int:
    count = 0
    for candle in reversed(candles[-lookback:]):
        if candle["close"] > candle["open"]:
            count += 1
        else:
            break
    return count


def market_context() -> Dict:
    """فلتر سوق مرن: لا يمنع كل الإشارات عند ضعف BTC، بل يشدد الشروط."""
    now = time.time()
    cached = MARKET_CACHE.get("data")
    updated_at = float(MARKET_CACHE.get("updated_at", 0.0))
    if cached and now - updated_at < MARKET_CACHE_SECONDS:
        return dict(cached)

    def asset_snapshot(symbol: str) -> Dict:
        c15 = get_klines(symbol, "15m", 120)[:-1]
        c1h = get_klines(symbol, "1h", 120)[:-1]
        close15 = [c["close"] for c in c15]
        close1h = [c["close"] for c in c1h]
        e20_15 = ema(close15, 20)[-1]
        e50_15 = ema(close15, 50)[-1]
        e20_1h = ema(close1h, 20)[-1]
        e50_1h = ema(close1h, 50)[-1]
        _, _, hist15 = macd(close15)
        _, _, hist1h = macd(close1h)
        p15 = close15[-1]
        p1h = close1h[-1]
        return {
            "price": p15,
            "rise_1h": pct_change(close15[-5], p15),
            "rise_4h": pct_change(close1h[-5], p1h),
            "bull15": bool(e20_15 and e50_15 and hist15 is not None and p15 > e20_15 > e50_15 and hist15 >= 0),
            "bear15": bool(e20_15 and e50_15 and hist15 is not None and p15 < e20_15 < e50_15 and hist15 < 0),
            "bull1h": bool(e20_1h and e50_1h and hist1h is not None and p1h > e20_1h > e50_1h and hist1h >= 0),
            "bear1h": bool(e20_1h and e50_1h and hist1h is not None and p1h < e20_1h < e50_1h and hist1h < 0),
        }

    try:
        btc = asset_snapshot("BTCUSDT")
        eth = asset_snapshot("ETHUSDT")
        points = 0
        points += 2 if btc["bull1h"] else (-2 if btc["bear1h"] else 0)
        points += 1 if btc["bull15"] else (-1 if btc["bear15"] else 0)
        points += 1 if eth["bull1h"] else (-1 if eth["bear1h"] else 0)
        points += 1 if eth["bull15"] else (-1 if eth["bear15"] else 0)

        severe_drop = btc["rise_1h"] <= -1.4 or btc["rise_4h"] <= -3.0
        if severe_drop or points <= -4:
            regime = "ضعيف جدًا"
            score_bonus_required = 12
            volume_multiplier = 1.35
            rsi_cap = 64.0
        elif points <= -2:
            regime = "ضعيف"
            score_bonus_required = 7
            volume_multiplier = 1.18
            rsi_cap = 66.0
        elif points >= 3:
            regime = "إيجابي"
            score_bonus_required = 0
            volume_multiplier = 1.0
            rsi_cap = 69.0
        else:
            regime = "محايد"
            score_bonus_required = 3
            volume_multiplier = 1.08
            rsi_cap = 67.0

        data = {
            "regime": regime,
            "points": points,
            "btc": btc,
            "eth": eth,
            "required_score": MIN_SCORE + score_bonus_required,
            "required_volume_ratio": MIN_VOLUME_RATIO * volume_multiplier,
            "rsi_cap": rsi_cap,
            "severe_drop": severe_drop,
        }
    except Exception as exc:
        print(f"Market filter error: {exc}", flush=True)
        data = {
            "regime": "غير متاح",
            "points": 0,
            "btc": {"rise_1h": 0.0, "rise_4h": 0.0},
            "eth": {"rise_1h": 0.0, "rise_4h": 0.0},
            "required_score": MIN_SCORE + 3,
            "required_volume_ratio": MIN_VOLUME_RATIO * 1.08,
            "rsi_cap": 67.0,
            "severe_drop": False,
        }

    MARKET_CACHE["data"] = data
    MARKET_CACHE["updated_at"] = now
    return dict(data)


def safe_last(values: List[Optional[float]], offset: int = 1) -> Optional[float]:
    if len(values) < offset:
        return None
    return values[-offset]


def prefilter_symbol(symbol: str) -> Optional[Tuple[str, float]]:
    try:
        candles = get_klines(symbol, "5m", 80)[:-1]
        if len(candles) < 55:
            return None
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        avg_vol = mean(volumes[-21:-1])
        vol_ratio = volumes[-1] / avg_vol if avg_vol else 0.0
        e9, e20 = ema(closes, 9)[-1], ema(closes, 20)[-1]
        if e9 is None or e20 is None:
            return None
        momentum = pct_change(closes[-7], closes[-1])
        resistance = max(c["high"] for c in candles[-21:-1])
        proximity = closes[-1] / resistance if resistance else 0.0
        score = vol_ratio * 30 + max(momentum, 0) * 4 + max(proximity - 0.97, 0) * 200
        if closes[-1] < e20 * 0.985:
            score -= 20
        return symbol, score
    except Exception as exc:
        print(f"Prefilter {symbol}: {exc}", flush=True)
        return None


def frame_snapshot(candles: List[Dict]) -> Optional[Dict]:
    closed = candles[:-1]
    if len(closed) < 210:
        return None
    closes = [c["close"] for c in closed]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1]
    r = rsi(closes)[-1]
    _, _, hist = macd(closes)
    if None in (e20, e50, e200, r, hist):
        return None
    price = closes[-1]
    return {
        "price": price, "e20": e20, "e50": e50, "e200": e200,
        "rsi": r, "macd_hist": hist,
        "bullish": price > e20 > e50 and price > e200 and hist > 0,
        "not_bearish": price > e50 and hist >= 0,
    }


def analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    try:
        c5 = get_klines(symbol, "5m", 260)
        c15 = get_klines(symbol, "15m", 260)
        c1h = get_klines(symbol, "1h", 260)
        c4h = get_klines(symbol, "4h", 260)
        closed = c5[:-1]
        if min(len(closed), len(c15) - 1, len(c1h) - 1, len(c4h) - 1) < 210:
            return None

        closes = [c["close"] for c in closed]
        volumes = [c["volume"] for c in closed]
        trades = [c["trades"] for c in closed]
        price = closes[-1]
        candle = closed[-1]

        e9 = ema(closes, 9)[-1]
        e20 = ema(closes, 20)[-1]
        e50 = ema(closes, 50)[-1]
        rsi_values = rsi(closes)
        rsi_now, rsi_prev = rsi_values[-1], rsi_values[-2]
        macd_now, macd_sig, macd_hist = macd(closes)
        if None in (e9, e20, e50, rsi_now, rsi_prev, macd_now, macd_sig, macd_hist):
            return None

        atr_now = atr(closed)
        adx_now = adx(closed)
        vw = vwap(closed)
        obv_values = obv(closed)
        _, bb_upper, _, bb_width = bollinger(closes)

        avg_vol = mean(volumes[-21:-1])
        volume_ratio = volumes[-1] / avg_vol if avg_vol else 0.0
        avg_trades = mean(trades[-21:-1])
        trade_ratio = trades[-1] / avg_trades if avg_trades else 0.0
        # المقاومة تُحسب بدون آخر شمعتين حتى يمكن اكتشاف:
        # 1) اختراق في الشمعة السابقة
        # 2) إعادة اختبار في الشمعة الحالية
        resistance = max(c["high"] for c in closed[-22:-2])
        previous_candle = closed[-2]
        recent_support = min(c["low"] for c in closed[-12:-1])

        breakout = (
            price > resistance
            and volume_ratio >= 1.2
            and candle["close"] > candle["open"]
        )
        near_breakout = (
            price >= resistance * 0.997
            and candle["close"] > candle["open"]
        )
        retest = (
            previous_candle["close"] > resistance
            and previous_candle["high"] > resistance
            and candle["low"] <= resistance * 1.004
            and candle["low"] >= resistance * 0.990
            and price > resistance
            and candle["close"] > candle["open"]
        )

        # انضغاط البولنجر: العرض الحالي أو السابق منخفض مقارنة بآخر 40 شمعة.
        widths = []
        for i in range(40, 0, -1):
            segment = closes[:-i] if i else closes
            if len(segment) >= 20:
                widths.append(bollinger(segment)[3])
        squeeze = bool(widths) and bb_width <= sorted(widths)[max(0, int(len(widths) * 0.30) - 1)]
        squeeze_breakout = squeeze and price >= bb_upper * 0.995 and volume_ratio >= 1.3

        snap15 = frame_snapshot(c15)
        snap1h = frame_snapshot(c1h)
        snap4h = frame_snapshot(c4h)
        if not snap15 or not snap1h or not snap4h:
            return None

        rise_1h = pct_change(closed[-13]["close"], price)
        rise_4h = pct_change(closed[-49]["close"], price)
        candle_body_pct = abs(candle["close"] - candle["open"]) / candle["open"] * 100 if candle["open"] else 0.0
        ema20_distance_pct = (price / e20 - 1) * 100 if e20 else 999.0
        green_streak = consecutive_green(closed)

        if rise_1h > MAX_1H_RISE_PCT or rise_4h > MAX_4H_RISE_PCT:
            return None
        if candle_body_pct > MAX_CANDLE_BODY_PCT:
            return None
        if ema20_distance_pct > MAX_EMA20_DISTANCE_PCT:
            return None
        if green_streak > MAX_CONSECUTIVE_GREEN:
            return None

        score = 0
        reasons: List[str] = []

        def add(condition: bool, points: int, reason: str) -> None:
            nonlocal score
            if condition:
                score += points
                reasons.append(reason)

        add(price > e9 > e20 > e50, 12, "ترتيب المتوسطات صاعد على 5 دقائق")
        add(price > vw, 6, "السعر ثابت فوق VWAP")
        add(54 <= rsi_now <= 70 and rsi_now >= rsi_prev, 8, f"RSI إيجابي {rsi_now:.1f}")
        add(macd_now > macd_sig and macd_hist > 0, 8, "MACD إيجابي")
        add(adx_now >= MIN_ADX, 7, f"قوة الاتجاه ADX {adx_now:.0f}")
        add(obv_values[-1] > obv_values[-5], 6, "OBV يؤكد دخول السيولة")
        add(volume_ratio >= MIN_VOLUME_RATIO, 11, f"ارتفاع الحجم ×{volume_ratio:.1f}")
        add(trade_ratio >= 1.25, 5, f"نشاط الصفقات ×{trade_ratio:.1f}")
        add(breakout, 13, "اختراق مقاومة بإغلاق")
        add(retest, 14, "إعادة اختبار ناجحة")
        add(squeeze_breakout, 10, "خروج من انضغاط Bollinger")
        add(near_breakout and not breakout, 5, "قريب جدًا من الاختراق")
        add(snap15["bullish"], 9, "تأكيد صاعد على 15 دقيقة")
        add(snap1h["bullish"], 10, "تأكيد صاعد على الساعة")
        add(snap4h["not_bearish"], 6, "فريم 4 ساعات لا يعاكس الصفقة")
        add(supertrend(c15[:-1]) is True, 6, "SuperTrend شراء على 15 دقيقة")
        relative_strength = rise_1h - float(market.get("btc", {}).get("rise_1h", 0.0))
        add(relative_strength >= 1.0, 8, f"قوة نسبية أعلى من BTC بـ {relative_strength:.1f}%")
        add(market.get("regime") == "إيجابي", 4, "السوق العام داعم")

        # شروط إجبارية تمنع الإشارات الضعيفة والمتأخرة.
        if not (snap15["bullish"] and snap1h["not_bearish"] and snap4h["not_bearish"]):
            return None
        if not (breakout or retest or squeeze_breakout):
            return None
        if volume_ratio < float(market.get("required_volume_ratio", MIN_VOLUME_RATIO)) or adx_now < MIN_ADX:
            return None
        if price < e20 or price < vw:
            return None
        if rsi_now > float(market.get("rsi_cap", 67.0)):
            return None
        if score < int(market.get("required_score", MIN_SCORE)):
            return None

        stop = min(recent_support, price - 1.25 * atr_now)
        risk = price - stop
        if risk <= 0:
            return None
        risk_pct = risk / price * 100
        if risk_pct > MAX_RISK_PCT:
            return None

        return {
            "symbol": symbol,
            "entry": price,
            "stop": stop,
            "tp1": price + 1.5 * risk,
            "tp2": price + 2.2 * risk,
            "tp3": price + 3.0 * risk,
            "risk_pct": risk_pct,
            "score": min(score, 99),
            "volume_ratio": volume_ratio,
            "rsi": rsi_now,
            "adx": adx_now,
            "setup": "إعادة اختبار" if retest else ("انضغاط واختراق" if squeeze_breakout else "اختراق"),
            "reasons": reasons[:7],
            "candle_close": candle["close_time"],
            "market_regime": market.get("regime", "غير متاح"),
            "btc_1h": float(market.get("btc", {}).get("rise_1h", 0.0)),
            "relative_strength": relative_strength,
        }
    except Exception as exc:
        print(f"Analyze {symbol}: {exc}", flush=True)
        return None


def fmt(value: float) -> str:
    if value >= 1000:
        digits = 2
    elif value >= 1:
        digits = 4
    elif value >= 0.01:
        digits = 5
    else:
        digits = 8
    return f"{value:.{digits}f}"


def signal_message(result: Dict) -> str:
    reasons = "\n".join(f"• {x}" for x in result["reasons"])
    return (
        f"🟢 إشارة شراء سبوت — {result['symbol']}\n\n"
        f"النموذج: {result['setup']}\n"
        f"قوة الإشارة: {result['score']}%\n"
        f"الفريمات: 5m / 15m / 1h / 4h\n"
        f"حالة السوق: {result['market_regime']} | BTC ساعة: {result['btc_1h']:+.2f}%\n"
        f"القوة النسبية أمام BTC: {result['relative_strength']:+.2f}%\n\n"
        f"سعر الشراء التقريبي: {fmt(result['entry'])}\n"
        f"وقف الخسارة: {fmt(result['stop'])} ({result['risk_pct']:.2f}%)\n"
        f"الهدف الأول: {fmt(result['tp1'])}\n"
        f"الهدف الثاني: {fmt(result['tp2'])}\n"
        f"الهدف الثالث: {fmt(result['tp3'])}\n\n"
        f"RSI: {result['rsi']:.1f}\n"
        f"ADX: {result['adx']:.1f}\n"
        f"الحجم: ×{result['volume_ratio']:.1f}\n\n"
        f"أسباب الإشارة:\n{reasons}\n\n"
        "⚠️ تحليل فني آلي وليس ضمانًا للربح."
    )


def load_state() -> Dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"alerts": {}}


def save_state(state: Dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def cooled(state: Dict, result: Dict) -> bool:
    previous = int(state.get("alerts", {}).get(result["symbol"], 0))
    return result["candle_close"] - previous >= COOLDOWN_MINUTES * 60 * 1000




def track_open_signals(state: Dict) -> None:
    if not TRACK_RESULTS:
        return

    open_signals = state.setdefault("open_signals", {})
    stats = state.setdefault("stats", {"tp1": 0, "tp2": 0, "tp3": 0, "stop": 0})
    now_ms = int(time.time() * 1000)

    for symbol, signal in list(open_signals.items()):
        try:
            candles = get_klines(symbol, "5m", 100)[:-1]
            last_checked = int(signal.get("last_checked", signal["time"]))
            relevant = [c for c in candles if c["close_time"] > last_checked]

            if not relevant:
                if now_ms - int(signal["time"]) > 48 * 60 * 60 * 1000:
                    del open_signals[symbol]
                continue

            reached = int(signal.get("reached", 0))
            closed_signal = False

            # فحص زمني شمعة بشمعة بدل استخدام أعلى/أدنى الفترة كاملة.
            for candle in relevant:
                high = float(candle["high"])
                low = float(candle["low"])

                # إذا ضربت الشمعة الوقف والهدف معًا، نستخدم ترتيبًا محافظًا:
                # الوقف أولًا لأن ترتيب الحركة داخل الشمعة غير معروف.
                if low <= float(signal["stop"]):
                    stats["stop"] = int(stats.get("stop", 0)) + 1
                    del open_signals[symbol]
                    closed_signal = True
                    break

                if reached < 1 and high >= float(signal["tp1"]):
                    stats["tp1"] = int(stats.get("tp1", 0)) + 1
                    reached = 1

                if reached < 2 and high >= float(signal["tp2"]):
                    stats["tp2"] = int(stats.get("tp2", 0)) + 1
                    reached = 2

                if reached < 3 and high >= float(signal["tp3"]):
                    stats["tp3"] = int(stats.get("tp3", 0)) + 1
                    reached = 3
                    del open_signals[symbol]
                    closed_signal = True
                    break

                signal["last_checked"] = candle["close_time"]

            if closed_signal:
                continue

            signal["reached"] = reached
            signal["last_checked"] = relevant[-1]["close_time"]

            if now_ms - int(signal["time"]) > 48 * 60 * 60 * 1000:
                del open_signals[symbol]

        except Exception as exc:
            print(f"Track {symbol}: {exc}", flush=True)


def scan(state: Dict) -> None:
    track_open_signals(state)
    market = market_context()
    symbols = get_symbols()
    ranked: List[Tuple[str, float]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(prefilter_symbol, symbol) for symbol in symbols]
        for future in as_completed(futures):
            item = future.result()
            if item:
                ranked.append(item)
    ranked.sort(key=lambda x: x[1], reverse=True)
    shortlist = [s for s, _ in ranked[:PREFILTER_LIMIT]]

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max(4, MAX_WORKERS // 2)) as pool:
        futures = [pool.submit(analyze_symbol, symbol, market) for symbol in shortlist]
        for future in as_completed(futures):
            result = future.result()
            if result and cooled(state, result):
                results.append(result)

    results.sort(key=lambda x: (x["score"], x["volume_ratio"]), reverse=True)
    sent = 0
    for result in results[:MAX_ALERTS_PER_SCAN]:
        send_message(signal_message(result))
        state.setdefault("alerts", {})[result["symbol"]] = result["candle_close"]
        state.setdefault("open_signals", {})[result["symbol"]] = {
            "time": result["candle_close"],
            "last_checked": result["candle_close"],
            "entry": result["entry"],
            "stop": result["stop"],
            "tp1": result["tp1"],
            "tp2": result["tp2"],
            "tp3": result["tp3"],
            "reached": 0,
        }
        sent += 1
        time.sleep(0.3)

    save_state(state)
    print(
        f"Scan finished | market={market.get('regime')} | universe={len(symbols)} | shortlist={len(shortlist)} | candidates={len(results)} | sent={sent}",
        flush=True,
    )


def main() -> None:
    state = load_state()
    send_message(
        "✅ تم تشغيل بوت إشارات الشراء للسبوت.\n"
        "يفحص جميع العملات غير المستقرة على Binance Spot مقابل USDT.\n"
        "الفريمات: 5m / 15m / 1h / 4h\n"
        "بدون شورت وبدون WATCH، مع فلتر BTC وETH ومنع الدخول المتأخر."
    )
    while True:
        started = time.time()
        try:
            scan(state)
        except Exception as exc:
            print(f"Scan error: {exc}", flush=True)
        elapsed = time.time() - started
        time.sleep(max(5, SCAN_MINUTES * 60 - elapsed))


if __name__ == "__main__":
    main()
