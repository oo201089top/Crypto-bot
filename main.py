# V12: engine-scoped environment variables, shared aggTrades cache, active OI hard block,
# thread-safe caches, safer stock-contract discovery, and self-healing thread supervisor.
import os, time, json
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple
import requests

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
STATE_FILE = Path("spot_signal_state.json")
SESSION = requests.Session()
SYMBOL_CACHE = {"symbols": [], "updated_at": 0.0}
MARKET_CACHE = {"data": None, "updated_at": 0.0}
SYMBOL_CACHE_LOCK = Lock()
MARKET_CACHE_LOCK = Lock()
REJECTION_LOCK = Lock()

STABLE_BASES = {"USDC","FDUSD","TUSD","USDP","DAI","USD1","BUSD","USDS","EUR","AEUR","EURT","TRY","BRL","GBP","AUD","BIDR","IDRT","UAH","RUB","NGN","VAI","PAX","UST","USTC"}
EXCLUDED_MAJORS = {x.strip().upper() for x in os.getenv("EXCLUDED_MAJORS", "").split(",") if x.strip()}
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
    print(f"Rejected {symbol}: {reason} | {row['details']}", flush=True)
    try:
        with REJECTION_LOCK:
            with REJECTION_LOG_FILE.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"Rejection log error: {exc}", flush=True)


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


def send_message(text: str) -> None:
    r = SESSION.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}, timeout=20)
    r.raise_for_status()


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


def frame_snapshot(candles: List[Dict]) -> Optional[Dict]:
    closed=candles[:-1]
    if len(closed)<210: return None
    closes=[c["close"] for c in closed]
    e20v,e50v,e200v=ema(closes,20),ema(closes,50),ema(closes,200)
    e20,e50,e200=e20v[-1],e50v[-1],e200v[-1]; rr=rsi(closes)[-1]; _,_,hist=macd(closes)
    if None in (e20,e50,e200,rr,hist): return None
    price=closes[-1]
    strongly_bearish = price<e20<e50 and price<e200 and hist<0 and e20<=e20v[-4]
    return {"price":price,"e20":e20,"e50":e50,"e200":e200,"rsi":rr,"macd_hist":hist,"adx":adx(closed),"not_bearish":not strongly_bearish,"strongly_bearish":strongly_bearish}


def market_context() -> Dict:
    now=time.time()
    with MARKET_CACHE_LOCK:
        if MARKET_CACHE["data"] and now-float(MARKET_CACHE["updated_at"])<MARKET_CACHE_SECONDS:
            return dict(MARKET_CACHE["data"])
    try:
        def snap(symbol):
            c5=get_klines(symbol,"5m",120)[:-1]; c15=get_klines(symbol,"15m",120)[:-1]
            c1=get_klines(symbol,"1h",120)[:-1]; c4=get_klines(symbol,"4h",120)[:-1]
            z=[c["close"] for c in c5]; a=[c["close"] for c in c15]
            b=[c["close"] for c in c1]; d=[c["close"] for c in c4]
            e20z,e50z=ema(z,20)[-1],ema(z,50)[-1]
            e20a,e50a=ema(a,20)[-1],ema(a,50)[-1]
            e20b,e50b=ema(b,20)[-1],ema(b,50)[-1]
            e20d,e50d=ema(d,20)[-1],ema(d,50)[-1]
            _,_,hz=macd(z); _,_,ha=macd(a); _,_,hb=macd(b); _,_,hd=macd(d)
            rz,ra,rb,rd=rsi(z)[-1],rsi(a)[-1],rsi(b)[-1],rsi(d)[-1]
            return {
                "rise_5m":pct_change(z[-2],z[-1]),
                "rise_15m":pct_change(z[-4],z[-1]),
                "rise_1h":pct_change(a[-5],a[-1]),
                "rise_4h":pct_change(b[-5],b[-1]),
                "rsi5":rz,"rsi15":ra,"rsi1h":rb,"rsi4h":rd,
                "bull5":z[-1]>e20z>e50z and hz>=0,"bear5":z[-1]<e20z and hz<0,
                "bull15":a[-1]>e20a>e50a and ha>=0,"bear15":a[-1]<e20a and ha<0,
                "bull1h":b[-1]>e20b>e50b and hb>=0,"bear1h":b[-1]<e20b and hb<0,
                "bull4h":d[-1]>e20d>e50d and hd>=0,"bear4h":d[-1]<e20d<e50d and hd<0,
            }
        btc,eth=snap("BTCUSDT"),snap("ETHUSDT")
        points=(2 if btc["bull4h"] else -2 if btc["bear4h"] else 0)+(2 if btc["bull1h"] else -2 if btc["bear1h"] else 0)+(1 if btc["bull15"] else -1 if btc["bear15"] else 0)+(1 if eth["bull1h"] else -1 if eth["bear1h"] else 0)+(1 if eth["bull15"] else -1 if eth["bear15"] else 0)
        sudden_drop = btc["rise_5m"] <= -BTC_5M_DROP_BLOCK_PCT and btc["bear5"] and btc["rsi15"] < BTC_15M_RSI_BLOCK
        trend_break = btc["rise_1h"] <= -BTC_1H_DROP_BLOCK_PCT and btc["bear15"] and btc["bear1h"]
        weak_pressure = bool(
            SMART_MARKET_FILTER
            and btc["rsi15"] < BTC_WEAK_RSI_15M
            and (
                btc["rsi5"] < BTC_WEAK_RSI_5M
                or btc["rise_15m"] <= -BTC_WEAK_15M_DROP_PCT
            )
            and (btc["bear5"] or btc["bear15"])
        )
        severe=btc["rise_1h"]<=-1.4 or btc["rise_4h"]<=-3 or sudden_drop or trend_break
        hard_block=bool(SMART_MARKET_FILTER and (sudden_drop or trend_break or (btc["bear4h"] and btc["bear1h"] and btc["bear15"])))
        regime="ضعيف جدًا" if severe or points<=-4 else "ضعيف" if points<=-2 or weak_pressure else "إيجابي" if points>=3 else "محايد"
        bonus=12 if regime=="ضعيف جدًا" else 7 if regime=="ضعيف" else 0 if regime=="إيجابي" else 3
        data={
            "regime":regime,
            "btc":btc,
            "eth":eth,
            "required_score":MIN_SCORE+bonus,
            "severe_drop":severe,
            "hard_block":hard_block,
            "weak_pressure":weak_pressure,
        }
    except Exception as exc:
        print(f"Market filter error: {exc}", flush=True)
        data={"regime":"غير متاح","btc":{"rise_1h":0,"rise_4h":0,"rise_15m":0,"rsi5":50,"rsi15":50},"eth":{"rise_1h":0,"rise_4h":0},"required_score":MIN_SCORE+3,"severe_drop":False,"hard_block":False,"weak_pressure":False}
    with MARKET_CACHE_LOCK:
        MARKET_CACHE["data"],MARKET_CACHE["updated_at"]=data,now
    return dict(data)


def prefilter_symbol(symbol: str) -> Optional[Tuple[str,float]]:
    try:
        c=get_klines(symbol,"5m",80)[:-1]
        if len(c)<55: return None
        closes=[x["close"] for x in c]; vols=[x["volume"] for x in c]
        av=mean(vols[-21:-1]); vr=vols[-1]/av if av else 0; e20=ema(closes,20)[-1]
        resistance=max(x["high"] for x in c[-21:-1]); proximity=closes[-1]/resistance if resistance else 0
        score=vr*34+max(pct_change(closes[-7],closes[-1]),0)*5+max(proximity-0.965,0)*230
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
        if e20 and closes[-1]<e20*0.985: score-=20
        return symbol,score
    except Exception as exc:
        print(f"Prefilter {symbol}: {exc}", flush=True); return None


def analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    try:
        c5,c15,c1h,c4h=[get_klines(symbol,x,260) for x in ("5m","15m","1h","4h")]
        closed5,closed15=c5[:-1],c15[:-1]
        if min(len(closed5),len(closed15),len(c1h)-1,len(c4h)-1)<210: return None
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
        resistance=max(c["high"] for c in closed15[-22:-2]); support=min(c["low"] for c in closed15[-12:-1])
        atr15=atr(closed15)
        breakout_range=candle15["high"]-candle15["low"]
        breakout_body=abs(candle15["close"]-candle15["open"])
        breakout=candle15["close"]>resistance and candle15["close"]>candle15["open"] and vr15>=MIN_15M_VOLUME_RATIO
        retest=prev15["close"]>resistance and candle15["low"]<=resistance*1.004 and candle15["low"]>=resistance*0.990 and candle15["close"]>resistance and candle15["close"]>candle15["open"]
        widths=[bollinger(closes15[:-i])[3] for i in range(40,0,-1) if len(closes15[:-i])>=20]; _,bbup,_,bbw=bollinger(closes15)
        squeeze=bool(widths) and bbw<=sorted(widths)[max(0,int(len(widths)*0.30)-1)]
        squeeze_break=squeeze and candle15["close"]>=bbup*0.995 and vr15>=MIN_15M_VOLUME_RATIO
        s1,s4=frame_snapshot(c1h),frame_snapshot(c4h)
        if not s1 or not s4: return None

        # فلتر RSI قاسٍ: يمنع الإشارات المتأخرة حتى لو اجتازت المسارات الأخرى.
        if r15 > MAX_RSI_HARD:
            log_rejection(symbol, "RSI مرتفع", {"rsi15": round(r15, 2), "limit": MAX_RSI_HARD})
            return None

        # شمعة اختراق مفرطة غالبًا تكون استنزافًا لا بداية آمنة.
        if breakout and atr15 > 0 and (breakout_range > MAX_BREAKOUT_RANGE_ATR * atr15 or breakout_body > MAX_BREAKOUT_BODY_ATR * atr15):
            log_rejection(symbol, "شمعة اختراق استنزافية", {
                "range_atr": round(breakout_range / atr15, 2),
                "body_atr": round(breakout_body / atr15, 2),
            })
            return None

        # يجب أن تكون آخر شمعة 5m مغلقة بعد شمعة الاختراق 15m، وتثبت فوق المستوى.
        if BREAKOUT_CONFIRM_5M and breakout:
            confirm_after_breakout = candle5["close_time"] > candle15["close_time"]
            held_level = candle5["close"] > resistance and candle5["low"] >= resistance * (1 - CONFIRM_MAX_DIP_PCT / 100)
            healthy_close = candle5["close"] >= candle5["open"] and close_location(candle5) >= MIN_CONFIRM_CLOSE_LOCATION
            if not (confirm_after_breakout and held_level and healthy_close and h5 >= 0):
                log_rejection(symbol, "فشل تأكيد الاختراق 5m", {
                    "after_breakout": confirm_after_breakout,
                    "held_level": held_level,
                    "healthy_close": healthy_close,
                    "confirm_close": candle5["close"],
                    "resistance": resistance,
                })
                return None

        drawdown15=recent_drawdown_pct(closed15, 18)
        recent_rsi_low=min(x for x in r15v[-10:] if x is not None)
        structure_shift=higher_low_structure(closed5, 18)
        reclaimed_15=candle15["close"]>candle15["open"] and candle15["close"]>=prev15["high"]*0.997 and candle15["close"]>e20_15
        reversal = REVERSAL_ENABLED and drawdown15>=REVERSAL_MIN_DRAWDOWN_PCT and recent_rsi_low<=REVERSAL_MAX_RSI_RECENT_LOW and REVERSAL_MIN_RSI_NOW<=r15<=REVERSAL_MAX_RSI_NOW and r15>r15v[-3] and reclaimed_15 and structure_shift and vr15>=REVERSAL_MIN_VOLUME_15M and vr5>=REVERSAL_MIN_VOLUME_5M and a15>=REVERSAL_MIN_ADX_15M and h5>=0 and not s1["strongly_bearish"] and not s4["strongly_bearish"] and not market.get("hard_block",False)

        momentum = MOMENTUM_ENABLED and candle15["close"]>resistance and candle15["close"]>candle15["open"] and vr15>=MOMENTUM_MIN_VOLUME_15M and vr5>=MOMENTUM_MIN_VOLUME_5M and a15>=MOMENTUM_MIN_ADX_15M and MOMENTUM_MIN_RSI_15M<=r15<=MOMENTUM_MAX_RSI_15M and h15>0 and h5>=0 and e20_15>e20v[-4] and not s1["strongly_bearish"] and not s4["strongly_bearish"] and not market.get("severe_drop",False) and not market.get("hard_block",False) and rise15<=MOMENTUM_MAX_15M_RISE and rise1<=MOMENTUM_MAX_1H_RISE and dist<=MOMENTUM_MAX_EMA20_DISTANCE and greens<=MOMENTUM_MAX_GREEN and body<=4.0

        accumulation_info = accumulation_base(closed15, ACCUMULATION_LOOKBACK_15M) if ACCUMULATION_ENABLED else None
        early_level = float(accumulation_info["resistance"]) if accumulation_info else 0.0
        early_break = bool(accumulation_info and candle15["close"] >= early_level * 0.998 and candle15["close"] > candle15["open"])
        accumulation = bool(
            ACCUMULATION_ENABLED and accumulation_info
            and accumulation_info["compressed"] and accumulation_info["higher_lows"] and accumulation_info["building_volume"]
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

        rel=rise1-float(market.get("btc",{}).get("rise_1h",0))

        if market.get("hard_block",False):
            log_rejection(symbol, "إيقاف كامل بسبب هبوط BTC", {
                "btc5": round(float(market.get("btc",{}).get("rise_5m",0)), 3),
                "btc15": round(float(market.get("btc",{}).get("rise_15m",0)), 3),
                "btc1h": round(float(market.get("btc",{}).get("rise_1h",0)), 3),
                "btc_rsi15": round(float(market.get("btc",{}).get("rsi15",0)), 2),
            })
            return None

        # في ضعف BTC المتوسط لا نسمح إلا بعملة تتفوق عليه بوضوح.
        if market.get("weak_pressure",False) and rel < BTC_WEAK_MIN_RELATIVE_STRENGTH:
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
            required_room = ACCUMULATION_MIN_NEXT_RESISTANCE_PCT if accumulation else MOMENTUM_MIN_NEXT_RESISTANCE_PCT if (momentum or reversal) else MIN_NEXT_RESISTANCE_PCT
            if overhead_pct < required_room:
                log_rejection(symbol, "مقاومة قريبة مؤكدة", {
                    "distance_pct": round(overhead_pct, 2),
                    "required_pct": required_room,
                    "resistance": round(float(overhead["level"]), 10),
                    "touches": int(overhead["touches"]),
                })
                return None

        if accumulation:
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
            if score < max(ACCUMULATION_MIN_SCORE, int(market.get("required_score", MIN_SCORE))): return None
            base_low = float(accumulation_info["base_low"])
            recent_low = min(c["low"] for c in closed5[-10:-1])
            stop = min(base_low, recent_low, price-1.20*atr5)
            mode,setup = "accumulation", "تجميع مبكر قبل الانطلاقة"
        elif reversal:
            score=22+16+12+10+8+8+6
            reasons=[f"ارتداد بعد هبوط {drawdown15:.1f}%", "خروج RSI من الضعف", "تحول هيكل 5m إلى قاع أعلى", "استعادة EMA20 على 15m", f"حجم ارتداد 15m ×{vr15:.1f}", f"تأكيد حجم 5m ×{vr5:.1f}", "السعر فوق VWAP"]
            if obv5[-1]>obv5[-5]: score+=6
            if rel>=0.5: score+=6
            if market.get("regime") in ("إيجابي","محايد"): score+=4
            if score<max(REVERSAL_MIN_SCORE,int(market.get("required_score",MIN_SCORE))): return None
            swing_low=min(c["low"] for c in closed15[-10:-1]); stop=min(swing_low,price-1.35*atr5)
            mode,setup="reversal","ارتداد ذكي بعد هبوط"
        elif momentum:
            score=20+18+10+10+10+7+7+4+4
            reasons=["اختراق قمة 15m بإغلاق واضح","تأكيد 5m حافظ على مستوى الاختراق",f"حجم انفجاري 15m ×{vr15:.1f}",f"تأكيد حجم 5m ×{vr5:.1f}",f"قوة اتجاه ADX {a15:.0f}",f"RSI زخم مناسب {r15:.1f}","MACD إيجابي على 15m و5m","السعر فوق VWAP","الساعة و4 ساعات لا يعاكسان الزخم"]
            if obv5[-1]>obv5[-5]: score+=6
            if tr15>=1.4: score+=6
            if rel>=0.25: score+=5
            if market.get("regime")=="إيجابي": score+=4
            if score<max(MOMENTUM_MIN_SCORE,int(market.get("required_score",MIN_SCORE))): return None
            recent=min(c["low"] for c in closed5[-10:-1]); stop=min(recent,resistance*0.992,price-1.10*atr5)
            mode,setup="momentum","زخم قوي واختراق 15m"
        else:
            if rise15>MAX_15M_RISE_PCT or rise1>MAX_1H_RISE_PCT or rise4>MAX_4H_RISE_PCT or body>MAX_CANDLE_BODY_PCT or dist>MAX_EMA20_DISTANCE_PCT or greens>MAX_CONSECUTIVE_GREEN: return None
            if not (breakout or retest or squeeze_break) or not (52<=r15<=65) or a15<22 or vr15<MIN_15M_VOLUME_RATIO: return None
            if not (e20_15>e50_15 and e20_15>e20v[-4] and e50_15>=e50v[-4]): return None
            if not (s1["e20"]>s1["e50"] and s1["macd_hist"]>0 and 50<=s1["rsi"]<=68 and s1["adx"]>=18) or not s4["not_bearish"]: return None
            score=(18 if breakout else 0)+(20 if retest else 0)+(14 if squeeze_break else 0)+(12 if vr15>=2 else 0)+(6 if tr15>=1.4 else 0)+(8 if 52<=r15<=62 else 0)+(8 if a15>=25 else 0)+8+10+5+(8 if dist<=0.6 else 0)+(6 if m5>s5 and r5>=r5p else 0)+(5 if obv5[-1]>obv5[-5] else 0)+(7 if rel>=0.8 else 0)+(4 if market.get("regime")=="إيجابي" else 0)
            if score<max(82,int(market.get("required_score",MIN_SCORE))): return None
            reasons=[]
            if breakout: reasons += ["اختراق مؤكد على 15 دقيقة","تأكيد 5m حافظ على مستوى الاختراق"]
            if retest: reasons.append("إعادة اختبار ناجحة على 15 دقيقة")
            if squeeze_break: reasons.append("خروج من انضغاط Bollinger")
            reasons += [f"حجم 15 دقيقة ×{vr15:.1f}",f"RSI 15m مناسب {r15:.1f}",f"ADX 15m {a15:.0f}","تأكيد صاعد على الساعة","4 ساعات لا يعاكس الصفقة"]
            recent=min(c["low"] for c in closed5[-12:-1]); stop=min(recent,support,price-1.25*atr5)
            mode="balanced"; setup="إعادة اختبار 15m" if retest else "انضغاط واختراق 15m" if squeeze_break else "اختراق 15m"
        risk=price-stop
        max_risk = ACCUMULATION_MAX_RISK_PCT if mode=="accumulation" else REVERSAL_MAX_RISK_PCT if mode=="reversal" else MAX_RISK_PCT
        if risk<=0 or risk/price*100>max_risk: return None
        return {"symbol":symbol,"entry":price,"stop":stop,"tp1":price+1.5*risk,"tp2":price+2.2*risk,"tp3":price+3*risk,"risk_pct":risk/price*100,"score":min(score,99),"volume_ratio":vr15,"rsi":r15,"adx":a15,"setup":setup,"mode":mode,"reasons":reasons[:8],"candle_close":candle5["close_time"],"market_regime":market.get("regime","غير متاح"),"btc_1h":float(market.get("btc",{}).get("rise_1h",0)),"btc_15m":float(market.get("btc",{}).get("rise_15m",0)),"btc_rsi15":float(market.get("btc",{}).get("rsi15",0)),"relative_strength":rel}
    except Exception as exc:
        print(f"Analyze {symbol}: {exc}", flush=True); return None


def fmt(v: float) -> str:
    d=2 if v>=1000 else 4 if v>=1 else 5 if v>=0.01 else 8
    return f"{v:.{d}f}"


def signal_message(r: Dict) -> str:
    reasons="\n".join(f"• {x}" for x in r["reasons"]); kind="تجميع قبل الانطلاقة" if r.get("mode")=="accumulation" else "انطلاقة قوية" if r.get("mode")=="momentum" else "ارتداد ذكي" if r.get("mode")=="reversal" else "دخول متوازن"
    return f"🟢 إشارة شراء سبوت — {r['symbol']}\n\nالنموذج: {r['setup']}\nنوع الإشارة: {kind}\nقوة الإشارة: {r['score']}%\nالفريمات: 4h فلتر، 1h تأكيد، 15m قرار، 5m دخول\nحالة السوق: {r['market_regime']} | BTC 15m: {r['btc_15m']:+.2f}% | RSI BTC: {r['btc_rsi15']:.1f}\nBTC ساعة: {r['btc_1h']:+.2f}% | القوة النسبية أمام BTC: {r['relative_strength']:+.2f}%\n\nسعر الشراء التقريبي: {fmt(r['entry'])}\nوقف الخسارة: {fmt(r['stop'])} ({r['risk_pct']:.2f}%)\nالهدف الأول: {fmt(r['tp1'])}\nالهدف الثاني: {fmt(r['tp2'])}\nالهدف الثالث: {fmt(r['tp3'])}\n\nRSI: {r['rsi']:.1f}\nADX: {r['adx']:.1f}\nالحجم: ×{r['volume_ratio']:.1f}\n\nأسباب الإشارة:\n{reasons}\n\n⚠️ تحليل فني آلي وليس ضمانًا للربح."


def load_state():
    try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception: return {"alerts":{}}


def save_state(s): STATE_FILE.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")

def cooled(s,r): return r["candle_close"]-int(s.get("alerts",{}).get(r["symbol"],0))>=COOLDOWN_MINUTES*60*1000


def track_open_signals(state: Dict) -> None:
    if not TRACK_RESULTS: return
    op=state.setdefault("open_signals",{}); stats=state.setdefault("stats",{"tp1":0,"tp2":0,"tp3":0,"stop":0}); now=int(time.time()*1000)
    for symbol,signal in list(op.items()):
        try:
            candles=get_klines(symbol,"5m",100)[:-1]; last=int(signal.get("last_checked",signal["time"])); rel=[c for c in candles if c["close_time"]>last]
            if not rel:
                if now-int(signal["time"])>48*60*60*1000: del op[symbol]
                continue
            reached=int(signal.get("reached",0)); closed=False
            for c in rel:
                if c["low"]<=float(signal["stop"]): stats["stop"]+=1; del op[symbol]; closed=True; break
                if reached<1 and c["high"]>=float(signal["tp1"]): stats["tp1"]+=1; reached=1
                if reached<2 and c["high"]>=float(signal["tp2"]): stats["tp2"]+=1; reached=2
                if reached<3 and c["high"]>=float(signal["tp3"]): stats["tp3"]+=1; del op[symbol]; closed=True; break
                signal["last_checked"]=c["close_time"]
            if not closed: signal["reached"]=reached; signal["last_checked"]=rel[-1]["close_time"]
        except Exception as exc: print(f"Track {symbol}: {exc}", flush=True)


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
    results.sort(key=lambda x:(3 if x.get("mode")=="accumulation" else 2 if x.get("mode")=="momentum" else 1 if x.get("mode")=="reversal" else 0,x["score"],x["volume_ratio"]),reverse=True)
    sent=0
    for r in results[:MAX_ALERTS_PER_SCAN]:
        send_message(signal_message(r)); state.setdefault("alerts",{})[r["symbol"]]=r["candle_close"]
        state.setdefault("open_signals",{})[r["symbol"]]={"time":r["candle_close"],"last_checked":r["candle_close"],"entry":r["entry"],"stop":r["stop"],"tp1":r["tp1"],"tp2":r["tp2"],"tp3":r["tp3"],"reached":0,"mode":r.get("mode","balanced")}
        sent+=1; time.sleep(0.3)
    save_state(state); print(f"Scan finished | market={market.get('regime')} | universe={len(symbols)} | shortlist={len(shortlist)} | candidates={len(results)} | sent={sent}",flush=True)


def main() -> None:
    state=load_state(); send_message("✅ تم تشغيل بوت إشارات الشراء للسبوت V2.4.\nالمسار الأول: دخول متوازن وإعادة اختبار.\nالمسار الثاني: زخم قوي لالتقاط الانطلاقات.\nالمسار الثالث: ارتداد ذكي بعد الهبوط.\nالمسار الرابع: تجميع مبكر قبل الانطلاقة.\nتم تشديد حماية BTC متعددة الفريمات: إيقاف كامل وقت الهبوط القوي، والسماح وقت الضعف فقط للعملات ذات القوة النسبية العالية.\nإشارات فقط — بدون تداول تلقائي وبدون شورت وبدون WATCH.")
    while True:
        started=time.time()
        try: scan(state)
        except Exception as exc: print(f"Scan error: {exc}",flush=True)
        time.sleep(max(5,SCAN_MINUTES*60-(time.time()-started)))



# ============================================================
# إضافة محرك الشورت Futures V1.0
# تمت إضافة هذا القسم دون حذف منطق وإعدادات الشراء الأصلية.
# تمت إضافة البادئة SHORT_ لأسماء محرك الشورت لمنع التعارض.
# ============================================================
import os
import time
import json
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple
import requests
SHORT_BINANCE_BASE = ENGINE_ENV("SHORT", 'BINANCE_FUTURES_BASE', 'https://fapi.binance.com')
SHORT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
SHORT_CHAT_ID = os.environ['TELEGRAM_CHAT_ID']
SHORT_SCAN_MINUTES = int(ENGINE_ENV("SHORT", 'SCAN_MINUTES', '1'))
SHORT_COOLDOWN_MINUTES = int(ENGINE_ENV("SHORT", 'COOLDOWN_MINUTES', '120'))
SHORT_MAX_ALERTS_PER_SCAN = int(ENGINE_ENV("SHORT", 'MAX_ALERTS_PER_SCAN', '5'))
SHORT_MAX_WORKERS = int(ENGINE_ENV("SHORT", 'MAX_WORKERS', '16'))
SHORT_SYMBOL_REFRESH_MINUTES = int(ENGINE_ENV("SHORT", 'SYMBOL_REFRESH_MINUTES', '30'))
SHORT_MIN_DAILY_QUOTE_VOLUME = float(ENGINE_ENV("SHORT", 'MIN_DAILY_QUOTE_VOLUME', '1000000'))
SHORT_PREFILTER_LIMIT = int(ENGINE_ENV("SHORT", 'PREFILTER_LIMIT', '80'))
SHORT_MIN_SCORE = int(ENGINE_ENV("SHORT", 'MIN_SCORE', '84'))
SHORT_MAX_RISK_PCT = float(ENGINE_ENV("SHORT", 'MAX_RISK_PCT', '4.0'))
SHORT_MIN_15M_VOLUME_RATIO = float(ENGINE_ENV("SHORT", 'MIN_15M_VOLUME_RATIO', '1.6'))
SHORT_MAX_RSI_15M_FOR_ENTRY = float(ENGINE_ENV("SHORT", 'MAX_RSI_15M_FOR_ENTRY', '52'))
SHORT_MIN_RSI_15M_HARD = float(ENGINE_ENV("SHORT", 'MIN_RSI_15M_HARD', '30'))
SHORT_MIN_ADX_15M = float(ENGINE_ENV("SHORT", 'MIN_ADX_15M', '22'))
SHORT_MAX_15M_DROP_PCT = float(ENGINE_ENV("SHORT", 'MAX_15M_DROP_PCT', '4.0'))
SHORT_MAX_1H_DROP_PCT = float(ENGINE_ENV("SHORT", 'MAX_1H_DROP_PCT', '9.0'))
SHORT_MAX_4H_DROP_PCT = float(ENGINE_ENV("SHORT", 'MAX_4H_DROP_PCT', '16.0'))
SHORT_MAX_EMA20_DISTANCE_PCT = float(ENGINE_ENV("SHORT", 'MAX_EMA20_DISTANCE_PCT', '2.2'))
SHORT_MAX_CONSECUTIVE_RED = int(ENGINE_ENV("SHORT", 'MAX_CONSECUTIVE_RED', '4'))
SHORT_MAX_CANDLE_BODY_PCT = float(ENGINE_ENV("SHORT", 'MAX_CANDLE_BODY_PCT', '3.5'))
SHORT_BREAKDOWN_CONFIRM_5M = ENGINE_ENV("SHORT", 'BREAKDOWN_CONFIRM_5M', '1') == '1'
SHORT_CONFIRM_MAX_SPIKE_PCT = float(ENGINE_ENV("SHORT", 'CONFIRM_MAX_SPIKE_PCT', '0.8'))
SHORT_MIN_CONFIRM_CLOSE_LOCATION = float(ENGINE_ENV("SHORT", 'MIN_CONFIRM_CLOSE_LOCATION', '0.45'))
SHORT_MAX_BREAKDOWN_RANGE_ATR = float(ENGINE_ENV("SHORT", 'MAX_BREAKDOWN_RANGE_ATR', '2.2'))
SHORT_MAX_BREAKDOWN_BODY_ATR = float(ENGINE_ENV("SHORT", 'MAX_BREAKDOWN_BODY_ATR', '1.8'))
SHORT_MIN_NEXT_SUPPORT_PCT = float(ENGINE_ENV("SHORT", 'MIN_NEXT_SUPPORT_PCT', '2.0'))
SHORT_MOMENTUM_MIN_NEXT_SUPPORT_PCT = float(ENGINE_ENV("SHORT", 'MOMENTUM_MIN_NEXT_SUPPORT_PCT', '1.2'))
SHORT_SUPPORT_MAX_DISTANCE_PCT = float(ENGINE_ENV("SHORT", 'SUPPORT_MAX_DISTANCE_PCT', '18'))
SHORT_SUPPORT_SWING_WINDOW = int(ENGINE_ENV("SHORT", 'SUPPORT_SWING_WINDOW', '2'))
SHORT_SUPPORT_CLUSTER_TOLERANCE_PCT = float(ENGINE_ENV("SHORT", 'SUPPORT_CLUSTER_TOLERANCE_PCT', '0.45'))
SHORT_SUPPORT_MIN_TOUCHES = int(ENGINE_ENV("SHORT", 'SUPPORT_MIN_TOUCHES', '2'))
SHORT_MOMENTUM_SHORT_ENABLED = ENGINE_ENV("SHORT", 'MOMENTUM_SHORT_ENABLED', '1') == '1'
SHORT_MOMENTUM_MIN_VOLUME_15M = float(ENGINE_ENV("SHORT", 'MOMENTUM_MIN_VOLUME_15M', '2.8'))
SHORT_MOMENTUM_MIN_VOLUME_5M = float(ENGINE_ENV("SHORT", 'MOMENTUM_MIN_VOLUME_5M', '1.8'))
SHORT_MOMENTUM_MIN_ADX_15M = float(ENGINE_ENV("SHORT", 'MOMENTUM_MIN_ADX_15M', '27'))
SHORT_MOMENTUM_MIN_RSI_15M = float(ENGINE_ENV("SHORT", 'MOMENTUM_MIN_RSI_15M', '32'))
SHORT_MOMENTUM_MAX_RSI_15M = float(ENGINE_ENV("SHORT", 'MOMENTUM_MAX_RSI_15M', '49'))
SHORT_MOMENTUM_MIN_SCORE = int(ENGINE_ENV("SHORT", 'MOMENTUM_MIN_SCORE', '88'))
SHORT_REJECTION_SHORT_ENABLED = ENGINE_ENV("SHORT", 'REJECTION_SHORT_ENABLED', '1') == '1'
SHORT_REJECTION_MIN_BOUNCE_PCT = float(ENGINE_ENV("SHORT", 'REJECTION_MIN_BOUNCE_PCT', '2.0'))
SHORT_REJECTION_MIN_VOLUME_15M = float(ENGINE_ENV("SHORT", 'REJECTION_MIN_VOLUME_15M', '1.25'))
SHORT_REJECTION_MIN_ADX_15M = float(ENGINE_ENV("SHORT", 'REJECTION_MIN_ADX_15M', '18'))
SHORT_REJECTION_MIN_RSI_15M = float(ENGINE_ENV("SHORT", 'REJECTION_MIN_RSI_15M', '42'))
SHORT_REJECTION_MAX_RSI_15M = float(ENGINE_ENV("SHORT", 'REJECTION_MAX_RSI_15M', '60'))
SHORT_REJECTION_MIN_SCORE = int(ENGINE_ENV("SHORT", 'REJECTION_MIN_SCORE', '84'))
# المحرك الخامس: انعكاس كبير من صعود إلى هبوط — شورت فقط
SHORT_TREND_REVERSAL_ENABLED = ENGINE_ENV("SHORT", 'TREND_REVERSAL_SHORT_ENABLED', '1') == '1'
SHORT_TREND_REVERSAL_MIN_PRIOR_RISE_PCT = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MIN_PRIOR_RISE_PCT', '3.0'))
SHORT_TREND_REVERSAL_MIN_VOLUME_15M = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MIN_VOLUME_15M', '1.35'))
SHORT_TREND_REVERSAL_MIN_VOLUME_5M = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MIN_VOLUME_5M', '1.15'))
SHORT_TREND_REVERSAL_MIN_ADX_15M = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MIN_ADX_15M', '18'))
SHORT_TREND_REVERSAL_MIN_RSI_PEAK = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MIN_RSI_PEAK', '58'))
SHORT_TREND_REVERSAL_MIN_RSI_NOW = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MIN_RSI_NOW', '38'))
SHORT_TREND_REVERSAL_MAX_RSI_NOW = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MAX_RSI_NOW', '54'))
SHORT_TREND_REVERSAL_MIN_SCORE = int(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MIN_SCORE', '88'))
SHORT_TREND_REVERSAL_MAX_RISK_PCT = float(ENGINE_ENV("SHORT", 'TREND_REVERSAL_MAX_RISK_PCT', '4.0'))
SHORT_SMART_MARKET_FILTER = ENGINE_ENV("SHORT", 'SMART_MARKET_FILTER', '1') == '1'
SHORT_BTC_5M_RISE_BLOCK_PCT = float(ENGINE_ENV("SHORT", 'BTC_5M_RISE_BLOCK_PCT', '0.75'))
SHORT_BTC_1H_RISE_BLOCK_PCT = float(ENGINE_ENV("SHORT", 'BTC_1H_RISE_BLOCK_PCT', '1.25'))
SHORT_BTC_15M_RSI_BLOCK = float(ENGINE_ENV("SHORT", 'BTC_15M_RSI_BLOCK', '60'))
SHORT_BTC_BULLISH_RSI_15M = float(ENGINE_ENV("SHORT", 'BTC_BULLISH_RSI_15M', '55'))
SHORT_BTC_BULLISH_MIN_RELATIVE_WEAKNESS = float(ENGINE_ENV("SHORT", 'BTC_BULLISH_MIN_RELATIVE_WEAKNESS', '-1.25'))
SHORT_MARKET_CACHE_SECONDS = int(ENGINE_ENV("SHORT", 'MARKET_CACHE_SECONDS', '55'))
SHORT_TRACK_RESULTS = ENGINE_ENV("SHORT", 'TRACK_RESULTS', '1') == '1'
SHORT_REJECTION_LOG_ENABLED = ENGINE_ENV("SHORT", 'REJECTION_LOG_ENABLED', '1') == '1'
SHORT_REJECTION_LOG_FILE = Path(ENGINE_ENV("SHORT", 'REJECTION_LOG_FILE', 'rejected_short_signals.jsonl'))
SHORT_STATE_FILE = Path(ENGINE_ENV("SHORT", 'STATE_FILE', 'short_signal_state.json'))
SHORT_SESSION = requests.Session()
SHORT_SYMBOL_CACHE = {'symbols': [], 'updated_at': 0.0}
SHORT_MARKET_CACHE = {'data': None, 'updated_at': 0.0}
SHORT_SYMBOL_CACHE_LOCK = Lock()
SHORT_MARKET_CACHE_LOCK = Lock()
SHORT_REJECTION_LOCK = Lock()
SHORT_STABLE_BASES = {'USDC', 'FDUSD', 'TUSD', 'USDP', 'DAI', 'USD1', 'BUSD', 'USDS', 'EUR', 'AEUR', 'EURT', 'TRY', 'BRL', 'GBP', 'AUD', 'BIDR', 'IDRT', 'UAH', 'RUB', 'NGN', 'VAI', 'PAX', 'UST', 'USTC'}
SHORT_EXCLUDED_MAJORS = {value.strip().upper() for value in ENGINE_ENV("SHORT", 'EXCLUDED_MAJORS', '').split(',') if value.strip()}
SHORT_LEVERAGED_SUFFIXES = ('UP', 'DOWN', 'BULL', 'BEAR')

def SHORT_log_rejection(symbol: str, reason: str, details: Optional[Dict]=None) -> None:
    if not SHORT_REJECTION_LOG_ENABLED:
        return
    row = {'time': int(time.time() * 1000), 'symbol': symbol, 'reason': reason, 'details': details or {}}
    print(f"Rejected {symbol}: {reason} | {row['details']}", flush=True)
    try:
        with SHORT_REJECTION_LOCK:
            with SHORT_REJECTION_LOG_FILE.open('a', encoding='utf-8') as file:
                file.write(json.dumps(row, ensure_ascii=False) + '\n')
    except Exception as exc:
        print(f'Rejection log error: {exc}', flush=True)

def SHORT_send_message(text: str) -> None:
    response = SHORT_SESSION.post(f'https://api.telegram.org/bot{SHORT_TOKEN}/sendMessage', json={'chat_id': SHORT_CHAT_ID, 'text': text, 'disable_web_page_preview': True}, timeout=20)
    response.raise_for_status()

# معالجة حجب Binance Futures (HTTP 451) على بعض مزودي الاستضافة.
SHORT_FALLBACK_TO_SPOT = os.getenv('SHORT_FALLBACK_TO_SPOT', '1') == '1'
STOCK_SHORT_YAHOO_FALLBACK = os.getenv('STOCK_SHORT_YAHOO_FALLBACK', '1') == '1'
SHORT_FUTURES_BLOCKED = False
SHORT_FALLBACK_NOTICE_AT = 0.0

def _SHORT_spot_path(path: str) -> Optional[str]:
    return {
        '/fapi/v1/klines': '/api/v3/klines',
        '/fapi/v1/exchangeInfo': '/api/v3/exchangeInfo',
        '/fapi/v1/ticker/24hr': '/api/v3/ticker/24hr',
        '/fapi/v1/depth': '/api/v3/depth',
        '/fapi/v1/aggTrades': '/api/v3/aggTrades',
    }.get(path)

def SHORT_get_json(path: str, params: Optional[Dict]=None, timeout: int=20):
    global SHORT_FUTURES_BLOCKED, SHORT_FALLBACK_NOTICE_AT
    try:
        response = SHORT_SESSION.get(f'{SHORT_BINANCE_BASE}{path}', params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as exc:
        status = getattr(exc.response, 'status_code', None)
        spot_path = _SHORT_spot_path(path)
        if status == 451 and SHORT_FALLBACK_TO_SPOT and spot_path:
            SHORT_FUTURES_BLOCKED = True
            now = time.time()
            if now - SHORT_FALLBACK_NOTICE_AT > 300:
                print('Binance Futures HTTP 451: crypto short switched to Binance Spot market-data fallback.', flush=True)
                SHORT_FALLBACK_NOTICE_AT = now
            response = SHORT_SESSION.get(f'{BINANCE_BASE}{spot_path}', params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if path == '/fapi/v1/exchangeInfo':
                for item in data.get('symbols', []):
                    item.setdefault('contractType', 'PERPETUAL')
            return data
        raise

def _STOCK_yahoo_symbol(symbol: str) -> str:
    value = symbol.upper().strip()
    return value[:-4] if value.endswith('USDT') else value

def _STOCK_yahoo_chart(symbol: str, interval: str, limit: int) -> List[Dict]:
    ticker = _STOCK_yahoo_symbol(symbol)
    yahoo_interval = {'5m': '5m', '15m': '15m', '1h': '60m', '4h': '60m'}.get(interval)
    if not ticker or not yahoo_interval:
        return []
    range_value = '60d' if yahoo_interval in ('5m', '15m') else '2y'
    response = SHORT_SESSION.get(
        f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}',
        params={'interval': yahoo_interval, 'range': range_value, 'includePrePost': 'true', 'events': 'div,splits'},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json().get('chart', {}).get('result') or []
    if not payload:
        return []
    result = payload[0]
    timestamps = result.get('timestamp') or []
    quote = ((result.get('indicators') or {}).get('quote') or [{}])[0]
    opens, highs = quote.get('open') or [], quote.get('high') or []
    lows, closes = quote.get('low') or [], quote.get('close') or []
    volumes = quote.get('volume') or []
    rows = []
    for i, ts in enumerate(timestamps):
        try:
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            if None in (o, h, l, c):
                continue
            v = float(volumes[i] or 0) if i < len(volumes) else 0.0
            rows.append({'open': float(o), 'high': float(h), 'low': float(l), 'close': float(c), 'volume': v, 'close_time': int(ts) * 1000, 'quote_volume': float(c) * v, 'trades': 0})
        except (IndexError, TypeError, ValueError):
            continue
    if interval == '4h':
        grouped = []
        for pos in range(0, len(rows), 4):
            chunk = rows[pos:pos+4]
            if len(chunk) < 4:
                continue
            grouped.append({'open': chunk[0]['open'], 'high': max(x['high'] for x in chunk), 'low': min(x['low'] for x in chunk), 'close': chunk[-1]['close'], 'volume': sum(x['volume'] for x in chunk), 'close_time': chunk[-1]['close_time'], 'quote_volume': sum(x['quote_volume'] for x in chunk), 'trades': 0})
        rows = grouped
    return rows[-limit:]

def SHORT_get_klines(symbol: str, interval: str, limit: int=260) -> List[Dict]:
    if STOCK_SHORT_YAHOO_FALLBACK and not symbol.upper().endswith('USDT'):
        return _STOCK_yahoo_chart(symbol, interval, limit)
    try:
        raw = SHORT_get_json('/fapi/v1/klines', {'symbol': symbol, 'interval': interval, 'limit': limit})
        return [{'open': float(row[1]), 'high': float(row[2]), 'low': float(row[3]), 'close': float(row[4]), 'volume': float(row[5]), 'close_time': int(row[6]), 'quote_volume': float(row[7]), 'trades': int(row[8])} for row in raw]
    except requests.HTTPError as exc:
        if STOCK_SHORT_YAHOO_FALLBACK and getattr(exc.response, 'status_code', None) in (400, 404, 451):
            rows = _STOCK_yahoo_chart(symbol, interval, limit)
            if rows:
                return rows
        raise

def SHORT_get_symbols() -> List[str]:
    now = time.time()
    with SHORT_SYMBOL_CACHE_LOCK:
        if SHORT_SYMBOL_CACHE['symbols'] and now - float(SHORT_SYMBOL_CACHE['updated_at']) < SHORT_SYMBOL_REFRESH_MINUTES * 60:
            return list(SHORT_SYMBOL_CACHE['symbols'])
    exchange = SHORT_get_json('/fapi/v1/exchangeInfo', timeout=30)
    tickers = SHORT_get_json('/fapi/v1/ticker/24hr', timeout=30)
    volume_map = {ticker.get('symbol', ''): float(ticker.get('quoteVolume', 0) or 0) for ticker in tickers}
    rows: List[Tuple[str, float]] = []
    for item in exchange.get('symbols', []):
        symbol = item.get('symbol', '')
        base = item.get('baseAsset', '').upper()
        quote = item.get('quoteAsset', '').upper()
        contract_type = item.get('contractType', '')
        if item.get('status') != 'TRADING' or quote != 'USDT' or contract_type != 'PERPETUAL':
            continue
        if base in SHORT_STABLE_BASES or base in SHORT_EXCLUDED_MAJORS or base.endswith(SHORT_LEVERAGED_SUFFIXES):
            continue
        quote_volume = volume_map.get(symbol, 0.0)
        if quote_volume >= SHORT_MIN_DAILY_QUOTE_VOLUME:
            rows.append((symbol, quote_volume))
    rows.sort(key=lambda item: item[1], reverse=True)
    fresh_symbols = [symbol for symbol, _ in rows]
    with SHORT_SYMBOL_CACHE_LOCK:
        SHORT_SYMBOL_CACHE['symbols'] = fresh_symbols
        SHORT_SYMBOL_CACHE['updated_at'] = now
        return list(SHORT_SYMBOL_CACHE['symbols'])

def SHORT_ema(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period:
        return [None] * len(values)
    output: List[Optional[float]] = [None] * (period - 1)
    current = mean(values[:period])
    output.append(current)
    multiplier = 2 / (period + 1)
    for value in values[period:]:
        current = (value - current) * multiplier + current
        output.append(current)
    return output

def SHORT_rsi(values: List[float], period: int=14) -> List[Optional[float]]:
    if len(values) <= period:
        return [None] * len(values)
    gains: List[float] = []
    losses: List[float] = []
    for index in range(1, len(values)):
        change = values[index] - values[index - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    average_gain = mean(gains[:period])
    average_loss = mean(losses[:period])
    output: List[Optional[float]] = [None] * period

    def calculate(gain: float, loss: float) -> float:
        return 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)
    output.append(calculate(average_gain, average_loss))
    for index in range(period, len(gains)):
        average_gain = (average_gain * (period - 1) + gains[index]) / period
        average_loss = (average_loss * (period - 1) + losses[index]) / period
        output.append(calculate(average_gain, average_loss))
    return output

def SHORT_macd(values: List[float]):
    if len(values) < 35:
        return (None, None, None)
    fast = SHORT_ema(values, 12)
    slow = SHORT_ema(values, 26)
    series = [a - b for a, b in zip(fast, slow) if a is not None and b is not None]
    if len(series) < 9:
        return (None, None, None)
    signal = SHORT_ema(series, 9)[-1]
    if signal is None:
        return (None, None, None)
    return (series[-1], signal, series[-1] - signal)

def SHORT_atr(candles: List[Dict], period: int=14) -> float:
    true_ranges: List[float] = []
    for index, candle in enumerate(candles):
        if index == 0:
            true_ranges.append(candle['high'] - candle['low'])
        else:
            previous_close = candles[index - 1]['close']
            true_ranges.append(max(candle['high'] - candle['low'], abs(candle['high'] - previous_close), abs(candle['low'] - previous_close)))
    return mean(true_ranges[-period:]) if true_ranges else 0.0

def SHORT_adx(candles: List[Dict], period: int=14) -> float:
    if len(candles) < period + 2:
        return 0.0
    true_ranges: List[float] = []
    plus_dm: List[float] = []
    minus_dm: List[float] = []
    for index in range(1, len(candles)):
        candle = candles[index]
        previous = candles[index - 1]
        up = candle['high'] - previous['high']
        down = previous['low'] - candle['low']
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        true_ranges.append(max(candle['high'] - candle['low'], abs(candle['high'] - previous['close']), abs(candle['low'] - previous['close'])))
    dx_values: List[float] = []
    for end in range(period, len(true_ranges) + 1):
        true_sum = sum(true_ranges[end - period:end])
        if true_sum <= 0:
            continue
        plus_di = 100 * sum(plus_dm[end - period:end]) / true_sum
        minus_di = 100 * sum(minus_dm[end - period:end]) / true_sum
        if plus_di + minus_di > 0:
            dx_values.append(100 * abs(plus_di - minus_di) / (plus_di + minus_di))
    return mean(dx_values[-period:]) if dx_values else 0.0

def SHORT_vwap(candles: List[Dict], period: int=20) -> float:
    window = candles[-period:]
    total_volume = sum((candle['volume'] for candle in window))
    if total_volume <= 0:
        return window[-1]['close']
    return sum(((candle['high'] + candle['low'] + candle['close']) / 3 * candle['volume'] for candle in window)) / total_volume

def SHORT_obv(candles: List[Dict]) -> List[float]:
    output = [0.0]
    for index in range(1, len(candles)):
        if candles[index]['close'] > candles[index - 1]['close']:
            output.append(output[-1] + candles[index]['volume'])
        elif candles[index]['close'] < candles[index - 1]['close']:
            output.append(output[-1] - candles[index]['volume'])
        else:
            output.append(output[-1])
    return output

def SHORT_bollinger(values: List[float], period: int=20, multiplier: float=2.0):
    window = values[-period:]
    middle = mean(window)
    deviation = pstdev(window)
    upper = middle + multiplier * deviation
    lower = middle - multiplier * deviation
    width = (upper - lower) / middle * 100 if middle else 0.0
    return (middle, upper, lower, width)

def SHORT_pct_change(old: float, new: float) -> float:
    return (new / old - 1) * 100 if old else 0.0

def SHORT_close_location(candle: Dict) -> float:
    span = candle['high'] - candle['low']
    return (candle['close'] - candle['low']) / span if span > 0 else 0.5

def SHORT_consecutive_red(candles: List[Dict], lookback: int=7) -> int:
    count = 0
    for candle in reversed(candles[-lookback:]):
        if candle['close'] < candle['open']:
            count += 1
        else:
            break
    return count

def SHORT_recent_bounce_pct(candles: List[Dict], lookback: int=18) -> float:
    if len(candles) < lookback:
        return 0.0
    window = candles[-lookback:]
    trough = min((candle['low'] for candle in window[:-1]))
    peak = max((candle['high'] for candle in window))
    return max(0.0, SHORT_pct_change(trough, peak))

def SHORT_lower_high_structure(candles: List[Dict], lookback: int=18) -> bool:
    if len(candles) < lookback + 4:
        return False
    window = candles[-lookback:]
    first = window[:lookback // 2]
    second = window[lookback // 2:-1]
    if not first or not second:
        return False
    high1 = max((candle['high'] for candle in first))
    high2 = max((candle['high'] for candle in second))
    short_low = min((candle['low'] for candle in window[-7:-2]))
    return high2 < high1 and window[-1]['close'] <= short_low * 1.003

def SHORT_nearest_below_support(candles: List[Dict], price: float, lookback: int=100) -> Optional[Dict]:
    if price <= 0 or len(candles) < SHORT_SUPPORT_SWING_WINDOW * 2 + 5:
        return None
    sample = candles[-lookback:]
    swing_levels: List[float] = []
    window = max(1, SHORT_SUPPORT_SWING_WINDOW)
    for index in range(window, len(sample) - window):
        low = float(sample[index]['low'])
        neighbors = sample[index - window:index] + sample[index + 1:index + window + 1]
        if low <= min((float(candle['low']) for candle in neighbors)):
            distance_pct = -SHORT_pct_change(price, low)
            if 0 < distance_pct <= SHORT_SUPPORT_MAX_DISTANCE_PCT:
                swing_levels.append(low)
    if not swing_levels:
        return None
    swing_levels.sort()
    clusters: List[List[float]] = []
    for level in swing_levels:
        placed = False
        for cluster in clusters:
            center = mean(cluster)
            if abs(level / center - 1) * 100 <= SHORT_SUPPORT_CLUSTER_TOLERANCE_PCT:
                cluster.append(level)
                placed = True
                break
        if not placed:
            clusters.append([level])
    valid: List[Dict] = []
    for cluster in clusters:
        if len(cluster) < SHORT_SUPPORT_MIN_TOUCHES:
            continue
        level = mean(cluster)
        distance_pct = -SHORT_pct_change(price, level)
        if 0 < distance_pct <= SHORT_SUPPORT_MAX_DISTANCE_PCT:
            valid.append({'level': level, 'distance_pct': distance_pct, 'touches': len(cluster)})
    return min(valid, key=lambda item: item['distance_pct']) if valid else None

def SHORT_frame_snapshot(candles: List[Dict]) -> Optional[Dict]:
    closed = candles[:-1]
    if len(closed) < 210:
        return None
    closes = [candle['close'] for candle in closed]
    ema20_values = SHORT_ema(closes, 20)
    ema50_values = SHORT_ema(closes, 50)
    ema200_values = SHORT_ema(closes, 200)
    ema20 = ema20_values[-1]
    ema50 = ema50_values[-1]
    ema200 = ema200_values[-1]
    rsi_value = SHORT_rsi(closes)[-1]
    _, _, histogram = SHORT_macd(closes)
    if None in (ema20, ema50, ema200, rsi_value, histogram):
        return None
    price = closes[-1]
    strongly_bullish = price > ema20 > ema50 and price > ema200 and (histogram > 0) and (ema20 >= ema20_values[-4])
    strongly_bearish = price < ema20 < ema50 and price < ema200 and (histogram < 0) and (ema20 <= ema20_values[-4])
    return {'price': price, 'e20': ema20, 'e50': ema50, 'e200': ema200, 'rsi': rsi_value, 'macd_hist': histogram, 'adx': SHORT_adx(closed), 'strongly_bullish': strongly_bullish, 'strongly_bearish': strongly_bearish, 'not_bullish': not strongly_bullish}

def SHORT_market_context() -> Dict:
    now = time.time()
    with SHORT_MARKET_CACHE_LOCK:
        if SHORT_MARKET_CACHE['data'] and now - float(SHORT_MARKET_CACHE['updated_at']) < SHORT_MARKET_CACHE_SECONDS:
            return dict(SHORT_MARKET_CACHE['data'])
    try:

        def snapshot(symbol: str) -> Dict:
            c5 = SHORT_get_klines(symbol, '5m', 120)[:-1]
            c15 = SHORT_get_klines(symbol, '15m', 120)[:-1]
            c1h = SHORT_get_klines(symbol, '1h', 120)[:-1]
            c4h = SHORT_get_klines(symbol, '4h', 120)[:-1]
            z = [candle['close'] for candle in c5]
            a = [candle['close'] for candle in c15]
            b = [candle['close'] for candle in c1h]
            d = [candle['close'] for candle in c4h]
            e20z, e50z = (SHORT_ema(z, 20)[-1], SHORT_ema(z, 50)[-1])
            e20a, e50a = (SHORT_ema(a, 20)[-1], SHORT_ema(a, 50)[-1])
            e20b, e50b = (SHORT_ema(b, 20)[-1], SHORT_ema(b, 50)[-1])
            e20d, e50d = (SHORT_ema(d, 20)[-1], SHORT_ema(d, 50)[-1])
            _, _, hz = SHORT_macd(z)
            _, _, ha = SHORT_macd(a)
            _, _, hb = SHORT_macd(b)
            _, _, hd = SHORT_macd(d)
            rz, ra, rb, rd = (SHORT_rsi(z)[-1], SHORT_rsi(a)[-1], SHORT_rsi(b)[-1], SHORT_rsi(d)[-1])
            return {'rise_5m': SHORT_pct_change(z[-2], z[-1]), 'rise_15m': SHORT_pct_change(z[-4], z[-1]), 'rise_1h': SHORT_pct_change(a[-5], a[-1]), 'rise_4h': SHORT_pct_change(b[-5], b[-1]), 'rsi5': rz, 'rsi15': ra, 'rsi1h': rb, 'rsi4h': rd, 'bull5': z[-1] > e20z > e50z and hz >= 0, 'bear5': z[-1] < e20z and hz < 0, 'bull15': a[-1] > e20a > e50a and ha >= 0, 'bear15': a[-1] < e20a and ha < 0, 'bull1h': b[-1] > e20b > e50b and hb >= 0, 'bear1h': b[-1] < e20b and hb < 0, 'bull4h': d[-1] > e20d > e50d and hd >= 0, 'bear4h': d[-1] < e20d < e50d and hd < 0}
        btc = snapshot('BTCUSDT')
        eth = snapshot('ETHUSDT')
        points = (-2 if btc['bull4h'] else 2 if btc['bear4h'] else 0) + (-2 if btc['bull1h'] else 2 if btc['bear1h'] else 0) + (-1 if btc['bull15'] else 1 if btc['bear15'] else 0) + (-1 if eth['bull1h'] else 1 if eth['bear1h'] else 0) + (-1 if eth['bull15'] else 1 if eth['bear15'] else 0)
        sudden_rise = btc['rise_5m'] >= SHORT_BTC_5M_RISE_BLOCK_PCT and btc['bull5'] and (btc['rsi15'] > SHORT_BTC_15M_RSI_BLOCK)
        trend_break_up = btc['rise_1h'] >= SHORT_BTC_1H_RISE_BLOCK_PCT and btc['bull15'] and btc['bull1h']
        bullish_pressure = bool(SHORT_SMART_MARKET_FILTER and btc['rsi15'] >= SHORT_BTC_BULLISH_RSI_15M and (btc['bull5'] or btc['bull15']))
        hard_block = bool(SHORT_SMART_MARKET_FILTER and (sudden_rise or trend_break_up or (btc['bull4h'] and btc['bull1h'] and btc['bull15'])))
        regime = 'هابط قوي' if points >= 4 else 'هابط' if points >= 2 else 'صاعد قوي' if hard_block or points <= -4 else 'صاعد' if points <= -2 or bullish_pressure else 'محايد'
        bonus = 0 if regime in ('هابط قوي', 'هابط') else 4 if regime == 'محايد' else 8
        data = {'regime': regime, 'btc': btc, 'eth': eth, 'required_score': SHORT_MIN_SCORE + bonus, 'hard_block': hard_block, 'bullish_pressure': bullish_pressure}
    except Exception as exc:
        print(f'Market filter error: {exc}', flush=True)
        data = {'regime': 'غير متاح', 'btc': {'rise_1h': 0, 'rise_15m': 0, 'rise_5m': 0, 'rsi15': 50}, 'eth': {}, 'required_score': SHORT_MIN_SCORE + 4, 'hard_block': False, 'bullish_pressure': False}
    with SHORT_MARKET_CACHE_LOCK:
        SHORT_MARKET_CACHE['data'] = data
        SHORT_MARKET_CACHE['updated_at'] = now
    return dict(data)

def SHORT_prefilter_symbol(symbol: str) -> Optional[Tuple[str, float]]:
    try:
        candles = SHORT_get_klines(symbol, '5m', 90)[:-1]
        if len(candles) < 60:
            return None
        closes = [candle['close'] for candle in candles]
        volumes = [candle['volume'] for candle in candles]
        average_volume = mean(volumes[-21:-1])
        volume_ratio = volumes[-1] / average_volume if average_volume else 0.0
        ema20 = SHORT_ema(closes, 20)[-1]
        support = min((candle['low'] for candle in candles[-21:-1]))
        proximity = support / closes[-1] if closes[-1] else 0.0
        negative_move = max(-SHORT_pct_change(closes[-7], closes[-1]), 0)
        score = volume_ratio * 34 + negative_move * 7 + max(proximity - 0.965, 0) * 230
        if ema20 and closes[-1] > ema20 * 1.015:
            score -= 20
        # لا نهمل الانعكاسات المبكرة بعد موجة صعود: ضعف تحت EMA20 مع RSI وMACD هابطين.
        if SHORT_TREND_REVERSAL_ENABLED and ema20:
            rsi_values = SHORT_rsi(closes)
            _, _, histogram = SHORT_macd(closes)
            prior_low = min(closes[-36:-12]) if len(closes) >= 36 else min(closes[:-12])
            prior_high = max(closes[-18:-3])
            prior_rise = SHORT_pct_change(prior_low, prior_high) if prior_low > 0 else 0.0
            if prior_rise >= SHORT_TREND_REVERSAL_MIN_PRIOR_RISE_PCT and closes[-1] < ema20 and histogram is not None and histogram < 0:
                score += 34
                if rsi_values[-1] is not None and rsi_values[-4] is not None and rsi_values[-1] < rsi_values[-4]:
                    score += 16
        return (symbol, score)
    except Exception as exc:
        print(f'Prefilter {symbol}: {exc}', flush=True)
        return None

def SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    try:
        c5, c15, c1h, c4h = [SHORT_get_klines(symbol, interval, 260) for interval in ('5m', '15m', '1h', '4h')]
        closed5 = c5[:-1]
        closed15 = c15[:-1]
        if min(len(closed5), len(closed15), len(c1h) - 1, len(c4h) - 1) < 210:
            return None
        closes5 = [candle['close'] for candle in closed5]
        volumes5 = [candle['volume'] for candle in closed5]
        price = closes5[-1]
        candle5 = closed5[-1]
        e20_5 = SHORT_ema(closes5, 20)[-1]
        rsi5_values = SHORT_rsi(closes5)
        rsi5_now, rsi5_previous = (rsi5_values[-1], rsi5_values[-2])
        macd5, signal5, hist5 = SHORT_macd(closes5)
        if None in (e20_5, rsi5_now, rsi5_previous, macd5, signal5, hist5):
            return None
        atr5 = SHORT_atr(closed5)
        vwap5 = SHORT_vwap(closed5)
        obv5 = SHORT_obv(closed5)
        average_volume5 = mean(volumes5[-21:-1])
        volume_ratio5 = volumes5[-1] / average_volume5 if average_volume5 else 0.0
        distance_ema20 = abs(price / e20_5 - 1) * 100 if e20_5 else 999
        drop15 = -SHORT_pct_change(closed5[-4]['close'], price)
        drop1h = -SHORT_pct_change(closed5[-13]['close'], price)
        drop4h = -SHORT_pct_change(closed5[-49]['close'], price)
        candle_body_pct = abs(candle5['close'] - candle5['open']) / candle5['open'] * 100 if candle5['open'] else 0
        red_count = SHORT_consecutive_red(closed5)
        if not (price < e20_5 and price < vwap5 and (hist5 <= 0)):
            return None
        closes15 = [candle['close'] for candle in closed15]
        volumes15 = [candle['volume'] for candle in closed15]
        trades15 = [candle['trades'] for candle in closed15]
        candle15 = closed15[-1]
        previous15 = closed15[-2]
        e20_values15 = SHORT_ema(closes15, 20)
        e50_values15 = SHORT_ema(closes15, 50)
        e20_15 = e20_values15[-1]
        e50_15 = e50_values15[-1]
        rsi15_values = SHORT_rsi(closes15)
        rsi15_now = rsi15_values[-1]
        macd15, signal15, hist15 = SHORT_macd(closes15)
        adx15 = SHORT_adx(closed15)
        if None in (e20_15, e50_15, rsi15_now, macd15, signal15, hist15):
            return None
        average_volume15 = mean(volumes15[-21:-1])
        volume_ratio15 = volumes15[-1] / average_volume15 if average_volume15 else 0.0
        average_trades15 = mean(trades15[-21:-1])
        trades_ratio15 = trades15[-1] / average_trades15 if average_trades15 else 0.0
        support = min((candle['low'] for candle in closed15[-22:-2]))
        resistance = max((candle['high'] for candle in closed15[-12:-1]))
        atr15 = SHORT_atr(closed15)
        breakdown_range = candle15['high'] - candle15['low']
        breakdown_body = abs(candle15['close'] - candle15['open'])
        breakdown = candle15['close'] < support and candle15['close'] < candle15['open'] and (volume_ratio15 >= SHORT_MIN_15M_VOLUME_RATIO)
        retest = previous15['close'] < support and candle15['high'] >= support * 0.996 and (candle15['high'] <= support * 1.01) and (candle15['close'] < support) and (candle15['close'] < candle15['open'])
        widths = [SHORT_bollinger(closes15[:-offset])[3] for offset in range(40, 0, -1) if len(closes15[:-offset]) >= 20]
        _, _, lower_band, current_width = SHORT_bollinger(closes15)
        squeeze = bool(widths) and current_width <= sorted(widths)[max(0, int(len(widths) * 0.3) - 1)]
        squeeze_break = squeeze and candle15['close'] <= lower_band * 1.005 and (volume_ratio15 >= SHORT_MIN_15M_VOLUME_RATIO)
        snapshot1h = SHORT_frame_snapshot(c1h)
        snapshot4h = SHORT_frame_snapshot(c4h)
        if not snapshot1h or not snapshot4h:
            return None
        if rsi15_now < SHORT_MIN_RSI_15M_HARD:
            SHORT_log_rejection(symbol, 'RSI منخفض جدًا للشورت', {'rsi15': round(rsi15_now, 2), 'limit': SHORT_MIN_RSI_15M_HARD})
            return None
        if breakdown and atr15 > 0 and (breakdown_range > SHORT_MAX_BREAKDOWN_RANGE_ATR * atr15 or breakdown_body > SHORT_MAX_BREAKDOWN_BODY_ATR * atr15):
            SHORT_log_rejection(symbol, 'شمعة انهيار استنزافية', {'range_atr': round(breakdown_range / atr15, 2), 'body_atr': round(breakdown_body / atr15, 2)})
            return None
        if SHORT_BREAKDOWN_CONFIRM_5M and breakdown:
            after_breakdown = candle5['close_time'] > candle15['close_time']
            held_below = candle5['close'] < support and candle5['high'] <= support * (1 + SHORT_CONFIRM_MAX_SPIKE_PCT / 100)
            healthy_close = candle5['close'] <= candle5['open'] and SHORT_close_location(candle5) <= SHORT_MIN_CONFIRM_CLOSE_LOCATION
            if not (after_breakdown and held_below and healthy_close and (hist5 <= 0)):
                SHORT_log_rejection(symbol, 'فشل تأكيد الكسر 5m', {'after_breakdown': after_breakdown, 'held_below': held_below, 'healthy_close': healthy_close, 'confirm_close': candle5['close'], 'support': support})
                return None
        bounce15 = SHORT_recent_bounce_pct(closed15, 18)
        lower_high = SHORT_lower_high_structure(closed5, 18)
        touched_resistance = candle15['high'] >= min(e20_15, e50_15) * 0.995
        rejection_candle = candle15['close'] < candle15['open'] and SHORT_close_location(candle15) <= 0.45
        rejection_short = SHORT_REJECTION_SHORT_ENABLED and bounce15 >= SHORT_REJECTION_MIN_BOUNCE_PCT and (SHORT_REJECTION_MIN_RSI_15M <= rsi15_now <= SHORT_REJECTION_MAX_RSI_15M) and touched_resistance and rejection_candle and lower_high and (volume_ratio15 >= SHORT_REJECTION_MIN_VOLUME_15M) and (adx15 >= SHORT_REJECTION_MIN_ADX_15M) and (hist5 <= 0) and (not snapshot1h['strongly_bullish']) and (not snapshot4h['strongly_bullish'])

        # المحرك الخامس: يلتقط تحوّل موجة صاعدة إلى هبوط قبل اكتمال الترند الهابط.
        reversal_window = closed15[-28:-2]
        reversal_first = reversal_window[:12]
        reversal_second = reversal_window[12:]
        prior_swing_low = min(candle['low'] for candle in reversal_first) if reversal_first else price
        prior_swing_high = max(candle['high'] for candle in reversal_second) if reversal_second else price
        prior_rise_pct = SHORT_pct_change(prior_swing_low, prior_swing_high) if prior_swing_low > 0 else 0.0
        recent_break_level = min(candle['low'] for candle in closed15[-8:-2])
        broke_recent_low = candle15['close'] < recent_break_level and candle15['close'] < candle15['open']
        rsi_recent_peak = max(value for value in rsi15_values[-14:-2] if value is not None)
        rsi_rolling_over = rsi15_now < rsi15_values[-3] and rsi15_values[-3] < rsi15_values[-5]
        macd_turn_down = hist15 < 0 and hist15 < SHORT_macd(closes15[:-2])[2] and hist5 <= 0
        reversal_confirm_5m = candle5['close_time'] > candle15['close_time'] and candle5['close'] < recent_break_level and candle5['close'] <= candle5['open'] and SHORT_close_location(candle5) <= 0.48
        trend_reversal_short = bool(
            SHORT_TREND_REVERSAL_ENABLED
            and prior_rise_pct >= SHORT_TREND_REVERSAL_MIN_PRIOR_RISE_PCT
            and lower_high and broke_recent_low and reversal_confirm_5m
            and rsi_recent_peak >= SHORT_TREND_REVERSAL_MIN_RSI_PEAK
            and SHORT_TREND_REVERSAL_MIN_RSI_NOW <= rsi15_now <= SHORT_TREND_REVERSAL_MAX_RSI_NOW
            and rsi_rolling_over and macd_turn_down
            and volume_ratio15 >= SHORT_TREND_REVERSAL_MIN_VOLUME_15M
            and volume_ratio5 >= SHORT_TREND_REVERSAL_MIN_VOLUME_5M
            and adx15 >= SHORT_TREND_REVERSAL_MIN_ADX_15M
            and price < e20_5 and price < vwap5
            and not snapshot1h['strongly_bullish']
        )
        momentum_short = SHORT_MOMENTUM_SHORT_ENABLED and candle15['close'] < support and (candle15['close'] < candle15['open']) and (volume_ratio15 >= SHORT_MOMENTUM_MIN_VOLUME_15M) and (volume_ratio5 >= SHORT_MOMENTUM_MIN_VOLUME_5M) and (adx15 >= SHORT_MOMENTUM_MIN_ADX_15M) and (SHORT_MOMENTUM_MIN_RSI_15M <= rsi15_now <= SHORT_MOMENTUM_MAX_RSI_15M) and (hist15 < 0) and (hist5 <= 0) and (e20_15 < e20_values15[-4]) and (not snapshot1h['strongly_bullish']) and (not snapshot4h['strongly_bullish']) and (drop15 <= SHORT_MAX_15M_DROP_PCT) and (drop1h <= SHORT_MAX_1H_DROP_PCT) and (distance_ema20 <= SHORT_MAX_EMA20_DISTANCE_PCT) and (red_count <= SHORT_MAX_CONSECUTIVE_RED) and (candle_body_pct <= SHORT_MAX_CANDLE_BODY_PCT)
        relative_weakness = SHORT_pct_change(closed5[-13]['close'], price) - float(market.get('btc', {}).get('rise_1h', 0))
        if market.get('hard_block', False):
            SHORT_log_rejection(symbol, 'إيقاف الشورت بسبب صعود BTC', {'btc5': round(float(market.get('btc', {}).get('rise_5m', 0)), 3), 'btc15': round(float(market.get('btc', {}).get('rise_15m', 0)), 3), 'btc1h': round(float(market.get('btc', {}).get('rise_1h', 0)), 3)})
            return None
        if market.get('bullish_pressure', False) and relative_weakness > SHORT_BTC_BULLISH_MIN_RELATIVE_WEAKNESS:
            SHORT_log_rejection(symbol, 'BTC صاعد والعملة ليست أضعف بما يكفي', {'relative_weakness': round(relative_weakness, 2), 'required': SHORT_BTC_BULLISH_MIN_RELATIVE_WEAKNESS})
            return None
        below_support = SHORT_nearest_below_support(c1h[:-1], price, 100)
        if below_support:
            room = float(below_support['distance_pct'])
            required_room = SHORT_MOMENTUM_MIN_NEXT_SUPPORT_PCT if momentum_short or rejection_short or trend_reversal_short else SHORT_MIN_NEXT_SUPPORT_PCT
            if room < required_room:
                SHORT_log_rejection(symbol, 'دعم قريب أسفل الشورت', {'distance_pct': round(room, 2), 'required_pct': required_room, 'support': round(float(below_support['level']), 10), 'touches': int(below_support['touches'])})
                return None
        if trend_reversal_short:
            score = 22 + 16 + 14 + 12 + 10 + 8 + 7
            reasons = [
                f'انعكاس بعد موجة صعود {prior_rise_pct:.1f}%',
                'تكوّنت قمة أدنى على 5m',
                'كسر القاع القصير على 15m',
                'تأكيد 5m حافظ أسفل مستوى الكسر',
                f'RSI هبط من قمة {rsi_recent_peak:.1f} إلى {rsi15_now:.1f}',
                'MACD تحوّل سلبي على 15m و5m',
                f'حجم بيع 15m ×{volume_ratio15:.1f}',
                'السعر أسفل EMA20 وVWAP على 5m',
            ]
            if obv5[-1] < obv5[-5]:
                score += 6
            if trades_ratio15 >= 1.25:
                score += 5
            if relative_weakness <= -0.35:
                score += 5
            if market.get('regime') in ('هابط', 'هابط قوي'):
                score += 4
            if snapshot4h['strongly_bullish']:
                score -= 8
            if score < max(SHORT_TREND_REVERSAL_MIN_SCORE, int(market.get('required_score', SHORT_MIN_SCORE))):
                return None
            reversal_high = max(candle['high'] for candle in closed15[-10:-1])
            recent_high = max(candle['high'] for candle in closed5[-12:-1])
            stop = max(recent_high, reversal_high, price + 1.20 * atr5)
            mode = 'trend_reversal_short'
            setup = 'انعكاس كبير من صعود إلى هبوط'
        elif rejection_short:
            score = 22 + 16 + 12 + 10 + 8 + 8 + 6
            reasons = [f'فشل ارتداد بعد صعود {bounce15:.1f}%', 'رفض من EMA20/EMA50 على 15m', 'تحول هيكل 5m إلى قمة أدنى', f'حجم رفض 15m ×{volume_ratio15:.1f}', f'ADX 15m {adx15:.0f}', f'RSI مناسب للشورت {rsi15_now:.1f}', 'السعر أسفل VWAP على 5m']
            if obv5[-1] < obv5[-5]:
                score += 6
            if relative_weakness <= -0.5:
                score += 6
            if market.get('regime') in ('هابط', 'هابط قوي'):
                score += 5
            if score < max(SHORT_REJECTION_MIN_SCORE, int(market.get('required_score', SHORT_MIN_SCORE))):
                return None
            recent_high = max((candle['high'] for candle in closed5[-12:-1]))
            stop = max(recent_high, resistance, price + 1.25 * atr5)
            mode = 'rejection_short'
            setup = 'فشل ارتداد إلى مقاومة'
        elif momentum_short:
            score = 22 + 18 + 12 + 10 + 10 + 8 + 7 + 5
            reasons = ['كسر دعم 15m بإغلاق واضح', 'تأكيد 5m حافظ أسفل مستوى الكسر', f'حجم انهيار 15m ×{volume_ratio15:.1f}', f'تأكيد حجم 5m ×{volume_ratio5:.1f}', f'قوة اتجاه ADX {adx15:.0f}', f'RSI مناسب للشورت {rsi15_now:.1f}', 'MACD سلبي على 15m و5m', 'السعر أسفل VWAP']
            if obv5[-1] < obv5[-5]:
                score += 6
            if trades_ratio15 >= 1.4:
                score += 6
            if relative_weakness <= -0.25:
                score += 5
            if market.get('regime') in ('هابط', 'هابط قوي'):
                score += 4
            if score < max(SHORT_MOMENTUM_MIN_SCORE, int(market.get('required_score', SHORT_MIN_SCORE))):
                return None
            recent_high = max((candle['high'] for candle in closed5[-10:-1]))
            stop = max(recent_high, support * 1.008, price + 1.1 * atr5)
            mode = 'momentum_short'
            setup = 'انهيار قوي وكسر دعم 15m'
        else:
            if drop15 > SHORT_MAX_15M_DROP_PCT or drop1h > SHORT_MAX_1H_DROP_PCT or drop4h > SHORT_MAX_4H_DROP_PCT or (candle_body_pct > SHORT_MAX_CANDLE_BODY_PCT) or (distance_ema20 > SHORT_MAX_EMA20_DISTANCE_PCT) or (red_count > SHORT_MAX_CONSECUTIVE_RED):
                return None
            if not (breakdown or retest or squeeze_break):
                return None
            if not SHORT_MIN_RSI_15M_HARD <= rsi15_now <= SHORT_MAX_RSI_15M_FOR_ENTRY:
                return None
            if adx15 < SHORT_MIN_ADX_15M or volume_ratio15 < SHORT_MIN_15M_VOLUME_RATIO:
                return None
            if not (e20_15 < e50_15 and e20_15 < e20_values15[-4] and (e50_15 <= e50_values15[-4])):
                return None
            if not (snapshot1h['e20'] < snapshot1h['e50'] and snapshot1h['macd_hist'] < 0 and (32 <= snapshot1h['rsi'] <= 52) and (snapshot1h['adx'] >= 18)):
                return None
            if snapshot4h['strongly_bullish']:
                return None
            score = (20 if breakdown else 0) + (22 if retest else 0) + (14 if squeeze_break else 0) + (12 if volume_ratio15 >= 2 else 0) + (6 if trades_ratio15 >= 1.4 else 0) + (8 if 35 <= rsi15_now <= 48 else 0) + (8 if adx15 >= 25 else 0) + 10 + 10 + (8 if distance_ema20 <= 0.7 else 0) + (6 if macd5 < signal5 and rsi5_now <= rsi5_previous else 0) + (5 if obv5[-1] < obv5[-5] else 0) + (7 if relative_weakness <= -0.8 else 0) + (4 if market.get('regime') in ('هابط', 'هابط قوي') else 0)
            if score < max(SHORT_MIN_SCORE, int(market.get('required_score', SHORT_MIN_SCORE))):
                return None
            reasons: List[str] = []
            if breakdown:
                reasons += ['كسر دعم مؤكد على 15 دقيقة', 'تأكيد 5m حافظ أسفل مستوى الكسر']
            if retest:
                reasons.append('إعادة اختبار فاشلة للدعم المكسور')
            if squeeze_break:
                reasons.append('كسر هابط بعد انضغاط Bollinger')
            reasons += [f'حجم 15 دقيقة ×{volume_ratio15:.1f}', f'RSI 15m مناسب {rsi15_now:.1f}', f'ADX 15m {adx15:.0f}', 'تأكيد هابط على الساعة', '4 ساعات لا يعاكس الشورت بقوة']
            recent_high = max((candle['high'] for candle in closed5[-12:-1]))
            stop = max(recent_high, resistance, price + 1.25 * atr5)
            mode = 'balanced_short'
            setup = 'إعادة اختبار هابطة 15m' if retest else 'انضغاط وكسر هابط 15m' if squeeze_break else 'كسر دعم 15m'
        risk = stop - price
        max_risk_pct = SHORT_TREND_REVERSAL_MAX_RISK_PCT if mode == 'trend_reversal_short' else SHORT_MAX_RISK_PCT
        if risk <= 0 or risk / price * 100 > max_risk_pct:
            return None
        return {'symbol': symbol, 'entry': price, 'stop': stop, 'tp1': price - 1.5 * risk, 'tp2': price - 2.2 * risk, 'tp3': price - 3.0 * risk, 'risk_pct': risk / price * 100, 'score': min(score, 99), 'volume_ratio': volume_ratio15, 'rsi': rsi15_now, 'adx': adx15, 'setup': setup, 'mode': mode, 'reasons': reasons[:8], 'candle_close': candle5['close_time'], 'market_regime': market.get('regime', 'غير متاح'), 'btc_1h': float(market.get('btc', {}).get('rise_1h', 0)), 'btc_15m': float(market.get('btc', {}).get('rise_15m', 0)), 'btc_rsi15': float(market.get('btc', {}).get('rsi15', 0)), 'relative_weakness': relative_weakness}
    except Exception as exc:
        print(f'Analyze {symbol}: {exc}', flush=True)
        return None

def SHORT_fmt(value: float) -> str:
    decimals = 2 if value >= 1000 else 4 if value >= 1 else 5 if value >= 0.01 else 8
    return f'{value:.{decimals}f}'

def SHORT_signal_message(result: Dict) -> str:
    reasons = '\n'.join((f'• {reason}' for reason in result['reasons']))
    kind = 'انعكاس ترند هابط مبكر' if result.get('mode') == 'trend_reversal_short' else 'انهيار قوي' if result.get('mode') == 'momentum_short' else 'فشل ارتداد' if result.get('mode') == 'rejection_short' else 'دخول شورت متوازن'
    return f"🔴 إشارة شورت — {result['symbol']}\n\nالنموذج: {result['setup']}\nنوع الإشارة: {kind}\nقوة الإشارة: {result['score']}%\nالفريمات: 4h فلتر، 1h تأكيد، 15m قرار، 5m دخول\nحالة السوق: {result['market_regime']} | BTC 15m: {result['btc_15m']:+.2f}% | RSI BTC: {result['btc_rsi15']:.1f}\nBTC ساعة: {result['btc_1h']:+.2f}% | الضعف النسبي أمام BTC: {result['relative_weakness']:+.2f}%\n\nسعر الدخول التقريبي: {SHORT_fmt(result['entry'])}\nوقف الخسارة: {SHORT_fmt(result['stop'])} ({result['risk_pct']:.2f}%)\nالهدف الأول: {SHORT_fmt(result['tp1'])}\nالهدف الثاني: {SHORT_fmt(result['tp2'])}\nالهدف الثالث: {SHORT_fmt(result['tp3'])}\n\nRSI: {result['rsi']:.1f}\nADX: {result['adx']:.1f}\nالحجم: ×{result['volume_ratio']:.1f}\n\nأسباب الإشارة:\n{reasons}\n\n⚠️ إشارة تحليلية فقط وليست ضمانًا للربح. لا يوجد تنفيذ تداول تلقائي."

def SHORT_load_state() -> Dict:
    try:
        return json.loads(SHORT_STATE_FILE.read_text(encoding='utf-8'))
    except Exception:
        return {'alerts': {}}

def SHORT_save_state(state: Dict) -> None:
    SHORT_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def SHORT_cooled(state: Dict, result: Dict) -> bool:
    previous = int(state.get('alerts', {}).get(result['symbol'], 0))
    return result['candle_close'] - previous >= SHORT_COOLDOWN_MINUTES * 60 * 1000

def SHORT_track_open_signals(state: Dict) -> None:
    if not SHORT_TRACK_RESULTS:
        return
    open_signals = state.setdefault('open_signals', {})
    stats = state.setdefault('stats', {'tp1': 0, 'tp2': 0, 'tp3': 0, 'stop': 0})
    now = int(time.time() * 1000)
    for symbol, signal in list(open_signals.items()):
        try:
            candles = SHORT_get_klines(symbol, '5m', 100)[:-1]
            last_checked = int(signal.get('last_checked', signal['time']))
            relevant = [candle for candle in candles if candle['close_time'] > last_checked]
            if not relevant:
                if now - int(signal['time']) > 48 * 60 * 60 * 1000:
                    del open_signals[symbol]
                continue
            reached = int(signal.get('reached', 0))
            closed = False
            for candle in relevant:
                if candle['high'] >= float(signal['stop']):
                    stats['stop'] += 1
                    del open_signals[symbol]
                    closed = True
                    break
                if reached < 1 and candle['low'] <= float(signal['tp1']):
                    stats['tp1'] += 1
                    reached = 1
                if reached < 2 and candle['low'] <= float(signal['tp2']):
                    stats['tp2'] += 1
                    reached = 2
                if reached < 3 and candle['low'] <= float(signal['tp3']):
                    stats['tp3'] += 1
                    del open_signals[symbol]
                    closed = True
                    break
                signal['last_checked'] = candle['close_time']
            if not closed:
                signal['reached'] = reached
                signal['last_checked'] = relevant[-1]['close_time']
        except Exception as exc:
            print(f'Track {symbol}: {exc}', flush=True)

def SHORT_scan(state: Dict) -> None:
    SHORT_track_open_signals(state)
    market = SHORT_market_context()
    symbols = SHORT_get_symbols()
    ranked: List[Tuple[str, float]] = []
    with ThreadPoolExecutor(max_workers=SHORT_MAX_WORKERS) as pool:
        futures = [pool.submit(SHORT_prefilter_symbol, symbol) for symbol in symbols]
        for future in as_completed(futures):
            item = future.result()
            if item:
                ranked.append(item)
    ranked.sort(key=lambda item: item[1], reverse=True)
    shortlist = [symbol for symbol, _ in ranked[:SHORT_PREFILTER_LIMIT]]
    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max(4, SHORT_MAX_WORKERS // 2)) as pool:
        futures = [pool.submit(SHORT_analyze_symbol, symbol, market) for symbol in shortlist]
        for future in as_completed(futures):
            result = future.result()
            if result and SHORT_cooled(state, result):
                results.append(result)
    results.sort(key=lambda result: (3 if result.get('mode') == 'trend_reversal_short' else 2 if result.get('mode') == 'momentum_short' else 1 if result.get('mode') == 'rejection_short' else 0, result['score'], result['volume_ratio']), reverse=True)
    sent = 0
    for result in results[:SHORT_MAX_ALERTS_PER_SCAN]:
        SHORT_send_message(SHORT_signal_message(result))
        state.setdefault('alerts', {})[result['symbol']] = result['candle_close']
        state.setdefault('open_signals', {})[result['symbol']] = {'time': result['candle_close'], 'last_checked': result['candle_close'], 'entry': result['entry'], 'stop': result['stop'], 'tp1': result['tp1'], 'tp2': result['tp2'], 'tp3': result['tp3'], 'reached': 0, 'mode': result.get('mode', 'balanced_short')}
        sent += 1
        time.sleep(0.3)
    SHORT_save_state(state)
    print(f"Scan finished | market={market.get('regime')} | universe={len(symbols)} | shortlist={len(shortlist)} | candidates={len(results)} | sent={sent}", flush=True)

def short_main() -> None:
    state = SHORT_load_state()
    SHORT_send_message('✅ تم تشغيل بوت إشارات الشورت فقط V1.0.\nالمسار الأول: كسر دعم وإعادة اختبار هابطة.\nالمسار الثاني: انهيار قوي مع حجم وزخم.\nالمسار الثالث: فشل ارتداد إلى مقاومة.\nالمسار الرابع للشورت: انعكاس كبير من صعود إلى هبوط قبل اكتمال الترند.\nفلتر BTC يمنع الشورت وقت الصعود القوي.\nإشارات فقط — بدون تنفيذ تداول تلقائي وبدون إشارات شراء.')
    while True:
        started = time.time()
        try:
            SHORT_scan(state)
        except Exception as exc:
            print(f'Scan error: {exc}', flush=True)
        time.sleep(max(5, SHORT_SCAN_MINUTES * 60 - (time.time() - started)))


# ============================================================
# محرك مستقل لشورت عقود الأسهم الدائمة على Binance Futures
# أضيف دون حذف أو تعديل محركات الشراء والشورت السابقة.
# يكتشف جميع عقود الأسهم تلقائيًا من بيانات Binance، مع دعم الإضافة والاستبعاد يدويًا.
# ============================================================
STOCK_SHORT_ENABLED = os.getenv("STOCK_SHORT_ENABLED", "1") == "1"
STOCK_SHORT_AUTO_DISCOVER = os.getenv("STOCK_SHORT_AUTO_DISCOVER", "1") == "1"
STOCK_SHORT_SYMBOLS = {
    value.strip().upper()
    for value in os.getenv("STOCK_SHORT_SYMBOLS", "").split(",")
    if value.strip()
}
STOCK_SHORT_FALLBACK_SYMBOLS = {
    value.strip().upper()
    for value in os.getenv(
        "STOCK_SHORT_FALLBACK_SYMBOLS",
        "MSTR,AMZN,CRCL,COIN,PLTR,NVDA,AAPL,MSFT,META,TSLA,AVGO,TSM"
    ).split(",")
    if value.strip()
}
STOCK_SHORT_EXCLUDE_SYMBOLS = {
    value.strip().upper()
    for value in os.getenv("STOCK_SHORT_EXCLUDE_SYMBOLS", "").split(",")
    if value.strip()
}
STOCK_SHORT_SYMBOL_CACHE = {"symbols": [], "updated_at": 0.0}
STOCK_SHORT_SYMBOL_CACHE_LOCK = Lock()
STOCK_SHORT_SYMBOL_REFRESH_MINUTES = int(os.getenv("STOCK_SHORT_SYMBOL_REFRESH_MINUTES", "30"))
STOCK_SHORT_SCAN_MINUTES = int(os.getenv("STOCK_SHORT_SCAN_MINUTES", "1"))
STOCK_SHORT_COOLDOWN_MINUTES = int(os.getenv("STOCK_SHORT_COOLDOWN_MINUTES", "180"))
STOCK_SHORT_MAX_ALERTS_PER_SCAN = int(os.getenv("STOCK_SHORT_MAX_ALERTS_PER_SCAN", "3"))
STOCK_SHORT_MIN_SCORE = int(os.getenv("STOCK_SHORT_MIN_SCORE", "88"))
STOCK_SHORT_MIN_VOLUME_RATIO = float(os.getenv("STOCK_SHORT_MIN_VOLUME_RATIO", "1.30"))
STOCK_SHORT_MIN_ADX = float(os.getenv("STOCK_SHORT_MIN_ADX", "20"))
STOCK_SHORT_MIN_RSI = float(os.getenv("STOCK_SHORT_MIN_RSI", "34"))
STOCK_SHORT_MAX_RSI = float(os.getenv("STOCK_SHORT_MAX_RSI", "58"))
STOCK_SHORT_MAX_RISK_PCT = float(os.getenv("STOCK_SHORT_MAX_RISK_PCT", "3.5"))
STOCK_SHORT_REQUIRE_RETEST = os.getenv("STOCK_SHORT_REQUIRE_RETEST", "0") == "1"
STOCK_SHORT_STATE_FILE = Path(os.getenv("STOCK_SHORT_STATE_FILE", "stock_short_signal_state.json"))


def STOCK_SHORT_is_equity_contract(item: Dict) -> bool:
    """يتعرف على عقود الأسهم/Equity Perps من تصنيف Binance بدل قائمة ثابتة."""
    underlying_type = str(item.get("underlyingType", "")).upper()
    subtypes = item.get("underlyingSubType", []) or []
    if isinstance(subtypes, str):
        subtypes = [subtypes]
    subtype_text = " ".join(str(value).upper() for value in subtypes)
    classification = " ".join((underlying_type, subtype_text))
    equity_words = ("STOCK", "EQUITY", "SHARE", "ADR")
    non_equity_words = ("ETF", "INDEX", "COMMODITY", "FOREX", "FX", "METAL")
    metadata_match = any(word in classification for word in equity_words) and not any(
        word in classification for word in non_equity_words
    )
    symbol = str(item.get("symbol", "")).upper()
    base = str(item.get("baseAsset", "")).upper()
    manual_bases = {value.removesuffix("USDT") for value in (STOCK_SHORT_SYMBOLS | STOCK_SHORT_FALLBACK_SYMBOLS)}
    return metadata_match or symbol in STOCK_SHORT_SYMBOLS or base in manual_bases


def STOCK_SHORT_available_symbols() -> List[str]:
    """يعيد جميع عقود الأسهم المتاحة تلقائيًا، مع دعم رموز إضافية أو مستبعدة."""
    now = time.time()
    with STOCK_SHORT_SYMBOL_CACHE_LOCK:
        cached = list(STOCK_SHORT_SYMBOL_CACHE["symbols"])
        updated_at = float(STOCK_SHORT_SYMBOL_CACHE["updated_at"])
    if cached and now - updated_at < STOCK_SHORT_SYMBOL_REFRESH_MINUTES * 60:
        return cached
    try:
        exchange = SHORT_get_json("/fapi/v1/exchangeInfo", timeout=30)
        trading_items = [
            item for item in exchange.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("quoteAsset", "").upper() == "USDT"
            and item.get("contractType", "") == "PERPETUAL"
        ]
        available = {item.get("symbol", "").upper() for item in trading_items}
        detected = {
            item.get("symbol", "").upper()
            for item in trading_items
            if STOCK_SHORT_is_equity_contract(item)
        } if STOCK_SHORT_AUTO_DISCOVER else set()

        # الرموز اليدوية تعتبر إضافات، وليست بديلًا عن الاكتشاف التلقائي.
        symbols = (detected | (STOCK_SHORT_SYMBOLS & available)) - STOCK_SHORT_EXCLUDE_SYMBOLS
        symbols.discard("")
        result = sorted(symbols)
        STOCK_SHORT_SYMBOL_CACHE["symbols"] = result
        STOCK_SHORT_SYMBOL_CACHE["updated_at"] = now
        print(
            f"Stock contracts discovered | automatic={len(detected)} | manual={len(STOCK_SHORT_SYMBOLS & available)} | total={len(result)}",
            flush=True,
        )
        return result
    except Exception as exc:
        print(f"Stock short symbols error: {exc}", flush=True)
        if cached:
            return list(cached)
        fallback = (STOCK_SHORT_SYMBOLS or STOCK_SHORT_FALLBACK_SYMBOLS) - STOCK_SHORT_EXCLUDE_SYMBOLS
        if fallback and STOCK_SHORT_YAHOO_FALLBACK:
            print(f"Stock short fallback active | provider=Yahoo underlying | symbols={len(fallback)}", flush=True)
            return sorted(fallback)
        return sorted(STOCK_SHORT_SYMBOLS - STOCK_SHORT_EXCLUDE_SYMBOLS)


def STOCK_SHORT_load_state() -> Dict:
    try:
        return json.loads(STOCK_SHORT_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"alerts": {}, "open_signals": {}, "stats": {"tp1": 0, "tp2": 0, "tp3": 0, "stop": 0}}


def STOCK_SHORT_save_state(state: Dict) -> None:
    STOCK_SHORT_STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def STOCK_SHORT_cooled(state: Dict, result: Dict) -> bool:
    previous = int(state.get("alerts", {}).get(result["symbol"], 0))
    return result["candle_close"] - previous >= STOCK_SHORT_COOLDOWN_MINUTES * 60 * 1000


def STOCK_SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    """
    يستخدم محرك الشورت الأصلي ثم يضيف فلاتر أكثر صرامة لعقود الأسهم:
    درجة أعلى، مخاطرة أقل، RSI غير متأخر، ADX وحجم واضحان.
    """
    result = SHORT_analyze_symbol(symbol, market)
    if not result:
        return None

    if float(result.get("score", 0)) < STOCK_SHORT_MIN_SCORE:
        return None
    if float(result.get("volume_ratio", 0)) < STOCK_SHORT_MIN_VOLUME_RATIO:
        return None
    if float(result.get("adx", 0)) < STOCK_SHORT_MIN_ADX:
        return None
    rsi_value = float(result.get("rsi", 0))
    if not (STOCK_SHORT_MIN_RSI <= rsi_value <= STOCK_SHORT_MAX_RSI):
        return None
    if float(result.get("risk_pct", 999)) > STOCK_SHORT_MAX_RISK_PCT:
        return None
    if STOCK_SHORT_REQUIRE_RETEST and result.get("mode") != "rejection_short":
        return None

    result = dict(result)
    result["mode"] = "stock_short"
    result["setup"] = f"شورت عقد سهم — {result.get('setup', 'كسر هابط')}"
    result["reasons"] = [
        "العقد ضمن قائمة عقود الأسهم المحددة",
        *list(result.get("reasons", [])),
    ][:8]
    return result


def STOCK_SHORT_signal_message(result: Dict) -> str:
    reasons = "\n".join(f"• {reason}" for reason in result["reasons"])
    return (
        f"🔻 إشارة شورت عقد سهم — {result['symbol']}\n\n"
        f"النموذج: {result['setup']}\n"
        f"قوة الإشارة: {result['score']}%\n"
        "الفريمات: 4h فلتر، 1h تأكيد، 15m قرار، 5m دخول\n"
        f"حالة السوق: {result['market_regime']} | BTC 15m: {result['btc_15m']:+.2f}%\n\n"
        f"سعر الدخول التقريبي: {SHORT_fmt(result['entry'])}\n"
        f"وقف الخسارة: {SHORT_fmt(result['stop'])} ({result['risk_pct']:.2f}%)\n"
        f"الهدف الأول: {SHORT_fmt(result['tp1'])}\n"
        f"الهدف الثاني: {SHORT_fmt(result['tp2'])}\n"
        f"الهدف الثالث: {SHORT_fmt(result['tp3'])}\n\n"
        f"RSI: {result['rsi']:.1f}\n"
        f"ADX: {result['adx']:.1f}\n"
        f"الحجم: ×{result['volume_ratio']:.1f}\n\n"
        f"أسباب الإشارة:\n{reasons}\n\n"
        "⚠️ إشارة تحليلية فقط وليست تنفيذًا تلقائيًا أو ضمانًا للربح."
    )


def STOCK_SHORT_track_open_signals(state: Dict) -> None:
    open_signals = state.setdefault("open_signals", {})
    stats = state.setdefault("stats", {"tp1": 0, "tp2": 0, "tp3": 0, "stop": 0})
    now = int(time.time() * 1000)

    for symbol, signal in list(open_signals.items()):
        try:
            candles = SHORT_get_klines(symbol, "5m", 100)[:-1]
            last_checked = int(signal.get("last_checked", signal["time"]))
            relevant = [c for c in candles if c["close_time"] > last_checked]
            if not relevant:
                if now - int(signal["time"]) > 72 * 60 * 60 * 1000:
                    del open_signals[symbol]
                continue

            reached = int(signal.get("reached", 0))
            closed = False
            for candle in relevant:
                if candle["high"] >= float(signal["stop"]):
                    stats["stop"] += 1
                    del open_signals[symbol]
                    closed = True
                    break
                if reached < 1 and candle["low"] <= float(signal["tp1"]):
                    stats["tp1"] += 1
                    reached = 1
                if reached < 2 and candle["low"] <= float(signal["tp2"]):
                    stats["tp2"] += 1
                    reached = 2
                if reached < 3 and candle["low"] <= float(signal["tp3"]):
                    stats["tp3"] += 1
                    del open_signals[symbol]
                    closed = True
                    break
                signal["last_checked"] = candle["close_time"]

            if not closed:
                signal["reached"] = reached
                signal["last_checked"] = relevant[-1]["close_time"]
        except Exception as exc:
            print(f"Stock short track {symbol}: {exc}", flush=True)


def STOCK_SHORT_scan(state: Dict) -> None:
    STOCK_SHORT_track_open_signals(state)
    market = SHORT_market_context()
    symbols = STOCK_SHORT_available_symbols()
    results: List[Dict] = []

    with ThreadPoolExecutor(max_workers=max(2, min(8, len(symbols) or 2))) as pool:
        futures = [pool.submit(STOCK_SHORT_analyze_symbol, symbol, market) for symbol in symbols]
        for future in as_completed(futures):
            result = future.result()
            if result and STOCK_SHORT_cooled(state, result):
                results.append(result)

    results.sort(
        key=lambda result: (result["score"], result["volume_ratio"]),
        reverse=True,
    )

    sent = 0
    for result in results[:STOCK_SHORT_MAX_ALERTS_PER_SCAN]:
        SHORT_send_message(STOCK_SHORT_signal_message(result))
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
            "mode": "stock_short",
        }
        sent += 1
        time.sleep(0.3)

    STOCK_SHORT_save_state(state)
    print(
        f"Stock short scan | symbols={len(symbols)} | candidates={len(results)} | sent={sent}",
        flush=True,
    )


def stock_short_main() -> None:
    if not STOCK_SHORT_ENABLED:
        print("Stock short engine disabled.", flush=True)
        return

    state = STOCK_SHORT_load_state()
    extra_symbols = ", ".join(sorted(STOCK_SHORT_SYMBOLS)) or "لا توجد إضافات يدوية"
    SHORT_send_message(
        "✅ تم تشغيل محرك شورت عقود الأسهم/الأصول الأساسية.\n"
        f"الاكتشاف التلقائي: {'مفعّل' if STOCK_SHORT_AUTO_DISCOVER else 'متوقف'}\n"
        f"إضافات يدوية: {extra_symbols}\n"
        "يستخدم Binance Futures عند توفره، ويتحول تلقائيًا لبيانات الأصل عند حجب API.\n"
        "فلترة مستقلة: اتجاه هابط، كسر/فشل ارتداد، وانعكاس كبير من صعود إلى هبوط، مع حجم وADX وRSI ومخاطرة منخفضة.\n"
        "إشارات فقط — بدون تنفيذ تداول تلقائي."
    )
    while True:
        started = time.time()
        try:
            STOCK_SHORT_scan(state)
        except Exception as exc:
            print(f"Stock short scan error: {exc}", flush=True)
        time.sleep(max(5, STOCK_SHORT_SCAN_MINUTES * 60 - (time.time() - started)))


# ============================================================
# محرك السيولة الذكي المجاني V1.0 — Binance Order Book
# يضاف كطبقة جودة أخيرة فقط بعد نجاح التحليل الفني لتقليل استهلاك API.
# يعمل مع: سبوت LONG + شورت العملات + شورت عقود الأسهم/TradFi.
# لا ينفذ أوامر تداول؛ إشارات تيليجرام فقط.
# ============================================================
LIQUIDITY_ENABLED = os.getenv("LIQUIDITY_ENABLED", "1") == "1"
LIQUIDITY_SAMPLES = max(1, int(os.getenv("LIQUIDITY_SAMPLES", "2")))
LIQUIDITY_SAMPLE_DELAY = max(0.0, float(os.getenv("LIQUIDITY_SAMPLE_DELAY", "0.30")))
LIQUIDITY_DEPTH_LIMIT = int(os.getenv("LIQUIDITY_DEPTH_LIMIT", "100"))
LIQUIDITY_RANGE_PCT = float(os.getenv("LIQUIDITY_RANGE_PCT", "2.0"))
LIQUIDITY_WALL_MULTIPLIER = float(os.getenv("LIQUIDITY_WALL_MULTIPLIER", "4.0"))
LIQUIDITY_MIN_SPOT_WALL_USDT = float(os.getenv("LIQUIDITY_MIN_SPOT_WALL_USDT", "25000"))
LIQUIDITY_MIN_FUTURES_WALL_USDT = float(os.getenv("LIQUIDITY_MIN_FUTURES_WALL_USDT", "50000"))
LIQUIDITY_LONG_HARD_BLOCK_DISTANCE_PCT = float(os.getenv("LIQUIDITY_LONG_HARD_BLOCK_DISTANCE_PCT", "0.45"))
LIQUIDITY_SHORT_HARD_BLOCK_DISTANCE_PCT = float(os.getenv("LIQUIDITY_SHORT_HARD_BLOCK_DISTANCE_PCT", "0.55"))
LIQUIDITY_MAX_SCORE_BONUS = int(os.getenv("LIQUIDITY_MAX_SCORE_BONUS", "8"))


def LIQUIDITY_depth(symbol: str, futures: bool = False) -> Dict:
    path = "/fapi/v1/depth" if futures else "/api/v3/depth"
    params = {"symbol": symbol, "limit": LIQUIDITY_DEPTH_LIMIT}
    return SHORT_get_json(path, params=params, timeout=12) if futures else get_json(path, params=params, timeout=12)


def LIQUIDITY_side_stats(levels, mid: float, side: str, min_wall_usdt: float) -> Dict:
    rows = []
    for raw_price, raw_qty in levels:
        price, qty = float(raw_price), float(raw_qty)
        if price <= 0 or qty <= 0:
            continue
        distance = ((price / mid) - 1) * 100 if side == "ask" else (1 - price / mid) * 100
        if 0 <= distance <= LIQUIDITY_RANGE_PCT:
            rows.append({"price": price, "qty": qty, "notional": price * qty, "distance_pct": distance})
    if not rows:
        return {"total": 0.0, "wall": None, "median": 0.0}
    notionals = sorted(row["notional"] for row in rows)
    n = len(notionals)
    median_notional = notionals[n // 2] if n % 2 else (notionals[n // 2 - 1] + notionals[n // 2]) / 2
    threshold = max(min_wall_usdt, median_notional * LIQUIDITY_WALL_MULTIPLIER)
    walls = [row for row in rows if row["notional"] >= threshold]
    wall = max(walls, key=lambda row: row["notional"]) if walls else max(rows, key=lambda row: row["notional"])
    wall = dict(wall)
    wall["qualified"] = bool(walls and wall["notional"] >= threshold)
    return {"total": sum(row["notional"] for row in rows), "wall": wall, "median": median_notional}


def LIQUIDITY_snapshot(symbol: str, futures: bool = False) -> Optional[Dict]:
    snapshots = []
    min_wall = LIQUIDITY_MIN_FUTURES_WALL_USDT if futures else LIQUIDITY_MIN_SPOT_WALL_USDT
    for index in range(LIQUIDITY_SAMPLES):
        book = LIQUIDITY_depth(symbol, futures=futures)
        bids, asks = book.get("bids", []), book.get("asks", [])
        if not bids or not asks:
            continue
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
        mid = (best_bid + best_ask) / 2
        bid_stats = LIQUIDITY_side_stats(bids, mid, "bid", min_wall)
        ask_stats = LIQUIDITY_side_stats(asks, mid, "ask", min_wall)
        snapshots.append({"mid": mid, "bid": bid_stats, "ask": ask_stats})
        if index + 1 < LIQUIDITY_SAMPLES and LIQUIDITY_SAMPLE_DELAY:
            time.sleep(LIQUIDITY_SAMPLE_DELAY)
    if not snapshots:
        return None
    bid_total = mean(item["bid"]["total"] for item in snapshots)
    ask_total = mean(item["ask"]["total"] for item in snapshots)
    bid_walls = [item["bid"]["wall"] for item in snapshots if item["bid"]["wall"]]
    ask_walls = [item["ask"]["wall"] for item in snapshots if item["ask"]["wall"]]
    bid_wall = max(bid_walls, key=lambda row: row["notional"]) if bid_walls else None
    ask_wall = max(ask_walls, key=lambda row: row["notional"]) if ask_walls else None
    ratio = bid_total / ask_total if ask_total > 0 else 99.0
    persistence = min(1.0, len(snapshots) / max(1, LIQUIDITY_SAMPLES))
    return {
        "bid_total": bid_total, "ask_total": ask_total, "bid_ask_ratio": ratio,
        "bid_wall": bid_wall, "ask_wall": ask_wall,
        "samples": len(snapshots), "persistence": persistence,
        "range_pct": LIQUIDITY_RANGE_PCT,
    }


def LIQUIDITY_apply(result: Dict, direction: str, futures: bool) -> Optional[Dict]:
    if not LIQUIDITY_ENABLED:
        return result
    try:
        data = LIQUIDITY_snapshot(result["symbol"], futures=futures)
        if not data:
            result = dict(result)
            result["liquidity"] = {"available": False}
            return result
        bid_wall, ask_wall = data.get("bid_wall"), data.get("ask_wall")
        bid_wall_value = float(bid_wall.get("notional", 0)) if bid_wall else 0.0
        ask_wall_value = float(ask_wall.get("notional", 0)) if ask_wall else 0.0
        ratio = float(data.get("bid_ask_ratio", 1.0))
        bonus, blocked, note = 0, False, "سيولة متوازنة"
        if direction == "long":
            if ask_wall and ask_wall.get("qualified") and ask_wall["distance_pct"] <= LIQUIDITY_LONG_HARD_BLOCK_DISTANCE_PCT and ask_wall_value >= max(1.5 * bid_wall_value, LIQUIDITY_MIN_SPOT_WALL_USDT):
                blocked, note = True, "جدار بيع قوي قريب جدًا"
            elif ratio >= 1.50:
                bonus, note = LIQUIDITY_MAX_SCORE_BONUS, "ضغط طلب داعم للشراء"
            elif ratio >= 1.20:
                bonus, note = 4, "أفضلية معتدلة للطلبات"
            elif ratio <= 0.65:
                bonus, note = -6, "ضغط عروض مرتفع"
            elif ratio <= 0.85:
                bonus, note = -3, "العروض أعلى قليلًا"
        else:
            if bid_wall and bid_wall.get("qualified") and bid_wall["distance_pct"] <= LIQUIDITY_SHORT_HARD_BLOCK_DISTANCE_PCT and bid_wall_value >= max(1.5 * ask_wall_value, LIQUIDITY_MIN_FUTURES_WALL_USDT):
                blocked, note = True, "جدار شراء قوي قريب قد يصد الهبوط"
            elif ratio >= 2.50:
                blocked, note = True, "اختلال طلب شديد؛ احتمال ارتداد/سكويز مرتفع"
            elif ratio <= 0.67:
                bonus, note = LIQUIDITY_MAX_SCORE_BONUS, "ضغط عروض داعم للشورت"
            elif ratio <= 0.83:
                bonus, note = 4, "أفضلية معتدلة للعروض"
            elif ratio >= 1.80:
                bonus, note = -8, "ضغط طلب قوي قد يسبب ارتدادًا/سكويز"
            elif ratio >= 1.30:
                bonus, note = -4, "الطلبات أعلى من العروض"
        if blocked:
            log_fn = SHORT_log_rejection if futures and "SHORT_log_rejection" in globals() else log_rejection
            log_fn(result["symbol"], f"رفض بواسطة السيولة: {note}", {
                "ratio": round(ratio, 2), "bid_wall": round(bid_wall_value, 2), "ask_wall": round(ask_wall_value, 2)
            })
            return None
        enriched = dict(result)
        enriched["score"] = max(0, min(99, int(round(float(result.get("score", 0)) + bonus))))
        enriched["liquidity"] = {
            "available": True, "direction": direction, "bonus": bonus, "note": note,
            **data,
        }
        reasons = list(enriched.get("reasons", []))
        reasons.append(f"السيولة: {note} ({bonus:+d} نقاط)")
        enriched["reasons"] = reasons[:8]
        return enriched
    except Exception as exc:
        print(f"Liquidity {result.get('symbol')}: {exc}", flush=True)
        enriched = dict(result)
        enriched["liquidity"] = {"available": False, "error": str(exc)}
        return enriched


def LIQUIDITY_money(value: float) -> str:
    value = float(value or 0)
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B USDT"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M USDT"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K USDT"
    return f"{value:.0f} USDT"


def LIQUIDITY_message_section(result: Dict) -> str:
    data = result.get("liquidity", {})
    if not data.get("available"):
        return "\n\n💧 السيولة: غير متاحة مؤقتًا — لم تُلغَ الإشارة."
    bid_wall, ask_wall = data.get("bid_wall"), data.get("ask_wall")
    bid_text = LIQUIDITY_money(bid_wall.get("notional", 0)) if bid_wall else "غير واضح"
    ask_text = LIQUIDITY_money(ask_wall.get("notional", 0)) if ask_wall else "غير واضح"
    bid_dist = f" ({bid_wall['distance_pct']:.2f}% أسفل السعر)" if bid_wall else ""
    ask_dist = f" ({ask_wall['distance_pct']:.2f}% أعلى السعر)" if ask_wall else ""
    return (
        "\n\n💧 تحليل السيولة من Binance Order Book\n"
        f"أقوى جدار شراء: {bid_text}{bid_dist}\n"
        f"أقوى جدار بيع: {ask_text}{ask_dist}\n"
        f"نسبة الطلب/العرض: ×{data.get('bid_ask_ratio', 1):.2f}\n"
        f"الحكم: {data.get('note', 'متوازن')} | تعديل التقييم: {int(data.get('bonus', 0)):+d}"
    )


# ============================================================
# Whale + Order Flow Engine V9 داخل ملف V8
# يعمل بعد نجاح التحليل الفني لتقليل استهلاك API.
# بيانات مجانية من Binance AggTrades فقط، ولا ينفذ صفقات.
# ============================================================
WHALE_FLOW_ENABLED = os.getenv("WHALE_FLOW_ENABLED", "1") == "1"
WHALE_FLOW_TRADE_LIMIT = int(os.getenv("WHALE_FLOW_TRADE_LIMIT", "500"))
WHALE_FLOW_MIN_SPOT_NOTIONAL = float(os.getenv("WHALE_FLOW_MIN_SPOT_NOTIONAL", "25000"))
WHALE_FLOW_MIN_FUTURES_NOTIONAL = float(os.getenv("WHALE_FLOW_MIN_FUTURES_NOTIONAL", "75000"))
WHALE_FLOW_WHALE_MULTIPLIER = float(os.getenv("WHALE_FLOW_WHALE_MULTIPLIER", "8"))
WHALE_FLOW_MAX_SCORE_BONUS = int(os.getenv("WHALE_FLOW_MAX_SCORE_BONUS", "10"))
WHALE_FLOW_HARD_BLOCK_RATIO = float(os.getenv("WHALE_FLOW_HARD_BLOCK_RATIO", "0.24"))
WHALE_FLOW_MIN_TOTAL_NOTIONAL = float(os.getenv("WHALE_FLOW_MIN_TOTAL_NOTIONAL", "100000"))
WHALE_FLOW_ABSORPTION_MOVE_PCT = float(os.getenv("WHALE_FLOW_ABSORPTION_MOVE_PCT", "0.18"))


WHALE_FLOW_TRADES_CACHE_SECONDS = float(os.getenv("WHALE_FLOW_TRADES_CACHE_SECONDS", "12"))
WHALE_FLOW_TRADES_CACHE: Dict[Tuple[str, bool], Dict] = {}
WHALE_FLOW_TRADES_CACHE_LOCK = Lock()

def WHALE_FLOW_get_trades(symbol: str, futures: bool = False) -> List[Dict]:
    """يجلب aggTrades مرة واحدة ويشارك العينة بين Whale Flow وCVD."""
    cache_key = (symbol.upper(), bool(futures))
    now = time.time()
    with WHALE_FLOW_TRADES_CACHE_LOCK:
        cached = WHALE_FLOW_TRADES_CACHE.get(cache_key)
        if cached and now - float(cached.get("updated_at", 0)) <= WHALE_FLOW_TRADES_CACHE_SECONDS:
            return [dict(row) for row in cached.get("trades", [])]
    path = "/fapi/v1/aggTrades" if futures else "/api/v3/aggTrades"
    params = {"symbol": symbol, "limit": max(50, min(1000, WHALE_FLOW_TRADE_LIMIT))}
    if futures:
        raw = SHORT_get_json(path, params=params, timeout=12)
    else:
        response = SESSION.get(f"{BINANCE_BASE}{path}", params=params, timeout=12)
        response.raise_for_status()
        raw = response.json()
    rows = []
    for item in raw:
        try:
            price = float(item["p"])
            qty = float(item["q"])
            rows.append({
                "price": price,
                "qty": qty,
                "notional": price * qty,
                "time": int(item.get("T", 0)),
                "side": "sell" if bool(item.get("m")) else "buy",
            })
        except (KeyError, TypeError, ValueError):
            continue
    with WHALE_FLOW_TRADES_CACHE_LOCK:
        WHALE_FLOW_TRADES_CACHE[cache_key] = {"updated_at": now, "trades": [dict(row) for row in rows]}
    return rows


def WHALE_FLOW_percentile(values: List[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * fraction))))
    return float(ordered[index])


def WHALE_FLOW_snapshot(symbol: str, futures: bool = False) -> Optional[Dict]:
    trades = WHALE_FLOW_get_trades(symbol, futures=futures)
    if len(trades) < 20:
        return None

    buy_notional = sum(t["notional"] for t in trades if t["side"] == "buy")
    sell_notional = sum(t["notional"] for t in trades if t["side"] == "sell")
    total = buy_notional + sell_notional
    if total <= 0:
        return None

    delta = buy_notional - sell_notional
    delta_pct = delta / total
    buy_ratio = buy_notional / total
    sell_ratio = sell_notional / total

    notionals = [t["notional"] for t in trades]
    median_trade = WHALE_FLOW_percentile(notionals, 0.50)
    p90_trade = WHALE_FLOW_percentile(notionals, 0.90)
    fixed_floor = WHALE_FLOW_MIN_FUTURES_NOTIONAL if futures else WHALE_FLOW_MIN_SPOT_NOTIONAL
    whale_threshold = max(fixed_floor, median_trade * WHALE_FLOW_WHALE_MULTIPLIER, p90_trade * 1.75)
    whales = [t for t in trades if t["notional"] >= whale_threshold]
    whale_buy = sum(t["notional"] for t in whales if t["side"] == "buy")
    whale_sell = sum(t["notional"] for t in whales if t["side"] == "sell")
    whale_total = whale_buy + whale_sell
    whale_delta_pct = (whale_buy - whale_sell) / whale_total if whale_total else 0.0

    first_price = trades[0]["price"]
    last_price = trades[-1]["price"]
    move_pct = ((last_price / first_price) - 1) * 100 if first_price else 0.0

    # امتصاص تقريبي: تدفق عدواني قوي لكن السعر لا يتحرك معه بما يكفي.
    absorption = "none"
    if total >= WHALE_FLOW_MIN_TOTAL_NOTIONAL and abs(move_pct) <= WHALE_FLOW_ABSORPTION_MOVE_PCT:
        if buy_ratio >= 0.68:
            absorption = "sell_absorption"  # شراء عدواني يتم امتصاصه بعروض ثابتة
        elif sell_ratio >= 0.68:
            absorption = "buy_absorption"   # بيع عدواني يتم امتصاصه بطلبات ثابتة

    largest = sorted(whales, key=lambda x: x["notional"], reverse=True)[:3]
    return {
        "available": True,
        "count": len(trades),
        "total_notional": total,
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "delta": delta,
        "delta_pct": delta_pct,
        "buy_ratio": buy_ratio,
        "sell_ratio": sell_ratio,
        "whale_threshold": whale_threshold,
        "whale_count": len(whales),
        "whale_buy": whale_buy,
        "whale_sell": whale_sell,
        "whale_delta_pct": whale_delta_pct,
        "price_move_pct": move_pct,
        "absorption": absorption,
        "largest": largest,
    }


def WHALE_FLOW_apply(result: Dict, direction: str, futures: bool) -> Optional[Dict]:
    if not WHALE_FLOW_ENABLED:
        return result
    try:
        data = WHALE_FLOW_snapshot(result["symbol"], futures=futures)
        if not data:
            enriched = dict(result)
            enriched["whale_flow"] = {"available": False}
            return enriched

        delta_pct = float(data["delta_pct"])
        whale_delta_pct = float(data["whale_delta_pct"])
        total = float(data["total_notional"])
        absorption = data.get("absorption", "none")
        bonus = 0
        blocked = False
        notes = []

        if direction == "long":
            if total >= WHALE_FLOW_MIN_TOTAL_NOTIONAL and delta_pct <= -WHALE_FLOW_HARD_BLOCK_RATIO and whale_delta_pct <= -0.20:
                blocked = True
                notes.append("بيع عدواني وحيتان بائعة يعاكسان الشراء")
            else:
                if delta_pct >= 0.22: bonus += 5
                elif delta_pct >= 0.10: bonus += 3
                elif delta_pct <= -0.18: bonus -= 4
                if whale_delta_pct >= 0.25: bonus += 5; notes.append("حيتان مشترية")
                elif whale_delta_pct <= -0.25: bonus -= 4; notes.append("حيتان بائعة")
                if absorption == "buy_absorption": bonus += 2; notes.append("امتصاص بيع بواسطة الطلبات")
                elif absorption == "sell_absorption": bonus -= 2; notes.append("امتصاص شراء بواسطة العروض")
        else:
            if total >= WHALE_FLOW_MIN_TOTAL_NOTIONAL and delta_pct >= WHALE_FLOW_HARD_BLOCK_RATIO and whale_delta_pct >= 0.20:
                blocked = True
                notes.append("شراء عدواني وحيتان مشترية قد تسبب سكويز")
            else:
                if delta_pct <= -0.22: bonus += 5
                elif delta_pct <= -0.10: bonus += 3
                elif delta_pct >= 0.18: bonus -= 4
                if whale_delta_pct <= -0.25: bonus += 5; notes.append("حيتان بائعة")
                elif whale_delta_pct >= 0.25: bonus -= 4; notes.append("حيتان مشترية")
                if absorption == "sell_absorption": bonus += 2; notes.append("امتصاص شراء بواسطة العروض")
                elif absorption == "buy_absorption": bonus -= 2; notes.append("امتصاص بيع بواسطة الطلبات")

        bonus = max(-WHALE_FLOW_MAX_SCORE_BONUS, min(WHALE_FLOW_MAX_SCORE_BONUS, bonus))
        if not notes:
            notes.append("تدفق أوامر متوازن")

        if blocked:
            log_fn = SHORT_log_rejection if futures and "SHORT_log_rejection" in globals() else log_rejection
            log_fn(result["symbol"], f"رفض بواسطة Whale/Order Flow: {'، '.join(notes)}", {
                "delta_pct": round(delta_pct, 4),
                "whale_delta_pct": round(whale_delta_pct, 4),
                "total": round(total, 2),
            })
            return None

        enriched = dict(result)
        enriched["score"] = max(0, min(99, int(round(float(result.get("score", 0)) + bonus))))
        data["bonus"] = bonus
        data["note"] = "، ".join(notes)
        enriched["whale_flow"] = data
        reasons = list(enriched.get("reasons", []))
        reasons.append(f"الحيتان وتدفق الأوامر: {data['note']} ({bonus:+d} نقاط)")
        enriched["reasons"] = reasons[:8]
        return enriched
    except Exception as exc:
        print(f"Whale flow {result.get('symbol')}: {exc}", flush=True)
        enriched = dict(result)
        enriched["whale_flow"] = {"available": False, "error": str(exc)}
        return enriched


def WHALE_FLOW_message_section(result: Dict) -> str:
    data = result.get("whale_flow", {})
    if not data.get("available"):
        return "\n\n🐋 الحيتان وتدفق الأوامر: غير متاح مؤقتًا — لم تُلغَ الإشارة."
    delta_pct = float(data.get("delta_pct", 0)) * 100
    whale_delta_pct = float(data.get("whale_delta_pct", 0)) * 100
    absorption_names = {
        "none": "لا يوجد امتصاص واضح",
        "buy_absorption": "امتصاص بيع عند الطلبات",
        "sell_absorption": "امتصاص شراء عند العروض",
    }
    whale_line = (
        f"صفقات الحيتان: {int(data.get('whale_count', 0))} | Whale Delta: {whale_delta_pct:+.1f}%\n"
        if int(data.get('whale_count', 0)) > 0
        else "لا توجد صفقات حيتان مؤثرة ضمن العينة الحالية.\n"
    )
    return (
        "\n\n🐋 Whale & Order Flow من Binance\n"
        f"حجم الصفقات المحللة: {LIQUIDITY_money(data.get('total_notional', 0))}\n"
        f"Delta العدواني: {delta_pct:+.1f}% | شراء {data.get('buy_ratio', 0)*100:.1f}% / بيع {data.get('sell_ratio', 0)*100:.1f}%\n"
        + whale_line
        + f"الامتصاص: {absorption_names.get(data.get('absorption'), 'غير واضح')}\n"
        + f"الحكم: {data.get('note', 'متوازن')} | تعديل التقييم: {int(data.get('bonus', 0)):+d}"
    )


# تغليف المحركات الأصلية بدل إعادة كتابتها، لضمان عدم حذف أي ميزة سابقة.
_ORIGINAL_LONG_ANALYZE = analyze_symbol
_ORIGINAL_SHORT_ANALYZE = SHORT_analyze_symbol
_ORIGINAL_STOCK_SHORT_ANALYZE = STOCK_SHORT_analyze_symbol
_ORIGINAL_LONG_MESSAGE = signal_message
_ORIGINAL_SHORT_MESSAGE = SHORT_signal_message
_ORIGINAL_STOCK_SHORT_MESSAGE = STOCK_SHORT_signal_message


def analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    result = _ORIGINAL_LONG_ANALYZE(symbol, market)
    if not result:
        return None
    result = LIQUIDITY_apply(result, "long", futures=False)
    return WHALE_FLOW_apply(result, "long", futures=False) if result else None


def SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    result = _ORIGINAL_SHORT_ANALYZE(symbol, market)
    if not result:
        return None
    result = LIQUIDITY_apply(result, "short", futures=True)
    return WHALE_FLOW_apply(result, "short", futures=True) if result else None


def STOCK_SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    # المحرك الأصلي يستدعي SHORT_analyze_symbol ديناميكيًا، لذلك يحصل تلقائيًا
    # على السيولة والحيتان ثم يطبق فلاتر عقود الأسهم الأكثر صرامة.
    return _ORIGINAL_STOCK_SHORT_ANALYZE(symbol, market)


def _append_microstructure_sections(base: str, result: Dict) -> str:
    sections = LIQUIDITY_message_section(result) + WHALE_FLOW_message_section(result)
    marker = "\n\n⚠️"
    return base.replace(marker, sections + marker, 1) if marker in base else base + sections


def signal_message(result: Dict) -> str:
    return _append_microstructure_sections(_ORIGINAL_LONG_MESSAGE(result), result)


def SHORT_signal_message(result: Dict) -> str:
    return _append_microstructure_sections(_ORIGINAL_SHORT_MESSAGE(result), result)


def STOCK_SHORT_signal_message(result: Dict) -> str:
    return _append_microstructure_sections(_ORIGINAL_STOCK_SHORT_MESSAGE(result), result)


# ============================================================
# CVD + Open Interest + Confidence Score 2.0
# طبقة جودة نهائية تضاف دون حذف أي محرك سابق.
# ============================================================
CVD_OI_ENABLED = os.getenv("CVD_OI_ENABLED", "1") == "1"
CVD_WINDOW_MINUTES = max(5, int(os.getenv("CVD_WINDOW_MINUTES", "30")))
CVD_MIN_COVERAGE_MINUTES = float(os.getenv("CVD_MIN_COVERAGE_MINUTES", "2"))
CVD_MAX_SCORE_BONUS = int(os.getenv("CVD_MAX_SCORE_BONUS", "8"))
OI_ENABLED = os.getenv("OPEN_INTEREST_ENABLED", "1") == "1"
OI_PERIOD = os.getenv("OPEN_INTEREST_PERIOD", "5m")
OI_HISTORY_LIMIT = max(3, min(30, int(os.getenv("OPEN_INTEREST_HISTORY_LIMIT", "7"))))
OI_MAX_SCORE_BONUS = int(os.getenv("OPEN_INTEREST_MAX_SCORE_BONUS", "8"))
OI_HARD_BLOCK_CHANGE_PCT = float(os.getenv("OPEN_INTEREST_HARD_BLOCK_CHANGE_PCT", "3.0"))
CONFIDENCE_2_ENABLED = os.getenv("CONFIDENCE_2_ENABLED", "1") == "1"
CONFIDENCE_MIN_FINAL = int(os.getenv("CONFIDENCE_MIN_FINAL", "78"))


def CVD_snapshot(symbol: str, futures: bool = False) -> Optional[Dict]:
    """يحسب CVD من الصفقات العدوانية المتاحة ويبيّن مدة التغطية الفعلية للعينة."""
    trades = WHALE_FLOW_get_trades(symbol, futures=futures)
    if len(trades) < 20:
        return None
    trades = sorted(trades, key=lambda row: int(row.get("time", 0)))
    newest = int(trades[-1].get("time", 0))
    if newest <= 0:
        return None
    cutoff = newest - CVD_WINDOW_MINUTES * 60 * 1000
    window = [row for row in trades if int(row.get("time", 0)) >= cutoff]
    if len(window) < 20:
        window = trades
    buy = sum(float(row.get("notional", 0)) for row in window if row.get("side") == "buy")
    sell = sum(float(row.get("notional", 0)) for row in window if row.get("side") == "sell")
    total = buy + sell
    if total <= 0:
        return None
    oldest = int(window[0].get("time", newest))
    coverage_minutes = max(0.0, (newest - oldest) / 60000)
    cvd = buy - sell
    cvd_pct = cvd / total
    half = max(10, len(window) // 2)
    recent = window[-half:]
    recent_buy = sum(float(row.get("notional", 0)) for row in recent if row.get("side") == "buy")
    recent_sell = sum(float(row.get("notional", 0)) for row in recent if row.get("side") == "sell")
    recent_total = recent_buy + recent_sell
    recent_pct = (recent_buy - recent_sell) / recent_total if recent_total else 0.0
    return {
        "available": True,
        "cvd": cvd,
        "cvd_pct": cvd_pct,
        "recent_cvd_pct": recent_pct,
        "buy_notional": buy,
        "sell_notional": sell,
        "total_notional": total,
        "trade_count": len(window),
        "coverage_minutes": coverage_minutes,
        "requested_minutes": CVD_WINDOW_MINUTES,
        "partial_window": coverage_minutes < CVD_MIN_COVERAGE_MINUTES,
    }


def OPEN_INTEREST_snapshot(symbol: str, entry_price: float) -> Optional[Dict]:
    """Open Interest حقيقي لعقود Binance Futures فقط؛ لا يختلق قيمة عند fallback."""
    if not OI_ENABLED or SHORT_FUTURES_BLOCKED or not symbol.upper().endswith("USDT"):
        return None
    try:
        current = SHORT_get_json("/fapi/v1/openInterest", {"symbol": symbol}, timeout=12)
        history = SHORT_get_json(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": OI_PERIOD, "limit": OI_HISTORY_LIMIT},
            timeout=12,
        )
        current_oi = float(current.get("openInterest", 0) or 0)
        rows = history if isinstance(history, list) else []
        if current_oi <= 0 or not rows:
            return None
        first_value = float(rows[0].get("sumOpenInterest", 0) or 0)
        last_value = float(rows[-1].get("sumOpenInterest", current_oi) or current_oi)
        reference = first_value if first_value > 0 else last_value
        change_pct = ((current_oi / reference) - 1) * 100 if reference > 0 else 0.0
        current_usdt = current_oi * float(entry_price or 0)
        return {
            "available": True,
            "open_interest": current_oi,
            "open_interest_usdt": current_usdt,
            "change_pct": change_pct,
            "period": OI_PERIOD,
            "samples": len(rows),
        }
    except Exception as exc:
        print(f"Open interest {symbol}: {exc}", flush=True)
        return None


def _directional_relative_move(result: Dict, direction: str) -> float:
    """Return the relative price move using the correct key for each engine."""
    key = "relative_weakness" if direction == "short" else "relative_strength"
    return float(result.get(key, 0) or 0)


def CVD_OI_apply(result: Dict, direction: str, futures: bool) -> Optional[Dict]:
    if not CVD_OI_ENABLED:
        return result
    enriched = dict(result)
    bonus = 0
    notes = []
    try:
        cvd = CVD_snapshot(result["symbol"], futures=futures)
    except Exception as exc:
        print(f"CVD {result.get('symbol')}: {exc}", flush=True)
        cvd = None
    if cvd:
        cvd_pct = float(cvd.get("cvd_pct", 0))
        recent_pct = float(cvd.get("recent_cvd_pct", 0))
        if direction == "long":
            if cvd_pct >= 0.18 and recent_pct >= 0.10: bonus += CVD_MAX_SCORE_BONUS; notes.append("CVD شرائي ومتسارع")
            elif cvd_pct >= 0.08: bonus += 4; notes.append("CVD يميل للشراء")
            elif cvd_pct <= -0.18: bonus -= 6; notes.append("CVD بيعي يعاكس الشراء")
        else:
            if cvd_pct <= -0.18 and recent_pct <= -0.10: bonus += CVD_MAX_SCORE_BONUS; notes.append("CVD بيعي ومتسارع")
            elif cvd_pct <= -0.08: bonus += 4; notes.append("CVD يميل للبيع")
            elif cvd_pct >= 0.18: bonus -= 6; notes.append("CVD شرائي يعاكس الشورت")
        cvd["bonus"] = max(-CVD_MAX_SCORE_BONUS, min(CVD_MAX_SCORE_BONUS, bonus))
    else:
        cvd = {"available": False}

    oi = OPEN_INTEREST_snapshot(result["symbol"], float(result.get("entry", 0))) if futures else None
    oi_bonus = 0
    if oi:
        change = float(oi.get("change_pct", 0))
        price_move = _directional_relative_move(result, direction)
        # حجب صارم: هبوط OI الكبير يعني غالبًا تصفية/تغطية لا بناء اتجاه جديد.
        # كما نحجب ارتفاع OI الكبير إذا كان تحرك السعر يعاكس اتجاه الإشارة.
        hard_oi_block = (
            change <= -abs(OI_HARD_BLOCK_CHANGE_PCT)
            or (direction == "short" and change >= abs(OI_HARD_BLOCK_CHANGE_PCT) and price_move >= 0)
            or (direction == "long" and change >= abs(OI_HARD_BLOCK_CHANGE_PCT) and price_move <= 0)
        )
        if hard_oi_block:
            print(f"Rejected {result.get('symbol')}: OI hard block | change={change:+.2f}% direction={direction} price_move={price_move:+.2f}%", flush=True)
            return None
        if direction == "short":
            if change >= 1.0 and price_move < 0:
                oi_bonus = min(OI_MAX_SCORE_BONUS, 6 if change < 3 else 8)
                notes.append("OI يرتفع مع الضعف السعري: دخول شورت جديد")
            elif change <= -1.5:
                oi_bonus = -5
                notes.append("OI ينخفض: الهبوط قد يكون تصفية لا بناء شورت")
        else:
            if change >= 1.0 and price_move > 0:
                oi_bonus = min(OI_MAX_SCORE_BONUS, 6 if change < 3 else 8)
                notes.append("OI يرتفع مع القوة السعرية: دخول مراكز جديدة")
            elif change <= -1.5:
                oi_bonus = -4
                notes.append("OI ينخفض: الحركة قد تكون تغطية مراكز")
        oi["bonus"] = oi_bonus
    else:
        oi = {"available": False, "reason": "غير متاح مع مصدر Spot/Yahoo أو عند حجب Futures"}

    micro_bonus = max(-12, min(12, bonus + oi_bonus))
    enriched["score"] = max(0, min(99, int(round(float(enriched.get("score", 0)) + micro_bonus))))
    enriched["cvd"] = cvd
    enriched["open_interest"] = oi
    enriched["cvd_oi_bonus"] = micro_bonus
    if notes:
        reasons = list(enriched.get("reasons", []))
        reasons.append(f"CVD/OI: {'، '.join(notes)} ({micro_bonus:+d} نقاط)")
        enriched["reasons"] = reasons[:8]
    return enriched


def CONFIDENCE_2_apply(result: Dict, direction: str) -> Optional[Dict]:
    if not CONFIDENCE_2_ENABLED:
        return result
    base = float(result.get("score", 0))
    liquidity = result.get("liquidity", {})
    flow = result.get("whale_flow", {})
    cvd = result.get("cvd", {})
    oi = result.get("open_interest", {})

    technical = max(0.0, min(100.0, base))
    trend = max(0.0, min(100.0, 50 + (float(result.get("adx", 0)) - 18) * 2.5))
    volume = max(0.0, min(100.0, 45 + (float(result.get("volume_ratio", 1)) - 1) * 35))
    market = 70.0 if result.get("market_regime") in ("إيجابي", "هابط") else 58.0

    liq_bonus = float(liquidity.get("bonus", 0)) if liquidity.get("available") else 0.0
    liquidity_score = max(0.0, min(100.0, 55 + liq_bonus * 5))
    flow_bonus = float(flow.get("bonus", 0)) if flow.get("available") else 0.0
    orderflow_score = max(0.0, min(100.0, 55 + flow_bonus * 4))
    if cvd.get("available"):
        cvd_alignment = float(cvd.get("cvd_pct", 0)) * (1 if direction == "long" else -1)
        orderflow_score = max(0.0, min(100.0, orderflow_score + cvd_alignment * 55))
    oi_score = 55.0 + float(oi.get("bonus", 0)) * 5 if oi.get("available") else 50.0

    final = (
        technical * 0.30 + trend * 0.18 + orderflow_score * 0.17 +
        liquidity_score * 0.15 + volume * 0.10 + oi_score * 0.07 + market * 0.03
    )
    final = int(round(max(0.0, min(99.0, final))))
    enriched = dict(result)
    enriched["legacy_score"] = int(round(base))
    enriched["score"] = final
    enriched["confidence2"] = {
        "technical": round(technical, 1), "trend": round(trend, 1),
        "orderflow": round(orderflow_score, 1), "liquidity": round(liquidity_score, 1),
        "volume": round(volume, 1), "open_interest": round(oi_score, 1),
        "market": round(market, 1), "final": final,
    }
    if final < CONFIDENCE_MIN_FINAL:
        log_fn = SHORT_log_rejection if direction == "short" and "SHORT_log_rejection" in globals() else log_rejection
        log_fn(result.get("symbol", "?"), "Confidence Score 2.0 منخفض", {"score": final, "required": CONFIDENCE_MIN_FINAL})
        return None
    return enriched


def CVD_OI_message_section(result: Dict) -> str:
    cvd = result.get("cvd", {})
    oi = result.get("open_interest", {})
    conf = result.get("confidence2", {})
    lines = ["\n\n📈 CVD + Open Interest"]
    if cvd.get("available"):
        coverage = float(cvd.get("coverage_minutes", 0))
        partial = " (عينة جزئية)" if cvd.get("partial_window") else ""
        lines.append(
            f"CVD: {float(cvd.get('cvd_pct', 0))*100:+.1f}% | آخر جزء: "
            f"{float(cvd.get('recent_cvd_pct', 0))*100:+.1f}% | تغطية {coverage:.1f}د{partial}"
        )
    else:
        lines.append("CVD: غير متاح مؤقتًا")
    if oi.get("available"):
        lines.append(
            f"Open Interest: {LIQUIDITY_money(oi.get('open_interest_usdt', 0))} | "
            f"التغير: {float(oi.get('change_pct', 0)):+.2f}% ({oi.get('period', '5m')})"
        )
    else:
        lines.append("Open Interest: غير متاح من مصدر البيانات الحالي")
    if conf:
        lines.append(
            f"Confidence 2.0: {int(conf.get('final', result.get('score', 0)))}% "
            f"| فني {conf.get('technical', 0):.0f} | تدفق {conf.get('orderflow', 0):.0f} "
            f"| سيولة {conf.get('liquidity', 0):.0f}"
        )
    return "\n".join(lines)


# إعادة تغليف الطبقة الحالية حتى تمر جميع الإشارات عبر CVD/OI والثقة الموزونة.
_V2_LONG_ANALYZE = analyze_symbol
_V2_SHORT_ANALYZE = SHORT_analyze_symbol
_V2_STOCK_ANALYZE = STOCK_SHORT_analyze_symbol
_V2_LONG_MESSAGE = signal_message
_V2_SHORT_MESSAGE = SHORT_signal_message
_V2_STOCK_MESSAGE = STOCK_SHORT_signal_message


def analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    result = _V2_LONG_ANALYZE(symbol, market)
    if not result:
        return None
    result = CVD_OI_apply(result, "long", futures=False)
    return CONFIDENCE_2_apply(result, "long") if result else None


def SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    result = _V2_SHORT_ANALYZE(symbol, market)
    if not result:
        return None
    result = CVD_OI_apply(result, "short", futures=True)
    return CONFIDENCE_2_apply(result, "short") if result else None


def STOCK_SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    # الدالة الأصلية تستدعي SHORT_analyze_symbol ديناميكيًا؛ لذلك لا نكرر CVD/OI مرتين.
    return _V2_STOCK_ANALYZE(symbol, market)


def _append_cvd_oi_section(base: str, result: Dict) -> str:
    section = CVD_OI_message_section(result)
    marker = "\n\n⚠️"
    return base.replace(marker, section + marker, 1) if marker in base else base + section


def signal_message(result: Dict) -> str:
    return _append_cvd_oi_section(_V2_LONG_MESSAGE(result), result)


def SHORT_signal_message(result: Dict) -> str:
    return _append_cvd_oi_section(_V2_SHORT_MESSAGE(result), result)


def STOCK_SHORT_signal_message(result: Dict) -> str:
    return _append_cvd_oi_section(_V2_STOCK_MESSAGE(result), result)



# ============================================================
# V11 — ربط فعلي للثقة الموزونة + فلتر ارتداد BTC للشورت
# يطبق بعد جميع طبقات الفني والسيولة والحيتان وCVD وOpen Interest.
# ============================================================
BTC_REBOUND_FILTER_ENABLED = os.getenv("BTC_REBOUND_FILTER_ENABLED", "1") == "1"
BTC_REBOUND_CACHE_SECONDS = int(os.getenv("BTC_REBOUND_CACHE_SECONDS", "35"))
BTC_REBOUND_5M_MIN_PCT = float(os.getenv("BTC_REBOUND_5M_MIN_PCT", "0.22"))
BTC_REBOUND_15M_MIN_PCT = float(os.getenv("BTC_REBOUND_15M_MIN_PCT", "0.35"))
BTC_REBOUND_RSI_MIN = float(os.getenv("BTC_REBOUND_RSI_MIN", "45"))
BTC_REBOUND_REQUIRED_REL_WEAKNESS = float(os.getenv("BTC_REBOUND_REQUIRED_REL_WEAKNESS", "-1.00"))
BTC_REBOUND_HARD_REQUIRED_REL_WEAKNESS = float(os.getenv("BTC_REBOUND_HARD_REQUIRED_REL_WEAKNESS", "-1.60"))
WEIGHTED_CONFIDENCE_MIN_SHORT = int(os.getenv("WEIGHTED_CONFIDENCE_MIN_SHORT", "80"))
WEIGHTED_CONFIDENCE_MIN_LONG = int(os.getenv("WEIGHTED_CONFIDENCE_MIN_LONG", "78"))
WEIGHTED_HARD_BLOCK_CONTRADICTIONS = int(os.getenv("WEIGHTED_HARD_BLOCK_CONTRADICTIONS", "2"))

_BTC_REBOUND_CACHE = {"data": None, "updated_at": 0.0}


def _V11_btc_rebound_snapshot() -> Dict:
    now = time.time()
    cached = _BTC_REBOUND_CACHE.get("data")
    if cached and now - float(_BTC_REBOUND_CACHE.get("updated_at", 0)) < BTC_REBOUND_CACHE_SECONDS:
        return dict(cached)
    try:
        c5 = get_klines("BTCUSDT", "5m", 80)[:-1]
        c15 = get_klines("BTCUSDT", "15m", 80)[:-1]
        z5 = [float(c["close"]) for c in c5]
        z15 = [float(c["close"]) for c in c15]
        r5 = float(rsi(z5)[-1] or 50)
        r15 = float(rsi(z15)[-1] or 50)
        e7 = float(ema(z5, 7)[-1] or z5[-1])
        e25 = float(ema(z5, 25)[-1] or z5[-1])
        rise5 = pct_change(z5[-2], z5[-1])
        rise15 = pct_change(z5[-4], z5[-1])
        recovery = pct_change(min(z5[-6:]), z5[-1])
        bullish_candle = c5[-1]["close"] > c5[-1]["open"] and c5[-1]["close"] >= c5[-2]["high"] * 0.998
        soft = bool(
            (rise5 >= BTC_REBOUND_5M_MIN_PCT or recovery >= BTC_REBOUND_15M_MIN_PCT)
            and r5 >= BTC_REBOUND_RSI_MIN
            and z5[-1] >= e7
        )
        hard = bool(
            soft and rise15 >= BTC_REBOUND_15M_MIN_PCT
            and z5[-1] > e7 >= e25
            and r5 >= max(BTC_REBOUND_RSI_MIN + 3, 48)
            and bullish_candle
        )
        data = {
            "available": True, "soft": soft, "hard": hard,
            "rise_5m": rise5, "rise_15m": rise15, "recovery": recovery,
            "rsi5": r5, "rsi15": r15, "above_ema7": z5[-1] >= e7,
            "ema7_above_ema25": e7 >= e25,
        }
    except Exception as exc:
        print(f"BTC rebound filter error: {exc}", flush=True)
        data = {"available": False, "soft": False, "hard": False}
    _BTC_REBOUND_CACHE["data"] = data
    _BTC_REBOUND_CACHE["updated_at"] = now
    return dict(data)


def _V11_component_scores(result: Dict, direction: str) -> Dict:
    legacy = float(result.get("legacy_score", result.get("score", 0)))
    adx_value = float(result.get("adx", 0))
    volume_ratio = float(result.get("volume_ratio", 1))
    technical = max(0.0, min(100.0, legacy))
    trend = max(0.0, min(100.0, 48 + (adx_value - 18) * 2.2))
    volume = max(0.0, min(100.0, 42 + (volume_ratio - 1) * 30))

    flow = result.get("whale_flow", {})
    cvd = result.get("cvd", {})
    delta = float(flow.get("delta_pct", 0)) if flow.get("available") else 0.0
    whale_delta = float(flow.get("whale_delta_pct", 0)) if flow.get("available") else 0.0
    cvd_all = float(cvd.get("cvd_pct", 0)) if cvd.get("available") else delta
    cvd_recent = float(cvd.get("recent_cvd_pct", 0)) if cvd.get("available") else delta
    sign = 1.0 if direction == "long" else -1.0
    aligned = sign * (0.42 * delta + 0.18 * whale_delta + 0.25 * cvd_all + 0.15 * cvd_recent)
    orderflow = max(0.0, min(100.0, 50 + aligned * 150))

    liq = result.get("liquidity", {})
    ratio = float(liq.get("bid_ask_ratio", 1.0)) if liq.get("available") else 1.0
    if direction == "short":
        liquidity = max(0.0, min(100.0, 55 + (1.0 - ratio) * 75))
    else:
        liquidity = max(0.0, min(100.0, 55 + (ratio - 1.0) * 55))

    oi = result.get("open_interest", {})
    oi_change = float(oi.get("change_pct", 0)) if oi.get("available") else 0.0
    oi_score = 55.0
    if oi.get("available"):
        if direction == "short":
            oi_score = 55 + oi_change * 10
        else:
            oi_score = 55 + oi_change * 10
        oi_score = max(0.0, min(100.0, oi_score))

    regime = str(result.get("market_regime", ""))
    if direction == "short":
        market = 90.0 if "هابط قوي" in regime else 80.0 if "هابط" in regime else 55.0
    else:
        market = 85.0 if regime in ("إيجابي", "محايد") else 45.0

    final = (
        technical * 0.50 + orderflow * 0.20 + liquidity * 0.15 +
        oi_score * 0.10 + market * 0.05
    )
    return {
        "technical": technical, "trend": trend, "volume": volume,
        "orderflow": orderflow, "liquidity": liquidity,
        "open_interest": oi_score, "market": market,
        "final_raw": final, "delta": delta, "cvd": cvd_all,
        "cvd_recent": cvd_recent, "oi_change": oi_change, "ratio": ratio,
    }


def _V11_classify_move(direction: str, scores: Dict) -> Dict:
    delta, cvd_all = scores["delta"], scores["cvd"]
    recent, oi_change = scores["cvd_recent"], scores["oi_change"]
    if direction == "short":
        if delta <= -0.12 and cvd_all <= -0.10 and oi_change >= 0.35:
            return {"name": "دخول بائعين جدد", "quality": "قوي", "adjustment": 5}
        if delta <= -0.12 and cvd_all <= -0.10 and oi_change <= -0.35:
            return {"name": "تصفية لونغ/خروج مراكز", "quality": "حذر", "adjustment": -4}
        if cvd_all <= -0.08 and recent >= 0.08:
            return {"name": "ضغط بيعي مع ارتداد شراء حديث", "quality": "حذر", "adjustment": -4}
        if cvd_all >= 0.12:
            return {"name": "شراء يعاكس الشورت", "quality": "ضعيف", "adjustment": -8}
        return {"name": "ضغط هابط غير محسوم", "quality": "متوسط", "adjustment": 0}
    if delta >= 0.12 and cvd_all >= 0.10 and oi_change >= 0.35:
        return {"name": "دخول مشترين جدد", "quality": "قوي", "adjustment": 5}
    if delta >= 0.12 and cvd_all >= 0.10 and oi_change <= -0.35:
        return {"name": "تغطية شورت/خروج مراكز", "quality": "حذر", "adjustment": -4}
    if cvd_all >= 0.08 and recent <= -0.08:
        return {"name": "ضغط شرائي مع بيع حديث", "quality": "حذر", "adjustment": -4}
    if cvd_all <= -0.12:
        return {"name": "بيع يعاكس الشراء", "quality": "ضعيف", "adjustment": -8}
    return {"name": "ضغط صاعد غير محسوم", "quality": "متوسط", "adjustment": 0}


def _V11_weighted_apply(result: Dict, direction: str) -> Optional[Dict]:
    if not result:
        return None
    scores = _V11_component_scores(result, direction)
    movement = _V11_classify_move(direction, scores)
    final = float(scores["final_raw"]) + float(movement["adjustment"])
    contradictions = 0
    contradiction_notes = []

    if direction == "short":
        if scores["cvd"] >= 0.15:
            contradictions += 1; contradiction_notes.append("CVD شرائي")
            final -= 6
        if scores["delta"] >= 0.15:
            contradictions += 1; contradiction_notes.append("Delta شرائي")
            final -= 5
        if scores["oi_change"] <= -0.50:
            contradiction_notes.append("OI ينخفض مع الهبوط")
            final -= 4
        if scores["ratio"] >= 1.80:
            contradictions += 1; contradiction_notes.append("طلبات قوية في دفتر الأوامر")
            final -= 7
    else:
        if scores["cvd"] <= -0.15:
            contradictions += 1; contradiction_notes.append("CVD بيعي")
            final -= 6
        if scores["delta"] <= -0.15:
            contradictions += 1; contradiction_notes.append("Delta بيعي")
            final -= 5
        if scores["oi_change"] <= -0.50:
            contradiction_notes.append("OI ينخفض مع الصعود")
            final -= 4
        if scores["ratio"] <= 0.60:
            contradictions += 1; contradiction_notes.append("عروض قوية في دفتر الأوامر")
            final -= 7

    btc = {"available": False, "soft": False, "hard": False}
    if direction == "short" and BTC_REBOUND_FILTER_ENABLED:
        btc = _V11_btc_rebound_snapshot()
        relative_weakness = _directional_relative_move(result, direction)
        if btc.get("hard") and relative_weakness > BTC_REBOUND_HARD_REQUIRED_REL_WEAKNESS:
            SHORT_log_rejection(result.get("symbol", "?"), "رفض شورت: ارتداد BTC قوي والعملة ليست أضعف بما يكفي", {
                "relative_weakness": round(relative_weakness, 2),
                "required": BTC_REBOUND_HARD_REQUIRED_REL_WEAKNESS,
                "btc5": round(float(btc.get("rise_5m", 0)), 3),
                "btc15": round(float(btc.get("rise_15m", 0)), 3),
                "btc_rsi5": round(float(btc.get("rsi5", 0)), 1),
            })
            return None
        if btc.get("soft") and relative_weakness > BTC_REBOUND_REQUIRED_REL_WEAKNESS:
            final -= 7
            contradiction_notes.append("BTC يرتد والضعف النسبي غير كافٍ")

    final = int(round(max(0.0, min(99.0, final))))
    min_required = WEIGHTED_CONFIDENCE_MIN_SHORT if direction == "short" else WEIGHTED_CONFIDENCE_MIN_LONG
    if contradictions >= WEIGHTED_HARD_BLOCK_CONTRADICTIONS:
        log_fn = SHORT_log_rejection if direction == "short" else log_rejection
        log_fn(result.get("symbol", "?"), "رفض بسبب تعارضات ميكروستركشر متعددة", {
            "contradictions": contradictions, "notes": contradiction_notes, "score": final,
        })
        return None
    if final < min_required:
        log_fn = SHORT_log_rejection if direction == "short" else log_rejection
        log_fn(result.get("symbol", "?"), "الثقة الموزونة النهائية منخفضة", {
            "score": final, "required": min_required, "notes": contradiction_notes,
        })
        return None

    enriched = dict(result)
    enriched["score"] = final
    enriched["weighted_confidence"] = {
        **{k: round(v, 1) if isinstance(v, (int, float)) else v for k, v in scores.items()},
        "final": final, "movement": movement, "contradictions": contradiction_notes,
        "btc_rebound": btc,
    }
    return enriched


def _V11_weighted_message_section(result: Dict) -> str:
    data = result.get("weighted_confidence", {})
    if not data:
        return ""
    movement = data.get("movement", {})
    btc = data.get("btc_rebound", {})
    lines = [
        "\n\n🔥 قراءة استمرار الحركة",
        f"النوع: {movement.get('name', 'غير محسوم')} | الجودة: {movement.get('quality', 'متوسط')}",
        f"الثقة الموزونة: {int(data.get('final', result.get('score', 0)))}% "
        f"| فني {float(data.get('technical', 0)):.0f} "
        f"| تدفق {float(data.get('orderflow', 0)):.0f} "
        f"| سيولة {float(data.get('liquidity', 0)):.0f} "
        f"| OI {float(data.get('open_interest', 0)):.0f}",
    ]
    notes = data.get("contradictions", [])
    if notes:
        lines.append("تنبيهات: " + "، ".join(notes))
    if btc.get("available"):
        state = "ارتداد قوي" if btc.get("hard") else "ارتداد" if btc.get("soft") else "لا يوجد ارتداد مؤثر"
        lines.append(
            f"BTC: {state} | 5m {float(btc.get('rise_5m', 0)):+.2f}% "
            f"| 15m {float(btc.get('rise_15m', 0)):+.2f}% | RSI5 {float(btc.get('rsi5', 0)):.1f}"
        )
    return "\n".join(lines)


# تغليف نهائي: جميع إشارات الشورت والشراء تمر عبر الربط الموزون الحقيقي.
_V11_PREV_LONG_ANALYZE = analyze_symbol
_V11_PREV_SHORT_ANALYZE = SHORT_analyze_symbol
_V11_PREV_STOCK_ANALYZE = STOCK_SHORT_analyze_symbol
_V11_PREV_LONG_MESSAGE = signal_message
_V11_PREV_SHORT_MESSAGE = SHORT_signal_message
_V11_PREV_STOCK_MESSAGE = STOCK_SHORT_signal_message


def analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    return _V11_weighted_apply(_V11_PREV_LONG_ANALYZE(symbol, market), "long")


def SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    return _V11_weighted_apply(_V11_PREV_SHORT_ANALYZE(symbol, market), "short")


def STOCK_SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    # محرك الأسهم الأصلي يستدعي SHORT_analyze_symbol الحالي، فلا نكرر الوزن مرتين.
    return _V11_PREV_STOCK_ANALYZE(symbol, market)


def _V11_append_weighted(base: str, result: Dict) -> str:
    section = _V11_weighted_message_section(result)
    marker = "\n\n⚠️"
    return base.replace(marker, section + marker, 1) if marker in base else base + section


def signal_message(result: Dict) -> str:
    return _V11_append_weighted(_V11_PREV_LONG_MESSAGE(result), result)


def SHORT_signal_message(result: Dict) -> str:
    return _V11_append_weighted(_V11_PREV_SHORT_MESSAGE(result), result)


def STOCK_SHORT_signal_message(result: Dict) -> str:
    return _V11_append_weighted(_V11_PREV_STOCK_MESSAGE(result), result)


# ============================================================
# V13 Probability Engine — Shadow Mode
# طبقة احتمالية فوق جميع المحركات الحالية دون حذف أو تعطيل أي فلتر سابق.
# في Shadow Mode يتم الحساب والعرض والتسجيل فقط، ولا تُرفض الإشارة بسببه.
# ============================================================
PROBABILITY_ENGINE_ENABLED = os.getenv("PROBABILITY_ENGINE_ENABLED", "1") == "1"
PROBABILITY_SHADOW_MODE = os.getenv("PROBABILITY_SHADOW_MODE", "1") == "1"
PROBABILITY_MIN_LONG = float(os.getenv("PROBABILITY_MIN_LONG", "75"))
PROBABILITY_MIN_SHORT = float(os.getenv("PROBABILITY_MIN_SHORT", "75"))
PROBABILITY_MIN_STOCK_SHORT = float(os.getenv("PROBABILITY_MIN_STOCK_SHORT", "78"))
PROBABILITY_LOG_ENABLED = os.getenv("PROBABILITY_LOG_ENABLED", "1") == "1"
PROBABILITY_LOG_FILE = Path(os.getenv("PROBABILITY_LOG_FILE", "probability_shadow_signals.jsonl"))
PROBABILITY_LOG_LOCK = Lock()


def _PROB_clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _PROB_component(result: Dict, key: str, default: float = 50.0) -> float:
    weighted = result.get("weighted_confidence", {})
    value = weighted.get(key, default)
    try:
        return _PROB_clamp(float(value))
    except (TypeError, ValueError):
        return default


def PROBABILITY_estimate(result: Dict, direction: str, engine: str) -> Dict:
    """
    تقدير احتمالي تجريبي مبني على المعلومات الموجودة فعليًا في الإشارة.
    لا يدّعي أنه احتمال مُعاير إحصائيًا قبل جمع نتائج كافية؛ لذلك يبدأ Shadow Mode.
    """
    weighted = result.get("weighted_confidence", {})
    technical = _PROB_component(result, "technical", float(result.get("score", 50)))
    trend = _PROB_component(result, "trend", technical)
    volume = _PROB_component(result, "volume", 50)
    orderflow = _PROB_component(result, "orderflow", 50)
    liquidity = _PROB_component(result, "liquidity", 50)
    oi_score = _PROB_component(result, "open_interest", 50)
    market = _PROB_component(result, "market", 50)

    flow = result.get("whale_flow", {})
    cvd = result.get("cvd", {})
    oi = result.get("open_interest", {})
    liq = result.get("liquidity", {})

    sign = 1.0 if direction == "long" else -1.0
    delta = float(flow.get("delta_pct", 0) or 0) if flow.get("available") else 0.0
    whale_delta = float(flow.get("whale_delta_pct", 0) or 0) if flow.get("available") else 0.0
    cvd_all = float(cvd.get("cvd_pct", 0) or 0) if cvd.get("available") else delta
    cvd_recent = float(cvd.get("recent_cvd_pct", 0) or 0) if cvd.get("available") else cvd_all
    oi_change = float(oi.get("change_pct", 0) or 0) if oi.get("available") else 0.0
    bid_ask_ratio = float(liq.get("bid_ask_ratio", 1) or 1) if liq.get("available") else 1.0

    # سرعة/اتجاه الحركة النسبية الحالية.
    relative_move = _directional_relative_move(result, direction)
    direction_move = relative_move if direction == "long" else -relative_move
    momentum_score = _PROB_clamp(50 + direction_move * 7.5)

    # توافق الميكروستركشر مع اتجاه الصفقة.
    aligned_flow = sign * (
        0.38 * delta +
        0.17 * whale_delta +
        0.28 * cvd_all +
        0.17 * cvd_recent
    )
    flow_score = _PROB_clamp(50 + aligned_flow * 170)

    # دفتر الأوامر: النسبة الأعلى تدعم LONG والأقل تدعم SHORT.
    book_edge = (bid_ask_ratio - 1.0) if direction == "long" else (1.0 - bid_ask_ratio)
    book_score = _PROB_clamp(50 + book_edge * 65)

    # OI المرتفع مع حركة متوافقة يرفع جودة الاستمرار، بينما انخفاضه يعطي ثقة أقل.
    oi_directional = _PROB_clamp(52 + oi_change * 9)
    if not oi.get("available"):
        oi_directional = 50.0

    # النموذج يبدأ كتجميع احتمالي محافظ، ثم تتم معايرته لاحقًا من النتائج الفعلية.
    raw = (
        technical * 0.24 +
        trend * 0.13 +
        volume * 0.09 +
        momentum_score * 0.10 +
        orderflow * 0.11 +
        flow_score * 0.11 +
        liquidity * 0.07 +
        book_score * 0.05 +
        oi_score * 0.04 +
        oi_directional * 0.03 +
        market * 0.03
    )

    contradictions = list(weighted.get("contradictions", []))
    penalty = min(15.0, len(contradictions) * 3.0)
    raw -= penalty

    # ضغط القيم المتطرفة لأن النسخة الأولى ليست معايرة بعد.
    probability = 50 + (raw - 50) * 0.82
    probability = int(round(_PROB_clamp(probability, 5, 95)))

    threshold = (
        PROBABILITY_MIN_STOCK_SHORT
        if engine == "stock_short"
        else PROBABILITY_MIN_SHORT
        if direction == "short"
        else PROBABILITY_MIN_LONG
    )

    evidence = {
        "technical": round(technical, 1),
        "trend": round(trend, 1),
        "volume": round(volume, 1),
        "momentum": round(momentum_score, 1),
        "orderflow": round(orderflow, 1),
        "flow_alignment": round(flow_score, 1),
        "liquidity": round(liquidity, 1),
        "orderbook": round(book_score, 1),
        "open_interest": round(oi_directional, 1),
        "market": round(market, 1),
    }
    strongest = sorted(evidence.items(), key=lambda item: item[1], reverse=True)[:3]
    weakest = sorted(evidence.items(), key=lambda item: item[1])[:2]

    return {
        "enabled": True,
        "shadow_mode": PROBABILITY_SHADOW_MODE,
        "engine": engine,
        "direction": direction,
        "probability": probability,
        "threshold": threshold,
        "would_pass": probability >= threshold,
        "raw_score": round(raw, 2),
        "penalty": round(penalty, 1),
        "strongest": strongest,
        "weakest": weakest,
        "components": evidence,
        "calibration_status": "shadow_un calibrated".replace(" ", ""),
    }


def PROBABILITY_log(result: Dict) -> None:
    if not PROBABILITY_LOG_ENABLED:
        return
    data = result.get("probability_engine")
    if not data:
        return
    row = {
        "time": int(time.time() * 1000),
        "symbol": result.get("symbol"),
        "entry": result.get("entry"),
        "score": result.get("score"),
        "mode": result.get("mode"),
        "probability_engine": data,
    }
    try:
        with PROBABILITY_LOG_LOCK:
            with PROBABILITY_LOG_FILE.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"Probability log error: {exc}", flush=True)


def PROBABILITY_apply(result: Optional[Dict], direction: str, engine: str) -> Optional[Dict]:
    if not result or not PROBABILITY_ENGINE_ENABLED:
        return result
    enriched = dict(result)
    data = PROBABILITY_estimate(enriched, direction, engine)
    enriched["probability_engine"] = data
    PROBABILITY_log(enriched)

    # الوضع الافتراضي Shadow: لا يمنع أي إشارة حالية.
    if PROBABILITY_SHADOW_MODE:
        return enriched

    # عند تعطيل Shadow Mode فقط يصبح فلترًا فعليًا.
    if not data.get("would_pass", False):
        log_fn = SHORT_log_rejection if direction == "short" else log_rejection
        log_fn(
            enriched.get("symbol", "?"),
            "رفض بواسطة Probability Engine",
            {
                "probability": data.get("probability"),
                "required": data.get("threshold"),
                "engine": engine,
            },
        )
        return None
    return enriched


def PROBABILITY_message_section(result: Dict) -> str:
    data = result.get("probability_engine", {})
    if not data:
        return ""
    state = "يمر لاحقًا" if data.get("would_pass") else "أقل من الحد التجريبي"
    mode = "Shadow — لا يمنع الإشارة" if data.get("shadow_mode") else "فلتر فعلي"
    strongest = "، ".join(f"{name} {value:.0f}" for name, value in data.get("strongest", []))
    weakest = "، ".join(f"{name} {value:.0f}" for name, value in data.get("weakest", []))
    return (
        "\n\n🧠 Probability Engine V13\n"
        f"الاحتمال التقديري: {int(data.get('probability', 0))}% "
        f"| الحد: {float(data.get('threshold', 0)):.0f}% | {state}\n"
        f"الوضع: {mode}\n"
        f"أقوى العوامل: {strongest or 'غير متاح'}\n"
        f"أضعف العوامل: {weakest or 'غير متاح'}\n"
        "ملاحظة: النسبة تجريبية وتحتاج معايرة بنتائج الصفقات الفعلية."
    )


# تغليف V13 فوق جميع الطبقات السابقة بدون حذف أي منطق.
_V13_PREV_LONG_ANALYZE = analyze_symbol
_V13_PREV_SHORT_ANALYZE = SHORT_analyze_symbol
_V13_PREV_STOCK_ANALYZE = STOCK_SHORT_analyze_symbol
_V13_PREV_LONG_MESSAGE = signal_message
_V13_PREV_SHORT_MESSAGE = SHORT_signal_message
_V13_PREV_STOCK_MESSAGE = STOCK_SHORT_signal_message


def analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    return PROBABILITY_apply(_V13_PREV_LONG_ANALYZE(symbol, market), "long", "spot")


def SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    return PROBABILITY_apply(_V13_PREV_SHORT_ANALYZE(symbol, market), "short", "crypto_short")


def STOCK_SHORT_analyze_symbol(symbol: str, market: Dict) -> Optional[Dict]:
    # يستدعي محرك الأسهم السابق، ثم نعيد توصيف الاحتمال كـ stock_short.
    result = _V13_PREV_STOCK_ANALYZE(symbol, market)
    if not result:
        return None
    enriched = dict(result)
    enriched.pop("probability_engine", None)
    return PROBABILITY_apply(enriched, "short", "stock_short")


def _V13_append_probability(base: str, result: Dict) -> str:
    section = PROBABILITY_message_section(result)
    marker = "\n\n⚠️"
    return base.replace(marker, section + marker, 1) if marker in base else base + section


def signal_message(result: Dict) -> str:
    return _V13_append_probability(_V13_PREV_LONG_MESSAGE(result), result)


def SHORT_signal_message(result: Dict) -> str:
    return _V13_append_probability(_V13_PREV_SHORT_MESSAGE(result), result)


def STOCK_SHORT_signal_message(result: Dict) -> str:
    return _V13_append_probability(_V13_PREV_STOCK_MESSAGE(result), result)



# ============================================================
# التشغيل الموحّد: شراء سبوت V2.4 + شورت Futures V1.0
# إشارات تيليجرام فقط، ولا يوجد تنفيذ أوامر تداول.
# ============================================================
import threading
import traceback

def run_long_bot() -> None:
    try:
        main()
    except Exception:
        print(f"Fatal LONG bot error:\n{traceback.format_exc()}", flush=True)
        raise

def run_short_bot() -> None:
    try:
        short_main()
    except Exception:
        print(f"Fatal SHORT bot error:\n{traceback.format_exc()}", flush=True)
        raise

def run_stock_short_bot() -> None:
    try:
        stock_short_main()
    except Exception:
        print(f"Fatal STOCK SHORT bot error:\n{traceback.format_exc()}", flush=True)
        raise

def combined_main() -> None:
    workers = {
        "long-spot-signals": run_long_bot,
        "short-futures-signals": run_short_bot,
        "stock-short-futures-signals": run_stock_short_bot,
    }
    threads: Dict[str, threading.Thread] = {}
    restart_counts: Dict[str, int] = {name: 0 for name in workers}

    def start_worker(name: str) -> None:
        thread = threading.Thread(target=workers[name], name=name, daemon=True)
        threads[name] = thread
        thread.start()
        print(f"Supervisor started: {name}", flush=True)

    for worker_name in workers:
        start_worker(worker_name)

    try:
        while True:
            for name, thread in list(threads.items()):
                if thread.is_alive():
                    continue
                restart_counts[name] += 1
                delay = min(60, 5 * restart_counts[name])
                print(f"Supervisor: {name} stopped; restarting in {delay}s", flush=True)
                time.sleep(delay)
                start_worker(name)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Supervisor shutdown requested.", flush=True)

if __name__ == "__main__":
    combined_main()
