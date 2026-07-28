import os, time, json
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple
import requests

BINANCE_BASE = "https://data-api.binance.vision"
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

SCAN_MINUTES = int(os.getenv("SCAN_MINUTES", "1"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "120"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
SYMBOL_REFRESH_MINUTES = int(os.getenv("SYMBOL_REFRESH_MINUTES", "30"))
MIN_DAILY_QUOTE_VOLUME = float(os.getenv("MIN_DAILY_QUOTE_VOLUME", "500000"))
PREFILTER_LIMIT = int(os.getenv("PREFILTER_LIMIT", "60"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "76"))
MIN_15M_VOLUME_RATIO = float(os.getenv("MIN_15M_VOLUME_RATIO", "1.6"))
MAX_1H_RISE_PCT = float(os.getenv("MAX_1H_RISE_PCT", "8"))
MAX_4H_RISE_PCT = float(os.getenv("MAX_4H_RISE_PCT", "14"))
MAX_15M_RISE_PCT = float(os.getenv("MAX_15M_RISE_PCT", "3"))
MAX_RISK_PCT = float(os.getenv("MAX_RISK_PCT", "4"))
MAX_CANDLE_BODY_PCT = float(os.getenv("MAX_CANDLE_BODY_PCT", "2.8"))
MAX_EMA20_DISTANCE_PCT = float(os.getenv("MAX_EMA20_DISTANCE_PCT", "1.5"))
MAX_CONSECUTIVE_GREEN = int(os.getenv("MAX_CONSECUTIVE_GREEN", "2"))

# فلاتر جودة الاختراق V2.2
BREAKOUT_CONFIRM_5M = os.getenv("BREAKOUT_CONFIRM_5M", "1") == "1"
CONFIRM_MAX_DIP_PCT = float(os.getenv("CONFIRM_MAX_DIP_PCT", "0.8"))
MIN_CONFIRM_CLOSE_LOCATION = float(os.getenv("MIN_CONFIRM_CLOSE_LOCATION", "0.55"))
MAX_RSI_HARD = float(os.getenv("MAX_RSI_HARD", "70"))
MAX_BREAKOUT_RANGE_ATR = float(os.getenv("MAX_BREAKOUT_RANGE_ATR", "2.0"))
MAX_BREAKOUT_BODY_ATR = float(os.getenv("MAX_BREAKOUT_BODY_ATR", "1.6"))
MIN_NEXT_RESISTANCE_PCT = float(os.getenv("MIN_NEXT_RESISTANCE_PCT", "2.5"))
MOMENTUM_MIN_NEXT_RESISTANCE_PCT = float(os.getenv("MOMENTUM_MIN_NEXT_RESISTANCE_PCT", "1.5"))
RESISTANCE_MAX_DISTANCE_PCT = float(os.getenv("RESISTANCE_MAX_DISTANCE_PCT", "15"))
RESISTANCE_SWING_WINDOW = int(os.getenv("RESISTANCE_SWING_WINDOW", "2"))
RESISTANCE_CLUSTER_TOLERANCE_PCT = float(os.getenv("RESISTANCE_CLUSTER_TOLERANCE_PCT", "0.45"))
RESISTANCE_MIN_TOUCHES = int(os.getenv("RESISTANCE_MIN_TOUCHES", "2"))
REJECTION_LOG_ENABLED = os.getenv("REJECTION_LOG_ENABLED", "1") == "1"
REJECTION_LOG_FILE = Path(os.getenv("REJECTION_LOG_FILE", "rejected_signals.jsonl"))

# فلتر سوق ذكي متعدد الفريمات V2.4
SMART_MARKET_FILTER = os.getenv("SMART_MARKET_FILTER", "1") == "1"
BTC_5M_DROP_BLOCK_PCT = float(os.getenv("BTC_5M_DROP_BLOCK_PCT", "0.75"))
BTC_15M_RSI_BLOCK = float(os.getenv("BTC_15M_RSI_BLOCK", "43"))
BTC_1H_DROP_BLOCK_PCT = float(os.getenv("BTC_1H_DROP_BLOCK_PCT", "1.2"))
BTC_WEAK_RSI_15M = float(os.getenv("BTC_WEAK_RSI_15M", "48"))
BTC_WEAK_RSI_5M = float(os.getenv("BTC_WEAK_RSI_5M", "45"))
BTC_WEAK_15M_DROP_PCT = float(os.getenv("BTC_WEAK_15M_DROP_PCT", "0.35"))
BTC_WEAK_MIN_RELATIVE_STRENGTH = float(os.getenv("BTC_WEAK_MIN_RELATIVE_STRENGTH", "1.75"))

# مسار الارتداد الذكي بعد الهبوط
REVERSAL_ENABLED = os.getenv("REVERSAL_ENABLED", "1") == "1"
REVERSAL_MIN_DRAWDOWN_PCT = float(os.getenv("REVERSAL_MIN_DRAWDOWN_PCT", "3.5"))
REVERSAL_MIN_VOLUME_15M = float(os.getenv("REVERSAL_MIN_VOLUME_15M", "1.5"))
REVERSAL_MIN_VOLUME_5M = float(os.getenv("REVERSAL_MIN_VOLUME_5M", "1.2"))
REVERSAL_MIN_RSI_NOW = float(os.getenv("REVERSAL_MIN_RSI_NOW", "42"))
REVERSAL_MAX_RSI_NOW = float(os.getenv("REVERSAL_MAX_RSI_NOW", "64"))
REVERSAL_MAX_RSI_RECENT_LOW = float(os.getenv("REVERSAL_MAX_RSI_RECENT_LOW", "40"))
REVERSAL_MIN_ADX_15M = float(os.getenv("REVERSAL_MIN_ADX_15M", "18"))
REVERSAL_MIN_SCORE = int(os.getenv("REVERSAL_MIN_SCORE", "84"))
REVERSAL_MAX_RISK_PCT = float(os.getenv("REVERSAL_MAX_RISK_PCT", "4.5"))

# مسار الزخم القوي مثل SHIB
MOMENTUM_ENABLED = os.getenv("MOMENTUM_ENABLED", "1") == "1"
MOMENTUM_MIN_VOLUME_15M = float(os.getenv("MOMENTUM_MIN_VOLUME_15M", "3.0"))
MOMENTUM_MIN_VOLUME_5M = float(os.getenv("MOMENTUM_MIN_VOLUME_5M", "2.0"))
MOMENTUM_MIN_ADX_15M = float(os.getenv("MOMENTUM_MIN_ADX_15M", "25"))
MOMENTUM_MIN_RSI_15M = float(os.getenv("MOMENTUM_MIN_RSI_15M", "54"))
MOMENTUM_MAX_RSI_15M = float(os.getenv("MOMENTUM_MAX_RSI_15M", "68"))
MOMENTUM_MAX_15M_RISE = float(os.getenv("MOMENTUM_MAX_15M_RISE", "5.5"))
MOMENTUM_MAX_1H_RISE = float(os.getenv("MOMENTUM_MAX_1H_RISE", "12"))
MOMENTUM_MAX_EMA20_DISTANCE = float(os.getenv("MOMENTUM_MAX_EMA20_DISTANCE", "2.6"))
MOMENTUM_MAX_GREEN = int(os.getenv("MOMENTUM_MAX_GREEN", "4"))
MOMENTUM_MIN_SCORE = int(os.getenv("MOMENTUM_MIN_SCORE", "84"))

MARKET_CACHE_SECONDS = int(os.getenv("MARKET_CACHE_SECONDS", "55"))
TRACK_RESULTS = os.getenv("TRACK_RESULTS", "1") == "1"
STATE_FILE = Path("spot_signal_state.json")
SESSION = requests.Session()
SYMBOL_CACHE = {"symbols": [], "updated_at": 0.0}
MARKET_CACHE = {"data": None, "updated_at": 0.0}
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
    SYMBOL_CACHE["symbols"] = [s for s,_ in rows]
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
    if MARKET_CACHE["data"] and now-float(MARKET_CACHE["updated_at"])<MARKET_CACHE_SECONDS: return dict(MARKET_CACHE["data"])
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
            required_room = MOMENTUM_MIN_NEXT_RESISTANCE_PCT if (momentum or reversal) else MIN_NEXT_RESISTANCE_PCT
            if overhead_pct < required_room:
                log_rejection(symbol, "مقاومة قريبة مؤكدة", {
                    "distance_pct": round(overhead_pct, 2),
                    "required_pct": required_room,
                    "resistance": round(float(overhead["level"]), 10),
                    "touches": int(overhead["touches"]),
                })
                return None

        if reversal:
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
        max_risk = REVERSAL_MAX_RISK_PCT if mode=="reversal" else MAX_RISK_PCT
        if risk<=0 or risk/price*100>max_risk: return None
        return {"symbol":symbol,"entry":price,"stop":stop,"tp1":price+1.5*risk,"tp2":price+2.2*risk,"tp3":price+3*risk,"risk_pct":risk/price*100,"score":min(score,99),"volume_ratio":vr15,"rsi":r15,"adx":a15,"setup":setup,"mode":mode,"reasons":reasons[:8],"candle_close":candle5["close_time"],"market_regime":market.get("regime","غير متاح"),"btc_1h":float(market.get("btc",{}).get("rise_1h",0)),"btc_15m":float(market.get("btc",{}).get("rise_15m",0)),"btc_rsi15":float(market.get("btc",{}).get("rsi15",0)),"relative_strength":rel}
    except Exception as exc:
        print(f"Analyze {symbol}: {exc}", flush=True); return None


def fmt(v: float) -> str:
    d=2 if v>=1000 else 4 if v>=1 else 5 if v>=0.01 else 8
    return f"{v:.{d}f}"


def signal_message(r: Dict) -> str:
    reasons="\n".join(f"• {x}" for x in r["reasons"]); kind="انطلاقة قوية" if r.get("mode")=="momentum" else "ارتداد ذكي" if r.get("mode")=="reversal" else "دخول متوازن"
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
    results.sort(key=lambda x:(2 if x.get("mode")=="momentum" else 1 if x.get("mode")=="reversal" else 0,x["score"],x["volume_ratio"]),reverse=True)
    sent=0
    for r in results[:MAX_ALERTS_PER_SCAN]:
        send_message(signal_message(r)); state.setdefault("alerts",{})[r["symbol"]]=r["candle_close"]
        state.setdefault("open_signals",{})[r["symbol"]]={"time":r["candle_close"],"last_checked":r["candle_close"],"entry":r["entry"],"stop":r["stop"],"tp1":r["tp1"],"tp2":r["tp2"],"tp3":r["tp3"],"reached":0,"mode":r.get("mode","balanced")}
        sent+=1; time.sleep(0.3)
    save_state(state); print(f"Scan finished | market={market.get('regime')} | universe={len(symbols)} | shortlist={len(shortlist)} | candidates={len(results)} | sent={sent}",flush=True)


def main() -> None:
    state=load_state(); send_message("✅ تم تشغيل بوت إشارات الشراء للسبوت V2.4.\nالمسار الأول: دخول متوازن وإعادة اختبار.\nالمسار الثاني: زخم قوي لالتقاط الانطلاقات.\nالمسار الثالث: ارتداد ذكي بعد الهبوط.\nتم تشديد حماية BTC متعددة الفريمات: إيقاف كامل وقت الهبوط القوي، والسماح وقت الضعف فقط للعملات ذات القوة النسبية العالية.\nإشارات فقط — بدون تداول تلقائي وبدون شورت وبدون WATCH.")
    while True:
        started=time.time()
        try: scan(state)
        except Exception as exc: print(f"Scan error: {exc}",flush=True)
        time.sleep(max(5,SCAN_MINUTES*60-(time.time()-started)))


if __name__ == "__main__": main()
