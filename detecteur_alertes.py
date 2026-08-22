#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Détecteur v4 — univers élargi + suivi de position + alertes de sortie
==========================================================================

CE QUI CHANGE (v3)
  Le détecteur ne se contente plus de signaler des entrées : il suit la vie
  de chaque signal, du déclenchement à la sortie, avec la logique EXACTE du
  backtest de tendance (backtest_tendance.py) :

    ENTRÉE   score de retournement >= 40 (RSI en excès + rejet de mèche +
             capitulation volume/écart MM20), PUIS confirmation : clôture
             au-delà de l'extrême de la bougie d'excès sous 3 bougies.
    SUIVI    à chaque scan horaire : stop structurel initial (pivot - 1,5 ATR),
             passage au point mort dès +1R, stop suiveur 3 x ATR qui ne
             recule jamais.
    SORTIE   alerte émise quand : le stop suiveur est touché, OU un signal
             d'excès INVERSE apparaît, OU la durée max (240 h) est atteinte.

  Chaque email contient un BLOC DE DONNÉES à coller dans le tableau de bord
  (l'application « Tableau des signaux ») pour visualiser entrées, stops
  et sorties sur une même vue. Le fichier signals.json du dépôt contient
  les mêmes données, toujours à jour.

MODE OBSERVATION
  Cette stratégie n'est PAS encore validée par backtest long. Tant que le
  verdict CSV n'est pas rendu, ces alertes servent à OBSERVER et à remplir
  le journal — pas à engager de l'argent.

INSTALLATION : inchangée (secrets SMTP + workflow scan.yml). Une seule
  modification au workflow : la ligne "git add" doit inclure signals.json.
"""

import json
import os
import smtplib
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone
from email.message import EmailMessage

# --- univers scanné -------------------------------------------------------
# Les 30 lignes du portefeuille sont TOUJOURS incluses, quel que soit leur rang.
PORTFOLIO = [
    "SOLUSD", "AVAXUSD", "SUIUSD", "ADAUSD", "XRPUSD", "LINKUSD", "ETHUSD",
    "DOGEUSD", "DOTUSD", "APTUSD", "NEARUSD", "TIAUSD", "INJUSD", "TAOUSD",
    "FETUSD", "ARBUSD", "RENDERUSD", "JUPUSD", "ENAUSD", "ONDOUSD",
    "STXUSD", "POLUSD", "AAVEUSD", "TONUSD", "XLMUSD", "FILUSD",
    "HBARUSD", "GRTUSD", "PEPEUSD", "SHIBUSD",
]

# Le reste de l'univers est découvert automatiquement à chaque scan :
# toutes les paires USD de Kraken, classées par volume quotidien en dollars,
# les MAX_PAIRS premières retenues. Un volume minimal écarte les paires
# trop illiquides, où le spread mangerait tout avantage éventuel.
MAX_PAIRS = 150            # taille totale de l'univers scanné
MIN_DOLLAR_VOL = 200_000   # volume 24h minimum, en dollars
UNIVERSE_CACHE_H = 24      # l'univers n'est recalculé qu'une fois par jour

EXCLUS = {"USDT", "USDC", "DAI", "USDG", "EURC", "PYUSD", "TUSD", "USDS",
          "RLUSD", "FDUSD", "USD", "ZUSD", "EUR", "GBP", "AUD", "CAD",
          "CHF", "JPY"}

INTERVAL_MIN = 60
SCORE_MIN = 40
RSI_LEN, MA_LEN, ATR_LEN = 14, 20, 14
CONFIRM_BARS = 3
ATR_STOP_MULT = 1.5
TRAIL_MULT = 3.0
MAX_HOLD = 240
COOLDOWN_HOURS = 24
HISTORY_KEEP = 50

BASE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE, "alerts_state.json")
SIGNALS_FILE = os.path.join(BASE, "signals.json")

MACRO_EVENTS_UTC = [
    "2026-09-11 12:30", "2026-09-16 18:00", "2026-10-14 12:30",
    "2026-10-28 18:00", "2026-11-10 13:30", "2026-12-09 19:00",
    "2026-12-10 13:30",
]
MACRO_WINDOW_H = 2
API = "https://api.kraken.com/0/public/OHLC"
API_PAIRS = "https://api.kraken.com/0/public/AssetPairs"
API_TICKER = "https://api.kraken.com/0/public/Ticker"

# ----------------------------------------------------------- indicateurs

def rsi_series(closes, length=RSI_LEN):
    out = [None] * len(closes)
    if len(closes) <= length:
        return out
    g = l_ = 0.0
    for i in range(1, length + 1):
        d = closes[i] - closes[i - 1]
        g += max(d, 0.0); l_ += max(-d, 0.0)
    ag, al = g / length, l_ / length
    out[length] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    for i in range(length + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (length - 1) + max(d, 0.0)) / length
        al = (al * (length - 1) + max(-d, 0.0)) / length
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def atr_series(candles, length=ATR_LEN):
    out = [None] * len(candles)
    trs = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        trs.append(max(c["h"] - c["l"], abs(c["h"] - p["c"]), abs(c["l"] - p["c"])))
    if len(trs) < length:
        return out
    a = sum(trs[:length]) / length
    out[length] = a
    for i in range(length + 1, len(candles)):
        a = (a * (length - 1) + trs[i - 1]) / length
        out[i] = a
    return out


def score_reversal(candles, i, rsi):
    r = rsi[i]
    if r is None or i < MA_LEN + 1:
        return 0, None
    c = candles[i]
    rng = c["h"] - c["l"]
    if rng <= 0:
        return 0, None
    if r <= 30:
        direction, pts_rsi = "long", 40 * min(1.0, (30 - r) / 20)
    elif r >= 70:
        direction, pts_rsi = "short", 40 * min(1.0, (r - 70) / 20)
    else:
        return 0, None
    bh, bl = max(c["o"], c["c"]), min(c["o"], c["c"])
    wick = (bl - c["l"]) / rng if direction == "long" else (c["h"] - bh) / rng
    pts_wick = 30 * min(1.0, wick / 0.5)
    vols = [candles[j]["v"] for j in range(i - MA_LEN, i)]
    va = sum(vols) / len(vols)
    vr = c["v"] / va if va > 0 else 0
    ma = sum(candles[j]["c"] for j in range(i - MA_LEN, i)) / MA_LEN
    dev = abs(c["c"] - ma) / ma if ma > 0 else 0
    pts_cap = 15 * min(1.0, max(0.0, (vr - 1.0) / 1.5)) + 15 * min(1.0, dev / 0.06)
    return round(pts_rsi + pts_wick + pts_cap), direction

# ----------------------------------------------------------- entrée confirmée

def detect_entry(closed, rsi, atr):
    """Une entrée confirmée SUR LA DERNIÈRE bougie close, sinon None."""
    j = len(closed) - 1
    if j < MA_LEN + RSI_LEN + CONFIRM_BARS + 2:
        return None
    for k in range(1, CONFIRM_BARS + 1):
        i = j - k
        sc, direction = score_reversal(closed, i, rsi)
        if sc < SCORE_MIN or direction is None:
            continue
        seuil = closed[i]["h"] if direction == "long" else closed[i]["l"]
        confirmed_before = any(
            (closed[m]["c"] > seuil if direction == "long" else closed[m]["c"] < seuil)
            for m in range(i + 1, j)
        )
        if confirmed_before:
            continue
        now_ok = closed[j]["c"] > seuil if direction == "long" else closed[j]["c"] < seuil
        if not now_ok:
            continue
        pivot = (min(c["l"] for c in closed[i:j + 1]) if direction == "long"
                 else max(c["h"] for c in closed[i:j + 1]))
        a0 = atr[j]
        if not a0 or a0 <= 0:
            return None
        entry = closed[j]["c"]
        stop = pivot - ATR_STOP_MULT * a0 if direction == "long" else pivot + ATR_STOP_MULT * a0
        if (direction == "long" and stop >= entry) or (direction == "short" and stop <= entry):
            return None
        return {
            "id": f"{closed[j]['t']}", "dir": direction, "score": sc,
            "entry": entry, "entry_ts": closed[j]["t"],
            "stop_init": stop, "risk": abs(entry - stop),
        }
    return None

# ----------------------------------------------------------- suivi de position

def track(sig, closed, rsi, atr):
    """Recalcule la position depuis l'entrée. Renvoie ('open', sig_maj) ou
    ('closed', evenement)."""
    entry_idx = next((k for k, c in enumerate(closed) if c["t"] == sig["entry_ts"]), None)
    if entry_idx is None:
        last = closed[-1]["c"]
        r = (last - sig["entry"]) / sig["risk"]
        if sig["dir"] == "short":
            r = -r
        return "closed", {**sig, "motif": "hors historique", "r": round(r, 2),
                          "exit": last, "exit_ts": closed[-1]["t"]}
    entry, risk, d = sig["entry"], sig["risk"], sig["dir"]
    stop, extreme, be = sig["stop_init"], entry, False
    for j in range(entry_idx + 1, len(closed)):
        c = closed[j]
        if d == "long" and c["l"] <= stop:
            return "closed", {**sig, "motif": "stop suiveur", "exit": stop,
                              "r": round((stop - entry) / risk, 2), "exit_ts": c["t"]}
        if d == "short" and c["h"] >= stop:
            return "closed", {**sig, "motif": "stop suiveur", "exit": stop,
                              "r": round((entry - stop) / risk, 2), "exit_ts": c["t"]}
        extreme = max(extreme, c["h"]) if d == "long" else min(extreme, c["l"])
        if not be:
            g = ((extreme - entry) if d == "long" else (entry - extreme)) / risk
            if g >= 1.0:
                stop = max(stop, entry) if d == "long" else min(stop, entry)
                be = True
        aj = atr[j] or atr[entry_idx]
        stop = (max(stop, extreme - TRAIL_MULT * aj) if d == "long"
                else min(stop, extreme + TRAIL_MULT * aj))
        sc, sd = score_reversal(closed, j, rsi)
        if sc >= SCORE_MIN and sd is not None and sd != d:
            out = c["c"]
            r = (out - entry) / risk if d == "long" else (entry - out) / risk
            return "closed", {**sig, "motif": "signal inverse", "exit": out,
                              "r": round(r, 2), "exit_ts": c["t"]}
        if j - entry_idx >= MAX_HOLD:
            out = c["c"]
            r = (out - entry) / risk if d == "long" else (entry - out) / risk
            return "closed", {**sig, "motif": "duree max", "exit": out,
                              "r": round(r, 2), "exit_ts": c["t"]}
    last = closed[-1]["c"]
    r_lat = (last - entry) / risk if d == "long" else (entry - last) / risk
    return "open", {**sig, "stop": round(stop, 10), "extreme": round(extreme, 10),
                    "breakeven": be, "last_price": last, "r_latent": round(r_lat, 2),
                    "maj_ts": closed[-1]["t"]}

# ----------------------------------------------------------- infrastructure

def _get(url, retries=2, timeout=25):
    for a in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if a == retries:
                return {"error": [str(e)]}
            time.sleep(2)


def build_universe(state):
    """Univers = portefeuille + paires USD les plus liquides.
    Deux requêtes seulement (AssetPairs + Ticker global), recalculé une
    fois par jour et mémorisé dans l'état."""
    cached = state.get("universe")
    ts = state.get("universe_ts", 0)
    if cached and time.time() - ts < UNIVERSE_CACHE_H * 3600:
        print(f"  Univers en cache : {len(cached)} paires.")
        return cached

    d = _get(API_PAIRS)
    if d.get("error"):
        print(f"  [!] AssetPairs : {d['error']}")
        return cached or list(PORTFOLIO)
    usd = []
    for _, info in d.get("result", {}).items():
        alt = info.get("altname", "")
        base = info.get("base", "").lstrip("X").lstrip("Z")
        quote = info.get("quote", "").lstrip("X").lstrip("Z")
        if quote != "USD" or base in EXCLUS or base.endswith("x"):
            continue
        if any(t in alt for t in (".", "_")):
            continue
        usd.append(alt)
    usd = sorted(set(usd))

    t = _get(API_TICKER)
    vols = {}
    if not t.get("error"):
        for k, v in t.get("result", {}).items():
            try:
                # v["v"][1] = volume 24h, v["c"][0] = dernier prix
                vols[k] = float(v["v"][1]) * float(v["c"][0])
            except (KeyError, ValueError, IndexError):
                continue

    def dv(p):
        if p in vols:
            return vols[p]
        for k in vols:
            if k.replace("X", "").replace("Z", "") == p:
                return vols[k]
        return 0.0

    liquides = [(p, dv(p)) for p in usd if p not in PORTFOLIO]
    liquides = [(p, v) for p, v in liquides if v >= MIN_DOLLAR_VOL]
    liquides.sort(key=lambda x: -x[1])

    univers = list(PORTFOLIO)
    for p, _ in liquides:
        if len(univers) >= MAX_PAIRS:
            break
        univers.append(p)

    state["universe"] = univers
    state["universe_ts"] = time.time()
    print(f"  Univers reconstruit : {len(univers)} paires "
          f"({len(PORTFOLIO)} du portefeuille + {len(univers) - len(PORTFOLIO)} "
          f"parmi {len(liquides)} liquides sur {len(usd)} paires USD).")
    return univers


def fetch_ohlc(pair, retries=2):
    url = f"{API}?pair={pair}&interval={INTERVAL_MIN}"
    for a in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                d = json.loads(r.read().decode())
            if d.get("error"):
                print(f"  [!] {pair}: {d['error']}"); return None
            res = d["result"]
            k = next(x for x in res if x != "last")
            return [{"t": int(c[0]), "o": float(c[1]), "h": float(c[2]),
                     "l": float(c[3]), "c": float(c[4]), "v": float(c[6])}
                    for c in res[k]]
        except Exception as e:
            if a == retries:
                print(f"  [!] {pair}: échec réseau ({e})"); return None
            time.sleep(2)


def in_macro_window(now):
    for s in MACRO_EVENTS_UTC:
        ev = datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        if abs((now - ev).total_seconds()) <= MACRO_WINDOW_H * 3600:
            return s
    return None


def load_state():
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
        st.setdefault("last_by_pair", {})
        st.setdefault("universe", None)
        st.setdefault("universe_ts", 0)
        st.setdefault("active", [])
        st.setdefault("history", [])
        return st
    except Exception:
        return {"last_by_pair": {}, "active": [], "history": [],
                "universe": None, "universe_ts": 0}


def save_json(path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=1)
    except Exception as e:
        print(f"  [!] écriture {path} : {e}")


def send_email(subject, body):
    host, user = os.environ.get("SMTP_HOST"), os.environ.get("SMTP_USER")
    pwd, to = os.environ.get("SMTP_PASS"), os.environ.get("ALERT_TO")
    port = int(os.environ.get("SMTP_PORT", "465"))
    if not all([host, user, pwd, to]):
        print("  [i] SMTP non configuré — sortie console uniquement.")
        return False
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, user, to
    msg.set_content(body)
    try:
        with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(),
                              timeout=20) as s:
            s.login(user, pwd); s.send_message(msg)
        return True
    except Exception as e:
        print(f"  [!] envoi email échoué : {e}"); return False


def fp(p):
    d = 8 if p < 0.001 else 6 if p < 0.01 else 4 if p < 1 else 2
    return f"{p:.{d}f}"

# ----------------------------------------------------------- scan principal

def main():
    now = datetime.now(timezone.utc)
    print(f"[{now:%Y-%m-%d %H:%M} UTC] scan (v4 univers elargi)")
    ev = in_macro_window(now)
    if ev:
        print(f"  Fenêtre macro ({ev} UTC ±{MACRO_WINDOW_H}h) — scan suspendu "
              f"(entrées ET suivi).")
        return

    state = load_state()
    pairs = build_universe(state)
    entries, exits, still_open = [], [], []
    active_by_pair = {s["pair"]: s for s in state["active"]}
    # une position ouverte est toujours suivie, même si sa paire sort de l'univers
    for p in active_by_pair:
        if p not in pairs:
            pairs = pairs + [p]

    for pair in pairs:
        candles = fetch_ohlc(pair)
        time.sleep(1.1)
        if not candles or len(candles) < 100:
            if pair in active_by_pair:
                still_open.append(active_by_pair[pair])
            continue
        closed = candles[:-1]
        rsi = rsi_series([c["c"] for c in closed])
        atr = atr_series(closed)

        if pair in active_by_pair:
            status, res = track(active_by_pair[pair], closed, rsi, atr)
            if status == "closed":
                exits.append(res)
                state["last_by_pair"][pair] = time.time()
            else:
                still_open.append(res)
            continue

        last_ts = state["last_by_pair"].get(pair, 0)
        if time.time() - last_ts < COOLDOWN_HOURS * 3600:
            continue
        sig = detect_entry(closed, rsi, atr)
        if sig:
            sig["pair"] = pair
            status, res = track(sig, closed, rsi, atr)
            if status == "open":
                entries.append(res)
                still_open.append(res)
                state["last_by_pair"][pair] = time.time()

    state["active"] = still_open
    state["history"] = (state["history"] + exits)[-HISTORY_KEEP:]
    save_json(STATE_FILE, state)

    board = {"generated": now.strftime("%Y-%m-%d %H:%M UTC"),
             "active": still_open, "history": state["history"]}
    save_json(SIGNALS_FILE, board)

    if entries or exits:
        lines = [f"Scan du {now:%d/%m/%Y %H:%M} UTC", "=" * 56, ""]
        if entries:
            lines.append(f"ENTRÉES CONFIRMÉES ({len(entries)})")
            for s in entries:
                sd = (s["entry"] - s["stop_init"]) / s["entry"] * 100
                if s["dir"] == "short":
                    sd = -sd
                lines += [
                    f"  {s['dir'].upper():5} {s['pair']}  score {s['score']}",
                    f"    Entrée {fp(s['entry'])} | Invalidation {fp(s['stop_init'])}"
                    f" ({abs(sd):.2f} %)", ""]
        if exits:
            lines.append(f"ALERTES DE SORTIE ({len(exits)})")
            for s in exits:
                lines += [
                    f"  {s['pair']} ({s['dir']}) — {s['motif']}",
                    f"    Entrée {fp(s['entry'])} -> Sortie {fp(s['exit'])}"
                    f"  |  Résultat : {s['r']:+.2f}R", ""]
        if still_open:
            lines.append(f"POSITIONS SUIVIES ({len(still_open)})")
            for s in still_open:
                lines.append(f"  {s['pair']} {s['dir']} | stop suiveur {fp(s['stop'])}"
                             f" | latent {s['r_latent']:+.2f}R"
                             f"{' | point mort acquis' if s.get('breakeven') else ''}")
            lines.append("")
        lines += [
            "-" * 56,
            "MODE OBSERVATION : stratégie non validée par backtest long.",
            "Chaque signal se note dans le journal, rien ne s'engage.",
            "",
            "=== DONNÉES TABLEAU DE BORD (copier tout le bloc, accolades",
            "=== comprises, et le coller dans l'application) ===",
            json.dumps(board, separators=(",", ":")),
        ]
        n_e, n_x = len(entries), len(exits)
        subject = f"[Signaux] {n_e} entrée(s), {n_x} sortie(s), {len(still_open)} suivie(s)"
        body = "\n".join(lines)
        print("\n" + body + "\n")
        send_email(subject, body)
    else:
        msg = f"  Aucun événement. Positions suivies : {len(still_open)}."
        print(msg)

    print("  Scan terminé.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
