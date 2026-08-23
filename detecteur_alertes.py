#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Détecteur v8 — trois modes : journalier, 30 min, 5 min, univers élargi + suivi de position + alertes de sortie
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

# --- deux modes simultanés, mesurés (test_horizons.py, 6 horizons, ~1000
# --- signaux chacun, facteurs de profit BRUT et NET de frais) :
#       5 min  brut 1,51  net 0,47      1 h   brut 0,82  net 0,44
#      15 min  brut 1,46  net 0,52      4 h   brut 0,77  net 0,57
#      30 min  brut 1,34  net 0,56      1 j   brut 1,13  net 1,02
#
# JOURNALIER  seul horizon dont l'espérance nette est positive (+0,006R).
#             Stop moyen 20,8 % : incompatible avec un fort levier.
# 30 MINUTES  meilleur compromis parmi les horizons courts. Signal brut
#             solide (1,34) mais frais à 0,403R : NET PERDANT (0,56).
#             Conservé pour l'observation des retournements rapides,
#             pas parce qu'il serait rentable.
#
# L'horizon 1 h, utilisé jusqu'ici, était le pire des six sur les deux
# critères. Il est abandonné.
MODES = [
    # cle          intervalle  scan tous les   univers  cooldown  email
    {"nom": "journalier", "cle": "1j", "interval": 1440,
     "scan_every_min": 60, "max_pairs": 150, "cooldown_h": 48, "email": True},
    {"nom": "30 minutes", "cle": "30m", "interval": 30,
     "scan_every_min": 30, "max_pairs": 150, "cooldown_h": 6, "email": True},
    # Le 5 minutes n'envoie PAS d'email : le test a mesuré ~4,6 signaux par
    # paire et par jour, soit près de 200 par jour sur 40 paires. Il alimente
    # le tableau, qu'on consulte à la demande. Univers réduit aux 40 paires
    # les plus liquides pour tenir dans une fenêtre de scan de 10 minutes.
    {"nom": "5 minutes", "cle": "5m", "interval": 5,
     "scan_every_min": 10, "max_pairs": 40, "cooldown_h": 2, "email": False},
]

SCORE_MIN = 40             # seuil du score de retournement
RSI_LEN, MA_LEN, ATR_LEN = 14, 20, 14
ATR_STOP_MULT = 1.5        # marge sous le pivot pour le stop initial
TRAIL_MULT = 2.0           # mesuré : facteur 1,08 contre 0,90 à 3 et 4 ATR
                           # (test_sortie.py, 1006 signaux, tenu sur les deux
                           # moitiés de période : 1,01 et 1,14)
MAX_HOLD = 60              # bougies : sortie forcée (60 j ou 30 h selon le mode)
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
API_FUTURES = "https://futures.kraken.com/derivatives/api/v3/instruments"

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
    """Entrée SANS confirmation, sur la dernière bougie close.

    CHANGEMENT MESURÉ (test_confirmation.py, 510 excès, 76 paires, 2 ans) :
      A. confirmation au plus haut/bas : 36,1 % des excès, entrée 9,29 %
         plus loin, avantage +0,48 vs hasard
      D. aucune confirmation           : 100 % des excès, entrée immédiate,
         avantage +1,59
    La confirmation éliminait 64 % des signaux et divisait l'avantage par
    trois. Elle est supprimée. Réserve honnête : le ratio potentiel/douleur
    de D (0,93) est un peu moins bon que celui de A (1,02).
    """
    j = len(closed) - 1
    if j < MA_LEN + RSI_LEN + 2:
        return None
    sc, direction = score_reversal(closed, j, rsi)
    if sc < SCORE_MIN or direction is None:
        return None
    a0 = atr[j]
    if not a0 or a0 <= 0:
        return None
    entry = closed[j]["c"]
    pivot = closed[j]["l"] if direction == "long" else closed[j]["h"]
    stop = pivot - ATR_STOP_MULT * a0 if direction == "long" else pivot + ATR_STOP_MULT * a0
    if (direction == "long" and stop >= entry) or (direction == "short" and stop <= entry):
        return None
    return {
        "id": f"{closed[j]['t']}", "dir": direction, "score": sc,
        "entry": entry, "entry_ts": closed[j]["t"],
        "stop_init": stop, "risk": abs(entry - stop),
    }


def detect_watch(closed, rsi):
    """RSI qui APPROCHE d'une zone d'excès, sans y être encore.

    La confirmation ayant été supprimée, un excès devient immédiatement une
    entrée : il n'y a plus rien à « attendre ». Cette section montre donc
    désormais ce qui se prépare — les paires dont le RSI entre en zone
    d'alerte (30-38 ou 62-70) mais n'a pas atteint le seuil d'excès.
    Aucun critère n'est assoupli : ce ne sont PAS des signaux.
    """
    j = len(closed) - 1
    if j < MA_LEN + RSI_LEN + 2:
        return None
    r = rsi[j]
    if r is None:
        return None
    if 30 < r <= 38:
        direction, reste = "long", round(r - 30, 1)
    elif 62 <= r < 70:
        direction, reste = "short", round(70 - r, 1)
    else:
        return None
    return {"dir": direction, "rsi": round(r, 1), "reste": reste,
            "last": closed[j]["c"], "ts": closed[j]["t"]}


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


def perp_bases(state):
    """Actifs disposant d'un perpétuel sur Kraken Futures. Une requête par
    jour, mémorisée. Renvoie None si l'information est indisponible —
    dans ce cas le détecteur n'affirme rien plutôt que d'induire en erreur."""
    cached = state.get("perps")
    ts = state.get("perps_ts", 0)
    if cached is not None and time.time() - ts < UNIVERSE_CACHE_H * 3600:
        return set(cached)
    d = _get(API_FUTURES)
    if d.get("error") or not d.get("instruments"):
        print("  [!] Liste des perpétuels indisponible.")
        return set(cached) if cached is not None else None
    bases = set()
    for ins in d["instruments"]:
        sym = (ins.get("symbol") or "").upper()
        if not sym.startswith("PF_") or not sym.endswith("USD"):
            continue
        if not ins.get("tradeable", True):
            continue
        b = sym[3:-3]
        bases.add(b)
        if b == "XBT":
            bases.add("BTC")
    state["perps"] = sorted(bases)
    state["perps_ts"] = time.time()
    print(f"  Perpétuels disponibles : {len(bases)} actifs.")
    return bases


def has_perp(pair, bases):
    """True / False / None (inconnu)."""
    if bases is None:
        return None
    return pair[:-3] in bases


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


def fetch_ohlc(pair, interval, retries=2):
    url = f"{API}?pair={pair}&interval={interval}"
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
        if st["last_by_pair"] and not all(
                isinstance(v, dict) for v in st["last_by_pair"].values()):
            st["last_by_pair"] = {}      # ancien format : on repart propre
        st.setdefault("universe", None)
        st.setdefault("universe_ts", 0)
        st.setdefault("perps", None)
        st.setdefault("perps_ts", 0)
        st.setdefault("last_scan", {})
        st.setdefault("active", [])
        st.setdefault("history", [])
        return st
    except Exception:
        return {"last_by_pair": {}, "active": [], "history": [],
                "universe": None, "universe_ts": 0,
                "perps": None, "perps_ts": 0, "last_scan": {}}


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
    print(f"[{now:%Y-%m-%d %H:%M} UTC] scan (v8 multi-mode : "
          + " + ".join(m["nom"] for m in MODES) + ")")
    ev = in_macro_window(now)
    if ev:
        print(f"  Fenêtre macro ({ev} UTC ±{MACRO_WINDOW_H}h) — scan suspendu.")
        return

    state = load_state()
    pairs = build_universe(state)
    bases = perp_bases(state)

    entries, exits, still_open, watch = [], [], [], []
    # positions ouvertes indexées par (paire, mode)
    active_by_key = {(s["pair"], s.get("mode", "1j")): s for s in state["active"]}
    for (p, _m) in active_by_key:
        if p not in pairs:
            pairs = pairs + [p]

    # Quels modes sont dus ? Chacun a sa propre cadence : inutile de
    # retélécharger des bougies journalières toutes les 10 minutes.
    dus = []
    for m in MODES:
        ecoule = (time.time() - state["last_scan"].get(m["cle"], 0)) / 60
        if ecoule >= m["scan_every_min"] - 1:
            dus.append(m)
    if not dus:
        print("  Aucun mode dû à cette exécution.")
        return
    print("  Modes scannés : " + ", ".join(m["nom"] for m in dus))

    for pair in pairs:
        for mode in dus:
            cle, interval = mode["cle"], mode["interval"]
            if pairs.index(pair) >= mode["max_pairs"]:
                continue
            candles = fetch_ohlc(pair, interval)
            time.sleep(1.1)
            if not candles or len(candles) < 100:
                k = (pair, cle)
                if k in active_by_key:
                    still_open.append(active_by_key[k])
                continue
            closed = candles[:-1]
            rsi = rsi_series([c["c"] for c in closed])
            atr = atr_series(closed)

            k = (pair, cle)
            if k in active_by_key:
                status, res = track(active_by_key[k], closed, rsi, atr)
                res["mode"] = cle
                res["mode_nom"] = mode["nom"]
                if status == "closed":
                    exits.append(res)
                    state["last_by_pair"].setdefault(cle, {})[pair] = time.time()
                else:
                    still_open.append(res)
                continue

            w = detect_watch(closed, rsi)
            if w:
                w.update({"pair": pair, "mode": cle, "mode_nom": mode["nom"],
                          "perp": has_perp(pair, bases)})
                watch.append(w)

            last_ts = state["last_by_pair"].get(cle, {}).get(pair, 0)
            if time.time() - last_ts < mode["cooldown_h"] * 3600:
                continue
            sig = detect_entry(closed, rsi, atr)
            if sig:
                sig.update({"pair": pair, "mode": cle, "mode_nom": mode["nom"],
                            "perp": has_perp(pair, bases)})
                status, res = track(sig, closed, rsi, atr)
                if status == "open":
                    res["mode"] = cle
                    res["mode_nom"] = mode["nom"]
                    entries.append(res)
                    still_open.append(res)
                    state["last_by_pair"].setdefault(cle, {})[pair] = time.time()

    for m in dus:
        state["last_scan"][m["cle"]] = time.time()
    state["active"] = still_open
    state["history"] = (state["history"] + exits)[-HISTORY_KEEP:]
    save_json(STATE_FILE, state)

    watch.sort(key=lambda x: (x["mode"], x["reste"]))
    board = {"generated": now.strftime("%Y-%m-%d %H:%M UTC"),
             "modes": [{"cle": m["cle"], "nom": m["nom"]} for m in MODES],
             "active": still_open, "history": state["history"],
             "watch": watch[:40]}
    save_json(SIGNALS_FILE, board)

    def par_mode(liste):
        d = {}
        for x in liste:
            d.setdefault(x.get("mode", "1j"), []).append(x)
        return d

    modes_mail = {m["cle"] for m in MODES if m["email"]}
    e_mail = [x for x in entries if x.get("mode") in modes_mail]
    x_mail = [x for x in exits if x.get("mode") in modes_mail]

    if e_mail or x_mail:
        entries, exits = e_mail, x_mail
        lines = [f"Scan du {now:%d/%m/%Y %H:%M} UTC", "=" * 60, ""]
        for mode in MODES:
            cle, nom = mode["cle"], mode["nom"]
            e_m = [x for x in entries if x.get("mode") == cle]
            x_m = [x for x in exits if x.get("mode") == cle]
            if not e_m and not x_m:
                continue
            lines += [f"### MODE {nom.upper()}", ""]
            if e_m:
                lines.append(f"  ENTRÉES ({len(e_m)})")
                for s in e_m:
                    sd = abs(s["entry"] - s["stop_init"]) / s["entry"] * 100
                    lines += [
                        f"    {s['dir'].upper():5} {s['pair']}  score {s['score']}",
                        f"      Entrée {fp(s['entry'])} | Invalidation "
                        f"{fp(s['stop_init'])} ({sd:.2f} %)",
                        f"      Perpétuel : " + ("oui" if s.get("perp")
                                                 else "NON — spot uniquement"
                                                 if s.get("perp") is False else "inconnu"),
                        ""]
            if x_m:
                lines.append(f"  SORTIES ({len(x_m)})")
                for s in x_m:
                    lines += [f"    {s['pair']} ({s['dir']}) — {s['motif']}",
                              f"      {fp(s['entry'])} -> {fp(s['exit'])}  |  "
                              f"{s['r']:+.2f}R", ""]
        po = par_mode(still_open)
        if still_open:
            lines.append(f"POSITIONS SUIVIES ({len(still_open)})")
            for mode in MODES:
                for s in po.get(mode["cle"], []):
                    lines.append(f"  [{mode['nom']}] {s['pair']} {s['dir']} | "
                                 f"stop {fp(s['stop'])} | latent {s['r_latent']:+.2f}R")
            lines.append("")
        lines += [
            "-" * 60,
            "MODE OBSERVATION.",
            "Journalier : seul horizon dont l'espérance nette est positive",
            "  (facteur 1,02 net de frais). Stop moyen 20,8 % : levier faible.",
            "30 minutes : meilleur signal brut (1,34) mais NET PERDANT (0,56),",
            "  les frais y pèsent 0,403R. Observation uniquement.",
            "5 minutes  : signal brut le plus fort (1,51) mais frais à 0,634R,",
            "  net 0,47. Visible dans le tableau, jamais par email (trop nombreux).",
            "",
            "=== DONNÉES TABLEAU DE BORD (copier tout le bloc) ===",
            json.dumps(board, separators=(",", ":")),
        ]
        n_e, n_x = len(entries), len(exits)
        det = "  ".join(f"{m['cle']}:{len([x for x in entries if x.get('mode')==m['cle']])}"
                        for m in MODES)
        subject = f"[Signaux] {n_e} entrée(s) ({det}), {n_x} sortie(s)"
        body = "\n".join(lines)
        print("\n" + body + "\n")
        send_email(subject, body)
    else:
        pw = par_mode(watch)
        print(f"  Aucun événement. Suivies : {len(still_open)}, "
              f"en approche : "
              + ", ".join(f"{m['nom']} {len(pw.get(m['cle'], []))}" for m in MODES) + ".")

    if watch:
        pw = par_mode(watch)
        for mode in MODES:
            lst = pw.get(mode["cle"], [])
            if not lst:
                continue
            print(f"\n  APPROCHE — {mode['nom']} ({len(lst)}) :")
            for w in lst[:8]:
                print(f"    {w['pair']:<12} {w['dir']:<5} RSI {w['rsi']:>5}  "
                      f"encore {w['reste']:>4} pts"
                      + ("" if w.get("perp") else "  [pas de perp]"
                         if w.get("perp") is False else ""))
        print("  (l'approche ne déclenche pas d'email)")

    print("  Scan terminé.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
