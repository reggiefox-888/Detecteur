#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Détecteur v9 — observation pure : excès constatés et mesurés, univers élargi + suivi de position + alertes de sortie
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

def detect_exces(closed, rsi):
    """Excès de surachat/survente sur la dernière bougie close.

    AUCUN stop, aucun R, aucune position. Le détecteur constate un état du
    marché et note le prix ; c'est le suivi qui mesurera ensuite ce que le
    prix a fait, en pourcentage.

    Ce que la recherche a établi sur ces excès (1,1 M de bougies, 27 600
    signaux, Binance 4 ans) : le potentiel favorable y est identique au
    hasard (EFM +12,9 % contre +11,5 %) mais l'excursion adverse est 60 %
    plus grande (-19,6 % contre -12,3 %). Ces excès marquent une asymétrie
    de risque DÉFAVORABLE. L'outil les mesure, il ne les recommande pas.
    """
    j = len(closed) - 1
    if j < MA_LEN + RSI_LEN + 2:
        return None
    sc, direction = score_reversal(closed, j, rsi)
    if sc < SCORE_MIN or direction is None:
        return None
    c = closed[j]
    rng = c["h"] - c["l"]
    bh, bl = max(c["o"], c["c"]), min(c["o"], c["c"])
    meche = ((bl - c["l"]) / rng if direction == "long" else (c["h"] - bh) / rng) if rng > 0 else 0
    vols = [closed[k]["v"] for k in range(j - MA_LEN, j)]
    va = sum(vols) / len(vols) if vols else 0
    return {
        "id": f"{c['t']}", "sens": direction, "score": sc,
        "rsi": round(rsi[j], 1), "meche": round(meche * 100),
        "volume_x": round(c["v"] / va, 2) if va > 0 else None,
        "prix": c["c"], "ts": c["t"],
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


# ----------------------------------------------------------- suivi

HORIZONS_SUIVI = [1, 3, 5, 10]     # en bougies du mode concerné


def suivi(obs, closed):
    """Mesure ce que le prix a fait depuis l'excès, en POURCENTAGE.
    Renvoie (etat, obs_maj) où etat vaut "encours" ou "complet"."""
    idx = next((k for k, c in enumerate(closed) if c["t"] == obs["ts"]), None)
    if idx is None:
        return "complet", {**obs, "hors_historique": True}
    sens = 1 if obs["sens"] == "long" else -1
    px = obs["prix"]
    dispo = len(closed) - 1 - idx

    fwd = dict(obs.get("fwd", {}))
    for n in HORIZONS_SUIVI:
        if str(n) in fwd or idx + n >= len(closed):
            continue
        fwd[str(n)] = round((closed[idx + n]["c"] - px) / px * 100 * sens, 2)

    fin = min(len(closed), idx + 1 + max(HORIZONS_SUIVI))
    seg = closed[idx + 1: fin]
    efm = eam = None
    if seg:
        if sens == 1:
            efm = round((max(c["h"] for c in seg) - px) / px * 100, 2)
            eam = round((min(c["l"] for c in seg) - px) / px * 100, 2)
        else:
            efm = round((px - min(c["l"] for c in seg)) / px * 100, 2)
            eam = round((px - max(c["h"] for c in seg)) / px * 100, 2)

    maj = {**obs, "fwd": fwd, "efm": efm, "eam": eam,
           "bougies_ecoulees": dispo, "prix_actuel": closed[-1]["c"],
           "variation": round((closed[-1]["c"] - px) / px * 100 * sens, 2)}
    return ("complet" if dispo >= max(HORIZONS_SUIVI) else "encours"), maj


# ----------------------------------------------------------- infrastructure
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
    print(f"[{now:%Y-%m-%d %H:%M} UTC] scan (v9 observation pure : "
          + " + ".join(m["nom"] for m in MODES) + ")")
    ev = in_macro_window(now)
    if ev:
        print(f"  Fenêtre macro ({ev} UTC ±{MACRO_WINDOW_H}h) — scan suspendu.")
        return

    state = load_state()
    pairs = build_universe(state)
    bases = perp_bases(state)

    dus = []
    for m in MODES:
        ecoule = (time.time() - state["last_scan"].get(m["cle"], 0)) / 60
        if ecoule >= m["scan_every_min"] - 1:
            dus.append(m)
    if not dus:
        print("  Aucun mode dû à cette exécution.")
        return
    print("  Modes scannés : " + ", ".join(m["nom"] for m in dus))

    nouveaux, termines, encours, approche = [], [], [], []
    suivies = {(o["pair"], o.get("mode", "1j")): o for o in state["active"]}
    for (p, _m) in suivies:
        if p not in pairs:
            pairs = pairs + [p]

    for pair in pairs:
        for mode in dus:
            cle, interval = mode["cle"], mode["interval"]
            if pairs.index(pair) >= mode["max_pairs"]:
                continue
            candles = fetch_ohlc(pair, interval)
            time.sleep(1.1)
            k = (pair, cle)
            if not candles or len(candles) < 100:
                if k in suivies:
                    encours.append(suivies[k])
                continue
            closed = candles[:-1]
            rsi = rsi_series([c["c"] for c in closed])

            if k in suivies:
                etat, maj = suivi(suivies[k], closed)
                maj["mode"], maj["mode_nom"] = cle, mode["nom"]
                (termines if etat == "complet" else encours).append(maj)
                continue

            w = detect_watch(closed, rsi)
            if w:
                w.update({"pair": pair, "mode": cle, "mode_nom": mode["nom"],
                          "perp": has_perp(pair, bases)})
                approche.append(w)

            last_ts = state["last_by_pair"].get(cle, {}).get(pair, 0)
            if time.time() - last_ts < mode["cooldown_h"] * 3600:
                continue
            e = detect_exces(closed, rsi)
            if e:
                e.update({"pair": pair, "mode": cle, "mode_nom": mode["nom"],
                          "perp": has_perp(pair, bases)})
                etat, maj = suivi(e, closed)
                nouveaux.append(maj)
                (termines if etat == "complet" else encours).append(maj)
                state["last_by_pair"].setdefault(cle, {})[pair] = time.time()

    for m in dus:
        state["last_scan"][m["cle"]] = time.time()
    state["active"] = encours
    state["history"] = (state["history"] + termines)[-200:]
    save_json(STATE_FILE, state)

    approche.sort(key=lambda x: (x["mode"], x["reste"]))
    board = {"generated": now.strftime("%Y-%m-%d %H:%M UTC"),
             "type": "observation",
             "horizons": HORIZONS_SUIVI,
             "modes": [{"cle": m["cle"], "nom": m["nom"]} for m in MODES],
             "active": encours, "history": state["history"],
             "watch": approche[:40]}
    save_json(SIGNALS_FILE, board)

    # ---- bilan cumulé, la seule chose qui compte vraiment
    hist = [o for o in state["history"] if o.get("efm") is not None]
    bilan = ""
    if len(hist) >= 5:
        n = len(hist)
        f3 = [o["fwd"].get("3") for o in hist if o.get("fwd", {}).get("3") is not None]
        efm = sum(o["efm"] for o in hist) / n
        eam = sum(o["eam"] for o in hist) / n
        bilan = (f"\n  BILAN CUMULÉ ({n} excès observés jusqu'au bout)\n"
                 f"    Variation moyenne à +3 bougies : "
                 f"{(sum(f3)/len(f3) if f3 else 0):+.2f} %\n"
                 f"    Excursion favorable moyenne    : {efm:+.2f} %\n"
                 f"    Excursion adverse moyenne      : {eam:+.2f} %\n"
                 f"    Rapport potentiel/risque       : "
                 f"{abs(efm/eam) if eam else 0:.2f}\n"
                 f"    (mesuré sur l'historique Binance : 0,66 — "
                 f"plus de risque que de potentiel)")
        print(bilan)

    modes_mail = {m["cle"] for m in MODES if m["email"]}
    n_mail = [x for x in nouveaux if x.get("mode") in modes_mail]
    t_mail = [x for x in termines if x.get("mode") in modes_mail]

    if n_mail or t_mail:
        lines = [f"Observation du {now:%d/%m/%Y %H:%M} UTC", "=" * 60, "",
                 "Ce message CONSTATE des états de marché. Il ne propose ni",
                 "entrée, ni stop, ni position — ces excès ont une asymétrie",
                 "de risque défavorable, mesurée sur 4 ans de données.", ""]
        for mode in MODES:
            cle, nom = mode["cle"], mode["nom"]
            nm = [x for x in n_mail if x.get("mode") == cle]
            tm = [x for x in t_mail if x.get("mode") == cle]
            if not nm and not tm:
                continue
            lines += [f"### {nom.upper()}", ""]
            if nm:
                lines.append(f"  EXCÈS CONSTATÉS ({len(nm)})")
                for o in nm:
                    lines += [
                        f"    {o['pair']}  {'SURVENTE' if o['sens']=='long' else 'SURACHAT'}"
                        f"  RSI {o['rsi']}  score {o['score']}",
                        f"      prix {fp(o['prix'])} | mèche {o['meche']} % du range"
                        f" | volume x{o.get('volume_x') or '?'}",
                        f"      perpétuel : " + ("oui" if o.get("perp") else "non"
                                                 if o.get("perp") is False else "inconnu"),
                        ""]
            if tm:
                lines.append(f"  OBSERVATIONS TERMINÉES ({len(tm)})")
                for o in tm:
                    f = o.get("fwd", {})
                    lines += [
                        f"    {o['pair']} ({'survente' if o['sens']=='long' else 'surachat'})",
                        f"      +1 {f.get('1', '?')} %  +3 {f.get('3', '?')} %  "
                        f"+5 {f.get('5', '?')} %  +10 {f.get('10', '?')} %",
                        f"      meilleur {o.get('efm', '?')} %  |  pire {o.get('eam', '?')} %",
                        ""]
        if encours:
            lines.append(f"EN COURS D'OBSERVATION ({len(encours)})")
            for o in encours[:15]:
                lines.append(f"  [{o.get('mode_nom','?')}] {o['pair']} "
                             f"{'survente' if o['sens']=='long' else 'surachat'} | "
                             f"{o.get('variation', 0):+.2f} % depuis l'excès "
                             f"({o.get('bougies_ecoulees', 0)} bougies)")
            lines.append("")
        lines += [bilan, "", "-" * 60,
                  "=== DONNÉES TABLEAU DE BORD (copier tout le bloc) ===",
                  json.dumps(board, separators=(",", ":"))]
        subject = (f"[Observation] {len(n_mail)} excès constaté(s), "
                   f"{len(t_mail)} observation(s) terminée(s)")
        body = "\n".join(lines)
        print("\n" + body + "\n")
        send_email(subject, body)
    else:
        print(f"  Aucun nouvel excès. En cours : {len(encours)}, "
              f"terminés au total : {len(state['history'])}.")

    if approche:
        pa = {}
        for x in approche:
            pa.setdefault(x["mode"], []).append(x)
        for mode in MODES:
            lst = pa.get(mode["cle"], [])
            if not lst:
                continue
            print(f"\n  APPROCHE — {mode['nom']} ({len(lst)}) :")
            for w in lst[:8]:
                print(f"    {w['pair']:<12} {w['dir']:<5} RSI {w['rsi']:>5}  "
                      f"encore {w['reste']:>4} pts")
    print("  Scan terminé.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
