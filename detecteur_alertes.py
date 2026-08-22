#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Détecteur d'opportunités — cassures de structure 1h, portefeuille Kraken
=========================================================================

CE QUE FAIT CE SCRIPT
  À chaque exécution (prévue : une fois par heure via cron), il :
    1. Télécharge les bougies 1h des 30 paires du portefeuille
       (API publique Kraken, sans clé — lecture de prix uniquement)
    2. Détecte les points pivots (sommets/creux de structure)
    3. Cherche une cassure de structure confirmée par le volume :
         - contexte baissier (sommets et creux descendants) + clôture au-dessus
           du dernier sommet + volume > 1,5× la moyenne  -> candidat HAUSSE
         - miroir exact pour un candidat BAISSE
    4. Calcule le niveau d'invalidation (dernier creux/sommet) et sa distance
    5. Filtre : distance de stop entre 0,8 % et 5 % (en dessous, les frais
       mangent le risque ; au-dessus, ce n'est plus du court terme)
    6. Envoie UN email récapitulatif par scan, listant TOUS les candidats
       détectés, classés par force de la cassure (ratio de volume)
    7. Jamais d'alerte dans une fenêtre de ±2h autour d'un CPI ou d'un FOMC
    8. Mémorise son état (pas de doublon par paire pendant 24h)

  La détection est exhaustive. La limite de 3 trades par jour est une règle
  d'EXÉCUTION : elle vit dans le Poste de sélection, pas ici.

CE QUE CE SCRIPT NE FAIT PAS
  Il ne passe aucun ordre. Il ne garantit rien : la règle n'est PAS backtestée.
  Un candidat = une configuration à vérifier sur le graphique, pas un ordre.

INSTALLATION
  Aucune dépendance externe (bibliothèque standard uniquement).
  Variables d'environnement pour l'email (exemple Gmail, mot de passe
  d'application requis : myaccount.google.com > Sécurité > Mots de passe des applications) :
    export SMTP_HOST="smtp.gmail.com"
    export SMTP_PORT="465"
    export SMTP_USER="toi@gmail.com"
    export SMTP_PASS="mot-de-passe-application"
    export ALERT_TO="toi@gmail.com"
  Sans ces variables, les candidats s'affichent en console (mode test).

PLANIFICATION (Linux/Mac, toutes les heures à h+02) :
  crontab -e
  2 * * * * /usr/bin/python3 /chemin/vers/detecteur_alertes.py >> /chemin/vers/detecteur.log 2>&1
  (Windows : Planificateur de tâches, déclencheur horaire.)
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

# ----------------------------------------------------------------------------
# CONFIGURATION — tout se règle ici
# ----------------------------------------------------------------------------

PAIRS = [
    "SOLUSD", "AVAXUSD", "SUIUSD", "ADAUSD", "XRPUSD", "LINKUSD", "ETHUSD",
    "DOGEUSD", "DOTUSD", "APTUSD", "NEARUSD", "TIAUSD", "INJUSD", "TAOUSD",
    "FETUSD", "ARBUSD", "RENDERUSD", "JUPUSD", "ENAUSD", "ONDOUSD",
    "STXUSD", "POLUSD", "AAVEUSD", "TONUSD", "XLMUSD", "FILUSD",
    "HBARUSD", "GRTUSD", "PEPEUSD", "SHIBUSD",
]

INTERVAL_MIN = 60          # bougies 1h
SWING_K = 2                # un pivot = extrême sur 2 bougies de chaque côté
VOL_MULT = 1.5             # volume de cassure requis vs moyenne 20 bougies
STOP_MIN_PCT = 0.8         # distance de stop minimale (sinon frais > ~60% du risque)
STOP_MAX_PCT = 5.0         # distance maximale (au-delà : pas un setup court terme)
COOLDOWN_HOURS = 24        # pas deux signaux sur la même paire en 24h
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts_state.json")

# Fenêtres macro (UTC) : aucune alerte de ±2h autour de ces instants.
# CPI = 12:30 UTC en heure d'été US, 13:30 UTC en heure d'hiver. FOMC = décision.
MACRO_EVENTS_UTC = [
    "2026-09-11 12:30",  # CPI août
    "2026-09-16 18:00",  # FOMC + dot plot
    "2026-10-14 12:30",  # CPI septembre
    "2026-10-28 18:00",  # FOMC
    "2026-11-10 13:30",  # CPI octobre (heure d'hiver US)
    "2026-12-09 19:00",  # FOMC + dot plot
    "2026-12-10 13:30",  # CPI novembre
]
MACRO_WINDOW_H = 2

API = "https://api.kraken.com/0/public/OHLC"

# ----------------------------------------------------------------------------
# Données
# ----------------------------------------------------------------------------

def fetch_ohlc(pair, interval=INTERVAL_MIN, retries=2):
    """Bougies Kraken : [time, open, high, low, close, vwap, volume, count]."""
    url = f"{API}?pair={pair}&interval={interval}"
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                data = json.loads(r.read().decode())
            if data.get("error"):
                print(f"  [!] {pair}: {data['error']}")
                return None
            result = data["result"]
            key = next(k for k in result if k != "last")
            rows = result[key]
            return [
                {"t": int(c[0]), "o": float(c[1]), "h": float(c[2]),
                 "l": float(c[3]), "c": float(c[4]), "v": float(c[6])}
                for c in rows
            ]
        except Exception as e:
            if attempt == retries:
                print(f"  [!] {pair}: échec réseau ({e})")
                return None
            time.sleep(2)


# ----------------------------------------------------------------------------
# Structure de marché
# ----------------------------------------------------------------------------

def find_swings(candles, k=SWING_K):
    """Pivots confirmés (k bougies de chaque côté). Les k dernières bougies
    ne peuvent pas contenir de pivot confirmé — lag assumé de k heures."""
    highs, lows = [], []
    for i in range(k, len(candles) - k):
        window = candles[i - k: i + k + 1]
        if candles[i]["h"] == max(c["h"] for c in window):
            highs.append({"i": i, "p": candles[i]["h"]})
        if candles[i]["l"] == min(c["l"] for c in window):
            lows.append({"i": i, "p": candles[i]["l"]})
    return highs, lows


def detect_signal(candles):
    """Retourne un dict candidat ou None. Règle strictement mécanique."""
    if len(candles) < 60:
        return None
    # Kraken renvoie la bougie en cours (non close) en dernier : on l'ignore.
    closed = candles[:-1]
    last = closed[-1]
    highs, lows = find_swings(closed)
    if len(highs) < 2 or len(lows) < 2:
        return None

    h1, h2 = highs[-2], highs[-1]          # avant-dernier, dernier sommet
    l1, l2 = lows[-2], lows[-1]
    vols = [c["v"] for c in closed[-21:-1]]
    vol_avg = sum(vols) / len(vols) if vols else 0
    vol_ok = vol_avg > 0 and last["v"] >= VOL_MULT * vol_avg

    # Contexte baissier + clôture au-dessus du dernier sommet -> HAUSSE
    bearish = h2["p"] < h1["p"] and l2["p"] < l1["p"]
    if bearish and last["c"] > h2["p"] and vol_ok:
        stop = l2["p"]
        dist = (last["c"] - stop) / last["c"] * 100
        if STOP_MIN_PCT <= dist <= STOP_MAX_PCT:
            return {"dir": "HAUSSE", "ref": last["c"], "invalidation": stop,
                    "dist_pct": dist, "broken_level": h2["p"],
                    "vol_ratio": last["v"] / vol_avg}

    # Contexte haussier + clôture sous le dernier creux -> BAISSE
    bullish = h2["p"] > h1["p"] and l2["p"] > l1["p"]
    if bullish and last["c"] < l2["p"] and vol_ok:
        stop = h2["p"]
        dist = (stop - last["c"]) / last["c"] * 100
        if STOP_MIN_PCT <= dist <= STOP_MAX_PCT:
            return {"dir": "BAISSE", "ref": last["c"], "invalidation": stop,
                    "dist_pct": dist, "broken_level": l2["p"],
                    "vol_ratio": last["v"] / vol_avg}
    return None


# ----------------------------------------------------------------------------
# Garde-fous
# ----------------------------------------------------------------------------

def in_macro_window(now_utc):
    for s in MACRO_EVENTS_UTC:
        ev = datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        if abs((now_utc - ev).total_seconds()) <= MACRO_WINDOW_H * 3600:
            return s
    return None


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"last_by_pair": {}}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"  [!] état non sauvegardé : {e}")


# ----------------------------------------------------------------------------
# Email
# ----------------------------------------------------------------------------

def send_email(subject, body):
    host = os.environ.get("SMTP_HOST")
    user = os.environ.get("SMTP_USER")
    pwd = os.environ.get("SMTP_PASS")
    to = os.environ.get("ALERT_TO")
    port = int(os.environ.get("SMTP_PORT", "465"))
    if not all([host, user, pwd, to]):
        print("  [i] SMTP non configuré — récapitulatif en console uniquement.")
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    msg.set_content(body)
    try:
        with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=20) as s:
            s.login(user, pwd)
            s.send_message(msg)
        return True
    except Exception as e:
        print(f"  [!] envoi email échoué : {e}")
        return False


def fmt_price(p):
    decimals = 8 if p < 0.001 else 6 if p < 0.01 else 4 if p < 1 else 2
    return f"{p:.{decimals}f}"


def format_digest(signals, now_utc):
    lines = [
        f"Scan du {now_utc:%d/%m/%Y %H:%M} UTC — {len(signals)} candidat(s) détecté(s)",
        "=" * 60,
        "",
    ]
    for i, (pair, sig) in enumerate(signals, 1):
        lines += [
            f"{i}. {sig['dir']} — {pair}",
            f"   Prix de référence   : {fmt_price(sig['ref'])}",
            f"   Niveau cassé        : {fmt_price(sig['broken_level'])}",
            f"   Invalidation (stop) : {fmt_price(sig['invalidation'])}  ({sig['dist_pct']:.2f} %)",
            f"   Volume de cassure   : {sig['vol_ratio']:.1f}× la moyenne",
            "",
        ]
    lines += [
        "-" * 60,
        "Classement par force de cassure (ratio de volume).",
        "Chaque ligne est un candidat à VÉRIFIER sur le graphique,",
        "pas un ordre. Saisis référence / invalidation dans le Poste",
        "de sélection : c'est lui qui dimensionne, classe par R:R net",
        "et applique la limite de 3 exécutions par jour.",
        "Règle non backtestée — prudence tant que ce n'est pas fait.",
    ]
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Boucle principale
# ----------------------------------------------------------------------------

def main():
    now = datetime.now(timezone.utc)
    print(f"[{now:%Y-%m-%d %H:%M} UTC] scan de {len(PAIRS)} paires")

    ev = in_macro_window(now)
    if ev:
        print(f"  Fenêtre macro ({ev} UTC ±{MACRO_WINDOW_H}h) — scan suspendu.")
        return

    state = load_state()
    signals = []

    for pair in PAIRS:
        last_ts = state["last_by_pair"].get(pair, 0)
        if time.time() - last_ts < COOLDOWN_HOURS * 3600:
            continue
        candles = fetch_ohlc(pair)
        time.sleep(1.1)  # courtoisie API publique
        if not candles:
            continue
        sig = detect_signal(candles)
        if not sig:
            continue
        signals.append((pair, sig))
        state["last_by_pair"][pair] = time.time()

    if signals:
        signals.sort(key=lambda x: x[1]["vol_ratio"], reverse=True)
        body = format_digest(signals, now)
        n_up = sum(1 for _, s in signals if s["dir"] == "HAUSSE")
        n_dn = len(signals) - n_up
        subject = f"[Candidats] {len(signals)} détecté(s) — {n_up} hausse / {n_dn} baisse"
        print("\n" + body + "\n")
        send_email(subject, body)
    else:
        print("  Aucun candidat sur ce scan.")

    save_state(state)
    print("  Scan terminé.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
