#!/usr/bin/env python3

import os
import sys
import time
import requests

# ── Konfiguracja ──────────────────────────────────────────────────────────────
METABASE_URL      = os.getenv("METABASE_URL",      "http://localhost:3000")
METABASE_USER     = os.getenv("METABASE_USER",     "admin@veturilo.local")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "admin1234")
MB_DB_HOST        = os.getenv("MB_DB_HOST",        "postgres")
MB_DB_PORT        = int(os.getenv("MB_DB_PORT",    "5432"))
MB_DB_NAME        = os.getenv("MB_DB_NAME",        "veturilo_db")
MB_DB_USER        = os.getenv("MB_DB_USER",        "veturilo_user")
MB_DB_PASSWORD    = os.getenv("MB_DB_PASSWORD",    "veturilo_password")

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg: str):
    print(f"  → {msg}")


def api(method: str, path: str, **kwargs):
    url = f"{METABASE_URL}/api{path}"
    resp = session.request(method, url, **kwargs)
    if not resp.ok:
        print(f"\n[BŁĄD] {method.upper()} {path}")
        print(f"       Status: {resp.status_code}")
        print(f"       Body:   {resp.text[:400]}")
        sys.exit(1)
    return resp.json()


def wait_for_metabase(retries=20, delay=10):
    print(f"\n⏳ Czekam na Metabase pod {METABASE_URL} ...")
    for i in range(retries):
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.ok and r.json().get("status") == "ok":
                print("✅ Metabase gotowy!\n")
                return
        except requests.exceptions.ConnectionError:
            pass
        print(f"   Próba {i+1}/{retries} — czekam {delay}s ...")
        time.sleep(delay)
    print("❌ Metabase niedostępny. Upewnij się że kontener działa.")
    sys.exit(1)


# ── Krok 1: Login ─────────────────────────────────────────────────────────────

def login():
    print("1. Logowanie do Metabase ...")
    data = api("POST", "/session", json={
        "username": METABASE_USER,
        "password": METABASE_PASSWORD,
    })
    token = data["id"]
    session.headers["X-Metabase-Session"] = token
    log(f"Zalogowano, token: {token[:8]}...")
    return token


# ── Krok 2: Baza danych ───────────────────────────────────────────────────────

def get_or_create_database():
    print("2. Konfiguracja połączenia z PostgreSQL ...")

    databases = api("GET", "/database")
    existing = databases.get("data", databases) if isinstance(databases, dict) else databases
    for db in existing:
        if db.get("name") == "Veturilo DB":
            log(f"Baza już istnieje (id={db['id']}), pomijam.")
            return db["id"]

    db = api("POST", "/database", json={
        "name":   "Veturilo DB",
        "engine": "postgres",
        "details": {
            "host":     MB_DB_HOST,
            "port":     MB_DB_PORT,
            "dbname":   MB_DB_NAME,
            "user":     MB_DB_USER,
            "password": MB_DB_PASSWORD,
            "ssl":      False,
        },
        "auto_run_queries":  True,
        "is_full_sync":      True,
        "is_on_demand":      False,
    })
    db_id = db["id"]
    log(f"Baza utworzona (id={db_id}). Czekam na sync metadanych (20s) ...")
    time.sleep(20)
    return db_id


# ── Krok 3: Tworzenie Questions (Cards) ──────────────────────────────────────

def create_card(payload: dict) -> int:
    card = api("POST", "/card", json=payload)
    log(f"Karta '{payload['name']}' (id={card['id']})")
    return card["id"]


def build_cards(db_id: int) -> dict:
    """Zwraca słownik {nazwa: card_id}"""
    print("3. Tworzenie pytań (questions) ...")
    cards = {}

    # ── 1. Aktywne stacje ────────────────────────────────────────────────────
    cards["aktywne_stacje"] = create_card({
        "name":    "🚲 Aktywne stacje",
        "display": "scalar",
        "visualization_settings": {},
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native": {
                "query": """
                    SELECT COUNT(DISTINCT name) as "Aktywne stacje"
                    FROM   station_status
                    WHERE  event_time >= (SELECT max(event_time) from station_status)
                    AND  bikes_available > 0;
                """,
            },
        },
    })

    # ── 2. Stacje krytyczne (< 10%) ──────────────────────────────────────────
    cards["stacje_krytyczne"] = create_card({
        "name":    "⚠️Stacje krytyczne (< 10%)",
        "display": "scalar",
        "visualization_settings": {},
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native": {
                "query": """
                    WITH latest AS (
                    SELECT DISTINCT ON (name)
                            name, occupancy_rate, bikes_available
                    FROM   station_status
                    ORDER BY name, event_time DESC
                    )
                    SELECT COUNT(*) AS critical_stations
                    FROM   latest
                    WHERE  occupancy_rate < 10
                    AND  bikes_available > 0;
                """,
            },
        },
    })

    # ── 3. Puste stacje ──────────────────────────────────────────────────────
    cards["puste_stacje"] = create_card({
        "name":    "🪹 Puste stacje",
        "display": "scalar",
        "visualization_settings": {},
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native": {
                "query": """
                    WITH latest AS (
                    SELECT DISTINCT ON (name)
                            name, bikes_available
                    FROM   station_status
                    ORDER BY name, event_time DESC
                    )
                    SELECT COUNT(*) AS empty_stations
                    FROM   latest
                    WHERE  bikes_available = 0;
                """,
            },
        },
    })

    # ── 4. Temperatura i Zapełnienie — bubble chart ──────────────────────────
    cards["temp_zapelnienie_bubble"] = create_card({
        "name":    "Temperatura i Zapełnienie - ostatnie 7 dni",
        "display": "scatter",
        "visualization_settings": {
        "graph.dimensions": ["temp_c"],           # oś X
        "graph.metrics": ["avg_occupancy_pct"],   # oś Y
        "scatter.bubble": "station_count",         # rozmiar bąbelka
        
        "graph.x_axis.title_text": "Temperatura (°C)",
        "graph.y_axis.title_text": "Zapełnienie (%)",
        "graph.x_axis.labels_enabled": True,
        "graph.y_axis.labels_enabled": True,
        
        "column_settings": {
            '["name","temp_c"]': {"suffix": "°C", "column_title": "Temperatura"},
            '["name","avg_occupancy_pct"]': {"suffix": "%", "column_title": "Zapełnienie"},
        },
    },
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native": {
                "query": """
                    WITH station_temp AS (
                        SELECT
                            station_id,
                            ROUND(temp::numeric) AS temp_c,
                            AVG(occupancy_rate::numeric) AS occupancy_rate
                        FROM station_status
                        WHERE temp IS NOT NULL
                        AND event_time >= NOW() - INTERVAL '7 days'
                        GROUP BY station_id, ROUND(temp::numeric)
                    )
                    SELECT
                        temp_c,
                        ROUND(AVG(occupancy_rate::numeric), 2) AS avg_occupancy_pct,
                        COUNT(*) AS station_count
                    FROM station_temp
                    GROUP BY temp_c
                    ORDER BY temp_c;
                """,
            },
        },
    })

    # ── 5. Ostatnie alerty ───────────────────────────────────────────────────
    cards["ostatnie_alerty"] = create_card({
    "name":    "Ostatnie alerty",
    "display": "table",
    "visualization_settings": {
        "table.columns": [
            {"name": "name",          "enabled": True},
            {"name": "bikes_available","enabled": False},   # ukryta
            {"name": "occupancy_pct", "enabled": True},
            {"name": "pogoda",        "enabled": True},
            {"name": "minuty_temu",   "enabled": True},
            {"name": "sent_at",       "enabled": False},    # ukryta
        ],
        "table.column_formatting": [
            {
                "columns":          ["occupancy_pct"],
                "type":             "single",
                "operator":         ">=",
                "value":            0,
                "color":            "#EF8C8C",   # czerwony
                "highlight_row":    False,
            }
        ],
        "column_settings": {
            '["name","name"]':          {"column_title": "Stacja"},
            '["name","occupancy_pct"]': {"column_title": "Zapełnienie", "suffix": "%"},
            '["name","pogoda"]':        {"column_title": "Pogoda"},
            '["name","minuty_temu"]':   {"column_title": "Minut temu"},
        },
    },
    "dataset_query": {
        "type":     "native",
        "database": db_id,
        "native": {
            "query": """
                SELECT
                    name,
                    bikes_available,
                    ROUND(occupancy_rate::numeric, 2) AS occupancy_pct,
                    CASE
                        WHEN rain > 0 THEN '🌧 ' || rain::text || 'mm'
                        ELSE '☀️'
                    END AS pogoda,
                    ROUND(
                        EXTRACT(EPOCH FROM (NOW() - sent_at)) / 60
                    ) AS minuty_temu,
                    sent_at
                FROM   veturilo_alerts
                ORDER BY sent_at DESC
                LIMIT  20;
            """,
        },
    },
    })

    # ── 6. Trend zapełnienia — ostatnie 3 godziny ────────────────────────────
    cards["trend_zapelnienia"] = create_card({
        "name":    "Trend zapełnienia - ostatnie 3 godziny",
        "display": "table",
        "visualization_settings": {
            "column_settings": {
                '["name","bucket"]':           {"column_title": "Godzina"},
                '["name","avg_occupancy_pct"]': {"column_title": "Procent zapełnienia", "suffix": "%"},
                '["name","min_occupancy_pct"]': {"column_title": "Min zapełnienie",     "suffix": "%"},
                '["name","max_occupancy_pct"]': {"column_title": "Max zapełnienie",     "suffix": "%"},
            },
        },
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native": {
                "query": """
                    SELECT
                        date_trunc('minute', event_time)
                            - (EXTRACT(MINUTE FROM event_time)::int % 15)
                            * INTERVAL '1 minute'               AS bucket,
                        ROUND(AVG(occupancy_rate::numeric), 2)  AS avg_occupancy_pct,
                        ROUND(MIN(occupancy_rate::numeric), 2)  AS min_occupancy_pct,
                        ROUND(MAX(occupancy_rate::numeric), 2)  AS max_occupancy_pct
                    FROM   station_status
                    WHERE  event_time >= NOW() - INTERVAL '3 hours'
                    GROUP BY bucket
                    ORDER BY bucket DESC;
                """,
            },
        },
    })

    # ── 7. Czy stacja wraca do normy? ────────────────────────────────────────
    cards["powrot_do_normy"] = create_card({
    "name":    "Czy stacja wraca do normy?",
    "display": "table",
    "visualization_settings": {
        "table.columns": [
            {"name": "name",               "enabled": True},
            {"name": "recovered_count",    "enabled": False},  # ukryta
            {"name": "stuck_count",        "enabled": False},  # ukryta
            {"name": "recovery_rate_pct",  "enabled": True},
        ],
        "table.column_formatting": [
            {
                "columns":       ["recovery_rate_pct"],
                "type":          "single",
                "operator":      ">=",
                "value":         80,
                "color":         "#84BB4C",   # zielony
                "highlight_row": False,
            },
            {
                "columns":       ["recovery_rate_pct"],
                "type":          "single",
                "operator":      ">=",
                "value":         40,
                "color":         "#F9CF48",   # żółty
                "highlight_row": False,
            },
            {
                "columns":       ["recovery_rate_pct"],
                "type":          "single",
                "operator":      "<",
                "value":         40,
                "color":         "#EF8C8C",   # czerwony
                "highlight_row": False,
            },
        ],
        "column_settings": {
            '["name","name"]':              {"column_title": "Stacja"},
            '["name","recovery_rate_pct"]': {"column_title": "Skuteczność wracania do normy po alercie", "suffix": "%"},
        },
    },
    "dataset_query": {
        "type":     "native",
        "database": db_id,
        "native": {
            "query": """
                WITH alert_times AS (
                    SELECT name, sent_at
                    FROM   veturilo_alerts
                    WHERE  sent_at >= NOW() - INTERVAL '7 days'
                ),
                next_status AS (
                    SELECT DISTINCT ON (a.name, a.sent_at)
                        a.name,
                        a.sent_at                         AS alert_time,
                        s.event_time                      AS next_check,
                        s.occupancy_rate,
                        s.bikes_available,
                        (s.occupancy_rate >= 10)          AS recovered
                    FROM   alert_times a
                    JOIN   station_status s
                        ON  s.name = a.name
                        AND s.event_time > a.sent_at
                    ORDER BY a.name, a.sent_at, s.event_time ASC
                )
                SELECT
                    name,
                    COUNT(*) FILTER (WHERE recovered)      AS recovered_count,
                    COUNT(*) FILTER (WHERE NOT recovered)  AS stuck_count,
                    ROUND(
                        COUNT(*) FILTER (WHERE recovered) * 100.0
                        / NULLIF(COUNT(*), 0), 1
                    )::numeric AS recovery_rate_pct
                FROM   next_status
                GROUP BY name
                ORDER BY recovery_rate_pct DESC
                LIMIT  10;
            """,
        },
    },
})

    # ── 8. Najmniej zaopatrzone stacje — bar chart ───────────────────────────
=    cards["najmniej_zaopatrzone"] = create_card({
    "name":    "Najmniej zaopatrzone stacje",
    "display": "row",   # <-- zamiast "bar"
    "visualization_settings": {
        "graph.dimensions":      ["name"],
        "graph.metrics":         ["critical_stations"],
        "graph.x_axis.title_text": "",
        "graph.y_axis.title_text": "",
        "graph.x_axis.labels_enabled": False,
        "graph.x_axis.title_enabled": False,   
        "graph.y_axis.title_enabled": False,
        "graph.colors":          ["#F28B82"],
        "column_settings": {
            '["name","critical_stations"]': {"suffix": "%", "column_title": "Zapełnienie"},
        },
    },
    "dataset_query": {
        "type":     "native",
        "database": db_id,
        "native": {
            "query": """
                WITH latest AS (
                    SELECT DISTINCT ON (name)
                        name, occupancy_rate, bikes_available
                    FROM   station_status
                    ORDER BY name, event_time DESC
                )
                SELECT name, occupancy_rate AS critical_stations
                FROM   latest
                WHERE  occupancy_rate < 10
                AND    bikes_available > 0;
            """,
        },
    },
    })

    # ── 9. Najbardziej zapełnione stacje ─────────────────────────────────────
    cards["najbardziej_zapelnione"] = create_card({
        "name":    "Najbardziej zapełnione stacje",
        "display": "table",
        "visualization_settings": {
            "column_settings": {
                '["name","name"]':           {"column_title": "Stacja"},
                '["name","occupancy_rate"]': {"column_title": "Zapełnienie", "suffix": "%"},
            },
        },
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native": {
                "query": """
                    WITH latest AS (
                        SELECT DISTINCT ON (name)
                            name,
                            occupancy_rate,
                            bikes_available,
                            event_time
                        FROM station_status
                        ORDER BY name, event_time DESC
                    )
                    SELECT name, occupancy_rate
                    FROM latest
                    WHERE occupancy_rate > 95
                    AND   occupancy_rate <= 100
                    AND   name NOT LIKE 'BIKE%'
                    ORDER BY occupancy_rate DESC, name ASC;
                """,
            },
        },
    })

    # ── 10. Deszcz i Zapełnienie — bubble chart ──────────────────────────────
    cards["deszcz_zapelnienie"] = create_card({
    "name":    "Deszcz i Zapełnienie",
    "display": "scatter",
    "visualization_settings": {
        "graph.dimensions": ["rain_mm"],          # oś X
        "graph.metrics":    ["avg_occupancy_pct"], # oś Y
        "scatter.bubble":   "sample_count",        # rozmiar bąbelka
        "graph.x_axis.title_text": "Opady w mm",
        "graph.y_axis.title_text": "Zapełnienie",
        "column_settings": {
            '["name","avg_occupancy_pct"]': {"suffix": "%", "column_title": "Zapełnienie"},
            '["name","rain_mm"]':           {"suffix": " mm", "column_title": "Opady"},
        },
    },
    "dataset_query": {
        "type":     "native",
        "database": db_id,
        "native": {
            "query": """
                SELECT
                    ROUND(rain::numeric, 2)                AS rain_mm,
                    ROUND(AVG(occupancy_rate::numeric), 2) AS avg_occupancy_pct,
                    COUNT(*)                               AS sample_count
                FROM   station_status
                WHERE  rain IS NOT NULL
                AND    event_time >= NOW() - INTERVAL '7 days'
                GROUP BY ROUND(rain::numeric, 2)
                ORDER BY rain_mm DESC;
            """,
        },
    },
})

    return cards



def create_dashboard(cards: dict) -> int:
    print("4. Tworzenie dashboardu ...")

    all_dashboards = api("GET", "/dashboard")
    for d in all_dashboards:
        if d.get("name") == "Veturilo - monitorowanie dostępności floty rowerów miejskich":
            log(f"Dashboard już istnieje (id={d['id']}), pomijam.")
            return d["id"]

    dash = api("POST", "/dashboard", json={
        "name": "Veturilo - monitorowanie dostępności floty rowerów miejskich",
    })
    dash_id = dash["id"]
    log(f"Dashboard utworzony (id={dash_id})")
    return dash_id


def add_cards_to_dashboard(dash_id: int, cards: dict):
    print("5. Dodawanie kart do dashboardu ...")

    layout = [
        # ── Rząd 1: metryki + bubble temp ──────────────────────────────────
        {"card_key": "aktywne_stacje",          "col": 0,  "row": 0,  "size_x": 3,  "size_y": 3},
        {"card_key": "stacje_krytyczne",        "col": 3,  "row": 0,  "size_x": 3,  "size_y": 3},
        {"card_key": "puste_stacje",            "col": 6, "row": 0,  "size_x": 3,  "size_y": 3},
        {"card_key": "temp_zapelnienie_bubble", "col": 9, "row": 0,  "size_x": 8,  "size_y": 3},

        # ── Rząd 2: alerty + trend zapełnienia ─────────────────────────────
        {"card_key": "ostatnie_alerty",         "col": 0,  "row": 4,  "size_x": 7, "size_y": 6},
        {"card_key": "trend_zapelnienia",       "col": 7, "row": 4,  "size_x": 10,  "size_y": 3},

        # ── Rząd 3: powrót do normy ─────────────────────────────────────────
        {"card_key": "powrot_do_normy",         "col": 7, "row": 7,  "size_x": 10,  "size_y": 3},

        # ── Rząd 4: bar + tabela + bubble deszcz ───────────────────────────
        {"card_key": "najmniej_zaopatrzone",    "col": 0,  "row": 12, "size_x": 6,  "size_y": 7},
        {"card_key": "najbardziej_zapelnione",  "col": 6,  "row": 12, "size_x": 6,  "size_y": 7},
        {"card_key": "deszcz_zapelnienie",      "col": 12, "row": 12, "size_x": 6,  "size_y": 7},
    ]

    for item in layout:
        card_id = cards[item["card_key"]]
        api("POST", f"/dashboard/{dash_id}/cards", json={
            "cardId": card_id,
            "col":    item["col"],
            "row":    item["row"],
            "size_x": item["size_x"],
            "size_y": item["size_y"],
        })
        log(f"  Dodano '{item['card_key']}' na pozycję ({item['col']}, {item['row']})")



def main():
    print("=" * 60)
    print("  Veturilo — Metabase Dashboard Seed")
    print("=" * 60)

    wait_for_metabase()
    login()
    db_id = get_or_create_database()
    cards = build_cards(db_id)
    dash_id = create_dashboard(cards)
    add_cards_to_dashboard(dash_id, cards)

    print("\n" + "=" * 60)
    print(f"✅ Gotowe! Dashboard dostępny pod:")
    print(f"   {METABASE_URL}/dashboard/{dash_id}")
    print("=" * 60)


if __name__ == "__main__":
    main()