import logging
import sqlite3
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

GATEWAY_URL = "http://gateway:8000"
CATALOG_SERVICE_URL = "http://catalog-service:8001"
CACHE_DB_PATH = "cache.db"


def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(CACHE_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    _ensure_tables(conn)
    return conn


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cached_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            last_synced TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cached_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT UNIQUE NOT NULL,
            email TEXT NOT NULL,
            name TEXT NOT NULL,
            last_synced TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT UNIQUE NOT NULL,
            last_sync_at TEXT NOT NULL,
            records_synced INTEGER NOT NULL DEFAULT 0
        );
    """)


def _get_last_sync(conn: sqlite3.Connection, entity_type: str) -> str | None:
    row = conn.execute(
        "SELECT last_sync_at FROM sync_state WHERE entity_type = ?",
        (entity_type,),
    ).fetchone()
    return row["last_sync_at"] if row else None


def _update_sync_state(conn: sqlite3.Connection, entity_type: str, count: int) -> None:
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO sync_state (entity_type, last_sync_at, records_synced)
        VALUES (?, ?, ?)
        ON CONFLICT(entity_type) DO UPDATE SET
            last_sync_at = excluded.last_sync_at,
            records_synced = excluded.records_synced
        """,
        (entity_type, now, count),
    )
    conn.commit()


def sync_products() -> dict[str, Any]:
    try:
        response = requests.get(
            f"{CATALOG_SERVICE_URL}/products",
            params={"page_size": 1000},
            timeout=30,
        )
        response.raise_for_status()
        products = response.json().get("products", [])
    except requests.RequestException as exc:
        logger.error("Failed to fetch products from catalog service: %s", exc)
        return {"synced": 0, "error": str(exc)}

    conn = _get_db()
    now = datetime.utcnow().isoformat()
    synced = 0

    try:
        for product in products:
            conn.execute(
                """
                INSERT INTO cached_products (source_id, name, price, last_synced)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    name = excluded.name,
                    price = excluded.price,
                    last_synced = excluded.last_synced
                """,
                (str(product["id"]), product["name"], product["price"], now),
            )
            synced += 1

        conn.commit()
        _update_sync_state(conn, "products", synced)
        logger.info("Synced %d products from catalog service", synced)
    finally:
        conn.close()

    return {"synced": synced}


def sync_users() -> dict[str, Any]:
    try:
        response = requests.get(
            f"{GATEWAY_URL}/api/users",
            params={"page_size": 1000},
            timeout=30,
        )
        response.raise_for_status()
        users = response.json().get("users", [])
    except requests.RequestException as exc:
        logger.error("Failed to fetch users from gateway: %s", exc)
        return {"synced": 0, "error": str(exc)}

    conn = _get_db()
    now = datetime.utcnow().isoformat()
    synced = 0

    try:
        for user in users:
            conn.execute(
                """
                INSERT INTO cached_users (source_id, email, name, last_synced)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    email = excluded.email,
                    name = excluded.name,
                    last_synced = excluded.last_synced
                """,
                (str(user["id"]), user["email"], user["name"], now),
            )
            synced += 1

        conn.commit()
        _update_sync_state(conn, "users", synced)
        logger.info("Synced %d users from gateway", synced)
    finally:
        conn.close()

    return {"synced": synced}


def sync_orders_incremental() -> dict[str, Any]:
    conn = _get_db()
    last_sync = _get_last_sync(conn, "orders")

    params: dict[str, Any] = {"page_size": 500}
    if last_sync:
        params["since"] = last_sync

    try:
        response = requests.get(
            f"{GATEWAY_URL}/api/orders",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        orders = data.get("orders", [])
    except requests.RequestException as exc:
        logger.error("Failed to fetch orders from gateway: %s", exc)
        conn.close()
        return {"synced": 0, "error": str(exc)}

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cached_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT UNIQUE NOT NULL,
            user_id TEXT NOT NULL,
            status TEXT NOT NULL,
            total_amount REAL NOT NULL,
            created_at TEXT NOT NULL,
            last_synced TEXT NOT NULL
        )
    """)

    now = datetime.utcnow().isoformat()
    synced = 0

    try:
        for order in orders:
            conn.execute(
                """
                INSERT INTO cached_orders (source_id, user_id, status, total_amount, created_at, last_synced)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    status = excluded.status,
                    total_amount = excluded.total_amount,
                    last_synced = excluded.last_synced
                """,
                (
                    str(order["id"]),
                    str(order["user_id"]),
                    order["status"],
                    order["total_amount"],
                    order.get("created_at", now),
                    now,
                ),
            )
            synced += 1

        conn.commit()
        _update_sync_state(conn, "orders", synced)
        logger.info("Incrementally synced %d orders from gateway", synced)
    finally:
        conn.close()

    return {"synced": synced, "since": last_sync}
