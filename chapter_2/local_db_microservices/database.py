import sqlite3

DATABASE = "reservations.db"


def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reservations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            guest_name  TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            time        TEXT    NOT NULL,
            party_size  INTEGER NOT NULL,
            notes       TEXT    DEFAULT '',
            created_at  TEXT    DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS latency (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            method        TEXT    NOT NULL,
            endpoint      TEXT    NOT NULL,
            status_code   INTEGER NOT NULL,
            duration_ms   REAL    NOT NULL,
            created_at    TEXT    DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()
    conn.close()
