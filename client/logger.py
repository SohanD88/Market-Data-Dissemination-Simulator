import sqlite3
import threading
import queue
import time
from typing import Optional, Tuple, List, Callable

Record = Tuple[str, int, str, float]

def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS marketdata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument TEXT NOT NULL,
            ts_ms INTEGER NOT NULL,
            kind TEXT NOT NULL,   -- 'snapshot' | 'incremental'
            value REAL
        )
    """)
    conn.commit()

def start_logger(db_path: str = "marketdata.db", commit_every: int = 100, flush_interval_sec: float = 1.0,) -> tuple[Callable[[Record], None], Callable[[], None]]:
    q: "queue.Queue[Optional[Record]]" = queue.Queue()
    stop_event = threading.Event()

    conn = sqlite3.connect(db_path, check_same_thread=False)
    _init_db(conn)

    def writer() -> None:
        buf: List[Record] = []
        last_flush = time.time()
        cur = conn.cursor()
        try:
            while not stop_event.is_set() or not q.empty():
                try:
                    item = q.get(timeout = 0.1)
                except queue.Empty:
                    item = None
                
                if item is not None:
                    buf.append(item)

                if buf and (len(buf) >= commit_every or (time.time() - last_flush) >= flush_interval_sec):
                    cur.executemany(
                        "INSERT INTO marketdata (instrument, ts_ms, kind, value) VALUES (?, ?, ?, ?)",
                        buf
                    )
                    conn.commit()
                    buf.clear()
                    last_flush = time.time()
                
            if buf:
                cur.executemany(
                    "INSERT INTO marketdata (instrument, ts_ms, kind, value) VALUES (?, ?, ?, ?)",
                    buf
                )
                conn.commit()
        finally:
            conn.close()

    t = threading.Thread(target=writer, name="db-writer", daemon=False)
    t.start()

    def enqueue(rec: Record) -> None:
        q.put(rec)

    def stop() -> None:
        stop_event.set()
        t.join()

    return enqueue, stop