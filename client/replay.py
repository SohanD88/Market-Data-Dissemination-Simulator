# client/replay.py
import sqlite3
import time
import threading
from typing import Dict, List
from client.live_plot import start_plot


def replay_from_db(
    db_path: str,
    instruments: List[str],
    playback_speed: float = 1.0,   # 1.0 = real time
) -> None:
    """Replays market data from the SQLite log, simulating a live stream."""
    state: Dict[str, List[float]] = {sym: [] for sym in instruments}
    lock = threading.Lock()
    stop_event = threading.Event()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT instrument, ts_ms, kind, value FROM marketdata "
        f"WHERE instrument IN ({','.join('?' * len(instruments))}) "
        "ORDER BY ts_ms ASC",
        instruments,
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print(f"[replay] No data found in {db_path}")
        return

    print(f"[replay] Loaded {len(rows)} events for {', '.join(instruments)}")

    # --- 1️⃣ Start the plot on the MAIN thread (Mac requirement)
    plot_thread_done = threading.Event()

    def run_plot():
        start_plot(
            state=state,
            lock=lock,
            instruments=instruments,
            stop_event=stop_event,
            interval_ms=500,
            auto_close=False,
        )
        plot_thread_done.set()

    # Start the plot synchronously (on main thread) before feeding data
    plot_thread = threading.Thread(target=run_plot, daemon=True)
    # 👇 Instead of .start(), call run_plot() directly (main thread)
    run_plot_thread = threading.Thread(target=lambda: None)

    # --- 2️⃣ Feed data on a background thread so GUI remains responsive
    def feeder():
        t0 = rows[0][1]
        start_time = time.time()
        for instrument, ts_ms, kind, val in rows:
            if stop_event.is_set():
                break
            dt = (ts_ms - t0) / 1000.0 / playback_speed
            target_time = start_time + dt
            sleep_time = target_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
            with lock:
                arr = state.get(instrument, [])
                arr.append(float(val))
                state[instrument] = arr[-200:]
        stop_event.set()
        print("[replay] Finished playback. Close the chart to exit.")

    feeder_thread = threading.Thread(target=feeder, daemon=True)
    feeder_thread.start()

    # Run the plot on the main thread (blocks until closed)
    start_plot(
        state=state,
        lock=lock,
        instruments=instruments,
        stop_event=stop_event,
        interval_ms=500,
        auto_close=False,
    )

    # Wait for data feeding to finish before exit
    feeder_thread.join()



if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="marketdata.db", help="Path to SQLite database")
    ap.add_argument("--instruments", default="ES,NQ", help="Comma-separated symbols")
    ap.add_argument("--speed", type=float, default=1.0, help="Playback speed (1.0 = real time)")
    args = ap.parse_args()

    instruments = [s.strip() for s in args.instruments.split(",") if s.strip()]
    replay_from_db(args.db, instruments, args.speed)
