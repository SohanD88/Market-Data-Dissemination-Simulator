# client/main.py
import argparse
import threading
import time
from typing import Dict, List, Optional
import traceback
import grpc

from md import marketdata_pb2 as pb2
from md import marketdata_pb2_grpc as pb2_grpc
from client.logger import start_logger   # ✅ new import for logging


# ---------------------------------------------------------------------------
# Simple connectivity check
# ---------------------------------------------------------------------------
def do_ping(stub: pb2_grpc.MarketDataStub) -> None:
    reply = stub.Ping(pb2.PingRequest(message="hello, server"))
    print(f"[client] ping reply: {reply.message}")


# ---------------------------------------------------------------------------
# Worker that handles one instrument stream
# ---------------------------------------------------------------------------
def stream_worker(
    stub: pb2_grpc.MarketDataStub,
    instrument: str,
    max_msgs: Optional[int],
    state: Dict[str, List[float]],
    lock: threading.Lock,
    stop_event: threading.Event,
    keep_last: int = 5,
    enqueue=None,   # ✅ new argument for logger
) -> None:
    """Subscribe to one instrument and store/print updates."""
    req = pb2.SubscribeRequest(instrument_id=instrument)
    count = 0

    try:
        call = stub.StreamPrices(req)
        for up in call:
            if stop_event.is_set():
                try:
                    call.cancel()  # close stream cleanly
                except Exception:
                    pass
                break

            # --- Snapshot message ---
            if up.HasField("snapshot"):
                prices = list(up.snapshot.prices)
                ts_ms = int(time.time() * 1000)  # snapshot doesn't carry ts
                if enqueue:
                    for p in prices:
                        enqueue((instrument, ts_ms, "snapshot", float(p)))
                with lock:
                    state[instrument] = prices[-keep_last:] if keep_last else prices
                print(f"[client][{instrument}] SNAPSHOT {state[instrument]}")

            # --- Incremental message ---
            elif up.HasField("incremental"):
                ts_ms = up.incremental.ts_ms
                val = float(up.incremental.value)
                if enqueue:
                    enqueue((instrument, ts_ms, "incremental", val))
                with lock:
                    arr = state.get(instrument, [])
                    arr.append(val)
                    if keep_last and len(arr) > keep_last:
                        arr = arr[-keep_last:]
                    state[instrument] = arr

            count += 1
            if max_msgs and count >= max_msgs:
                break

    except grpc.RpcError as e:
        code = e.code() if hasattr(e, "code") else None
        if code != grpc.StatusCode.CANCELLED:
            print(f"[client][{instrument}] gRPC error: {e} (code={code})")
            traceback.print_exc()
    except Exception:
        print(f"[client][{instrument}] unexpected error:")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Periodic table printer
# ---------------------------------------------------------------------------
def print_state_periodically(
    state: Dict[str, List[float]],
    lock: threading.Lock,
    instruments: List[str],
    interval: float,
    run_seconds: Optional[int],
    stop_event: threading.Event,
) -> None:
    t0 = time.time()
    while not stop_event.is_set():
        with lock:
            lines: List[str] = []
            for sym in instruments:
                latest = state.get(sym, [])
                if latest:
                    lines.append(f"{sym:>6}: {latest[-1]:.4f}")
                else:
                    lines.append(f"{sym:>6}: …")

        print(f"\n[client] {time.strftime('%X')} latest")
        print("\n".join(lines))

        time.sleep(interval)
        if run_seconds and (time.time() - t0) >= run_seconds:
            break

    stop_event.set()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(
    host: str,
    port: int,
    instruments: List[str],
    max_msgs: Optional[int],
    run_seconds: Optional[int],
) -> None:
    addr = f"{host}:{port}"
    state: Dict[str, List[float]] = {}
    lock = threading.Lock()
    stop_event = threading.Event()

    # ✅ 1. Start the logger
    enqueue, stop_logger = start_logger("marketdata.db")

    with grpc.insecure_channel(addr) as channel:
        stub = pb2_grpc.MarketDataStub(channel)
        do_ping(stub)

        # ✅ 2. Start one thread per instrument
        threads: List[threading.Thread] = []
        for sym in instruments:
            th = threading.Thread(
                target=stream_worker,
                args=(stub, sym, max_msgs, state, lock, stop_event),
                kwargs={"enqueue": enqueue},   # pass logger
                daemon=False,
            )
            th.start()
            threads.append(th)

        # ✅ 3. Display state periodically
        print_state_periodically(
            state, lock, instruments,
            interval=1.0,
            run_seconds=run_seconds,
            stop_event=stop_event,
        )

        # ✅ 4. Clean shutdown
        stop_event.set()
        for th in threads:
            th.join()

    # ✅ 5. Stop the logger and flush to DB
    stop_logger()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument(
        "--instruments",
        default="ES,NQ",
        help="Comma-separated list of instruments (e.g., ES,NQ,CL)",
    )
    ap.add_argument("--max-msgs", type=int, default=0, help="0 = unlimited per instrument")
    ap.add_argument("--seconds", type=int, default=8, help="0 = run until stopped")
    args = ap.parse_args()

    instruments: List[str] = [s.strip() for s in args.instruments.split(",") if s.strip()]
    max_msgs: Optional[int] = None if args.max_msgs == 0 else args.max_msgs
    run_seconds: Optional[int] = None if args.seconds == 0 else args.seconds

    main(args.host, args.port, instruments, max_msgs, run_seconds)
