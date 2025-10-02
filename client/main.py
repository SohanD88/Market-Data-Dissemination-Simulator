# client/main.py
import argparse
import threading
import time
from typing import Dict, Optional, List
import grpc
import traceback

from md import marketdata_pb2 as pb2
from md import marketdata_pb2_grpc as pb2_grpc


def do_ping(stub: pb2_grpc.MarketDataStub) -> None:
    """Send a simple Ping to confirm the server is reachable."""
    reply = stub.Ping(pb2.PingRequest(message="hello, server"))
    print(f"[client] ping reply: {reply.message}")


def stream_worker(
    stub: pb2_grpc.MarketDataStub,
    instrument: str,
    max_msgs: Optional[int],
    state: Dict[str, float],
    lock: threading.Lock,
    stop_event: threading.Event,
) -> None:
    """
    Subscribe to one instrument and keep the latest value in `state`.
    Exits cleanly when:
      - max_msgs is reached, OR
      - stop_event is set, OR
      - channel is closed (CANCELLED at shutdown).
    """
    req = pb2.SubscribeRequest(instrument_id=instrument)
    count = 0
    try:
        call = stub.StreamPrices(req)
        for up in call:
            if stop_event.is_set():
                # Ask gRPC to stop this stream; will raise CANCELLED
                try:
                    call.cancel()  # type: ignore[attr-defined]
                except Exception:
                    pass
                break
            with lock:
                state[up.instrument_id] = up.value
            count += 1
            if max_msgs and count >= max_msgs:
                break
    except grpc.RpcError as e:
        # During normal shutdown we expect CANCELLED when the channel closes.
        code = e.code() if hasattr(e, "code") else None
        if code != grpc.StatusCode.CANCELLED:
            print(f"[client][{instrument}] gRPC error: {e} (code={code})")
            traceback.print_exc()
    except Exception:
        print(f"[client][{instrument}] unexpected error:")
        traceback.print_exc()


def print_state_periodically(
    state: Dict[str, float],
    lock: threading.Lock,
    instruments: List[str],
    interval: float,
    run_seconds: Optional[int],
    stop_event: threading.Event,
) -> None:
    """Print a small table of latest values every `interval` seconds."""
    t0 = time.time()
    while not stop_event.is_set():
        with lock:
            lines = [f"{sym:>6}: {state.get(sym, float('nan')):.4f}" for sym in instruments]
        print("\n[client] latest")
        print("\n".join(lines))
        time.sleep(interval)
        if run_seconds and (time.time() - t0) >= run_seconds:
            break
    # signal done (idempotent)
    stop_event.set()


def main(
    host: str,
    port: int,
    instruments: List[str],
    max_msgs: Optional[int],
    run_seconds: Optional[int],
) -> None:
    addr = f"{host}:{port}"
    state: Dict[str, float] = {}
    lock = threading.Lock()
    stop_event = threading.Event()

    # Open the channel for the whole session.
    with grpc.insecure_channel(addr) as channel:
        stub = pb2_grpc.MarketDataStub(channel)
        do_ping(stub)

        # Start one worker per instrument (non-daemon so we can join)
        threads: List[threading.Thread] = []
        for sym in instruments:
            th = threading.Thread(
                target=stream_worker,
                args=(stub, sym, max_msgs, state, lock, stop_event),
                daemon=False,
            )
            th.start()
            threads.append(th)

        # Print the table for run_seconds (or until stop_event is set)
        print_state_periodically(
            state, lock, instruments,
            interval=1.0,
            run_seconds=run_seconds,
            stop_event=stop_event
        )

        # Tell workers to stop if we had a timed run
        stop_event.set()

        # Join all workers before leaving the channel context
        for th in threads:
            th.join()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument(
        "--instruments",
        default="ES,NQ",
        help="Comma-separated list, e.g. ES,NQ,CL",
    )
    ap.add_argument("--max-msgs", type=int, default=0, help="0 = infinite per instrument")
    ap.add_argument("--seconds", type=int, default=8, help="0 = run until streams end")
    args = ap.parse_args()

    instruments: List[str] = [s.strip() for s in args.instruments.split(",") if s.strip()]
    max_msgs: Optional[int] = None if args.max_msgs == 0 else args.max_msgs
    run_seconds: Optional[int] = None if args.seconds == 0 else args.seconds

    main(args.host, args.port, instruments, max_msgs, run_seconds)
