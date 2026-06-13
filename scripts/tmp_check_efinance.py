from __future__ import annotations

import argparse
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data.provider_utils import normalize_symbol, standardize_ohlcv  # noqa: E402


PROXY_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)


@contextmanager
def maybe_clear_proxy(clear: bool):
    old_values = {key: os.environ.get(key) for key in PROXY_KEYS}
    if clear:
        for key in PROXY_KEYS:
            os.environ.pop(key, None)
    try:
        yield
    finally:
        if clear:
            for key, value in old_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def print_proxy_env() -> None:
    print("proxy env:")
    found = False
    for key in PROXY_KEYS:
        value = os.environ.get(key)
        if value:
            print(f"  {key}={value}")
            found = True
    if not found:
        print("  <empty>")


def fetch_once(symbol: str, beg: str, end: str, fqt: int, clear_proxy: bool) -> None:
    label = "proxy-cleared" if clear_proxy else "current-env"
    print(f"\n=== efinance direct fetch: {label} ===")
    with maybe_clear_proxy(clear_proxy):
        print_proxy_env()
        import efinance as ef  # type: ignore

        code = normalize_symbol(symbol)
        started = time.perf_counter()
        try:
            raw = ef.stock.get_quote_history(stock_codes=code, beg=beg, end=end, klt=101, fqt=fqt)
        except Exception as exc:
            elapsed = time.perf_counter() - started
            print(f"exception after {elapsed:.2f}s: {type(exc).__name__}: {exc}")
            return

        elapsed = time.perf_counter() - started
        if isinstance(raw, list):
            print(f"raw type=list len={len(raw)}")
            raw = raw[0] if raw else None
        else:
            print(f"raw type={type(raw).__name__}")

        if raw is None:
            print(f"raw is None after {elapsed:.2f}s")
            return
        print(f"elapsed={elapsed:.2f}s raw shape={getattr(raw, 'shape', None)}")
        print(f"raw columns={list(getattr(raw, 'columns', []))}")
        if getattr(raw, "empty", True):
            print("raw dataframe is empty")
            return

        print("raw tail:")
        print(raw.tail(5).to_string(index=False))

        normalized = standardize_ohlcv(raw, symbol)
        print(f"normalized rows={len(normalized)}")
        if normalized.empty:
            return
        print(
            "normalized range="
            f"{normalized['time'].min().date().isoformat()} ~ "
            f"{normalized['time'].max().date().isoformat()}"
        )
        print("normalized tail:")
        print(normalized.tail(5).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Temporary efinance daily history diagnostic.")
    parser.add_argument("--symbol", default="512000.SS")
    parser.add_argument("--beg", default="20260526")
    parser.add_argument("--end", default="20260612")
    parser.add_argument("--fqt", type=int, default=1, help="0=none, 1=qfq, 2=hfq")
    args = parser.parse_args()

    print(f"python={sys.executable}")
    print(f"symbol={args.symbol} code={normalize_symbol(args.symbol)} beg={args.beg} end={args.end} fqt={args.fqt}")
    for clear in (False, True):
        fetch_once(args.symbol, args.beg, args.end, args.fqt, clear_proxy=clear)


if __name__ == "__main__":
    main()
