"""
Build a clean India-focused 20 GB dataset bundle for this project.

What it does:
1. Copies the existing India dataset into data/india_20gb/existing_india
2. Downloads additional Indian market stocks into data/india_20gb/new_downloads_raw
3. Creates controlled augmented variants until the bundle reaches the target size
4. Writes a manifest and summary report for later inspection
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parent
SOURCE_INDIA_DIR = ROOT / "data" / "india"
TARGET_ROOT = ROOT / "data" / "india_20gb"
EXISTING_COPY_DIR = TARGET_ROOT / "existing_india"
NEW_DOWNLOAD_DIR = TARGET_ROOT / "new_downloads_raw"
AUGMENTED_DIR = TARGET_ROOT / "augmented"
MANIFEST_PATH = TARGET_ROOT / "manifest.csv"
SUMMARY_PATH = TARGET_ROOT / "summary.json"

TARGET_SIZE_GB = 20.0
TARGET_SIZE_BYTES = int(TARGET_SIZE_GB * (1024 ** 3))
DOWNLOAD_START = "2010-01-01"

# A broad India-focused list. Some symbols may be skipped if Yahoo has no data.
EXPANDED_INDIAN_STOCKS = [
    "ABB.NS", "ACC.NS", "ADANIENT.NS", "ADANIPORTS.NS", "ALKEM.NS",
    "AMBUJACEM.NS", "APOLLOHOSP.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "AUBANK.NS",
    "AUROPHARMA.NS", "AXISBANK.NS", "BAJAJ-AUTO.NS", "BAJAJFINSV.NS", "BAJFINANCE.NS",
    "BALKRISIND.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BEL.NS", "BHARATFORG.NS",
    "BHARTIARTL.NS", "BHEL.NS", "BIOCON.NS", "BOSCHLTD.NS", "BPCL.NS",
    "BRITANNIA.NS", "CANBK.NS", "CHOLAFIN.NS", "CIPLA.NS", "COALINDIA.NS",
    "COFORGE.NS", "COLPAL.NS", "CONCOR.NS", "CROMPTON.NS", "CUMMINSIND.NS",
    "DABUR.NS", "DALBHARAT.NS", "DEEPAKNTR.NS", "DIVISLAB.NS", "DIXON.NS",
    "DLF.NS", "DRREDDY.NS", "ESCORTS.NS", "FEDERALBNK.NS", "GAIL.NS",
    "GLENMARK.NS", "GODREJCP.NS", "GODREJPROP.NS", "GRASIM.NS", "HAVELLS.NS",
    "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HEROMOTOCO.NS", "HINDALCO.NS",
    "HINDPETRO.NS", "HINDUNILVR.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS",
    "IDEA.NS", "IDFCFIRSTB.NS", "IEX.NS", "INDHOTEL.NS", "INDIGO.NS",
    "INDUSINDBK.NS", "INFY.NS", "IOC.NS", "IRCTC.NS", "ITC.NS",
    "JINDALSTEL.NS", "JKCEMENT.NS", "JSWSTEEL.NS", "JUBLFOOD.NS", "KOTAKBANK.NS",
    "LALPATHLAB.NS", "LAURUSLABS.NS", "LICHSGFIN.NS", "LT.NS", "LTIM.NS",
    "LTTS.NS", "LUPIN.NS", "M&M.NS", "MANKIND.NS", "MARICO.NS",
    "MARUTI.NS", "MAXHEALTH.NS", "MCDOWELL-N.NS", "MOTHERSON.NS", "MPHASIS.NS",
    "MRF.NS", "NAUKRI.NS", "NESTLEIND.NS", "NHPC.NS", "NMDC.NS",
    "NTPC.NS", "OBEROIRLTY.NS", "OFSS.NS", "ONGC.NS", "PAGEIND.NS",
    "PEL.NS", "PERSISTENT.NS", "PETRONET.NS", "PIDILITIND.NS", "PIIND.NS",
    "POLYCAB.NS", "POWERGRID.NS", "PVRINOX.NS", "RAMCOCEM.NS", "RECLTD.NS",
    "RELIANCE.NS", "SAIL.NS", "SBICARD.NS", "SBILIFE.NS", "SBIN.NS",
    "SHREECEM.NS", "SIEMENS.NS", "SRF.NS", "SUNPHARMA.NS", "SUPREMEIND.NS",
    "TATACONSUM.NS", "TATAMOTORS.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TCS.NS",
    "TECHM.NS", "TITAN.NS", "TORNTPHARM.NS", "TRENT.NS", "TVSMOTOR.NS",
    "ULTRACEMCO.NS", "UNITDSPR.NS", "UPL.NS", "VBL.NS", "VEDL.NS",
    "VOLTAS.NS", "WIPRO.NS", "ZOMATO.NS", "ZYDUSLIFE.NS",
]


@dataclass
class ManifestRow:
    category: str
    symbol: str
    file_path: str
    rows: int
    size_bytes: int
    source: str
    parent: str


def file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def parse_symbol_from_name(path: Path) -> str:
    name = path.stem
    name = name.split("_dup")[0]
    name = name.split("_aug")[0]
    name = name.replace("_raw", "")
    return name.replace("_", ".")


def existing_files() -> list[Path]:
    if not SOURCE_INDIA_DIR.exists():
        return []
    return sorted(p for p in SOURCE_INDIA_DIR.glob("*.csv") if p.is_file())


def copy_existing_dataset() -> list[Path]:
    EXISTING_COPY_DIR.mkdir(parents=True, exist_ok=True)
    copied = []
    for src in existing_files():
        dest = EXISTING_COPY_DIR / src.name
        if not dest.exists() or src.stat().st_mtime > dest.stat().st_mtime:
            shutil.copy2(src, dest)
        copied.append(dest)
    return copied


def existing_symbol_set(paths: Iterable[Path]) -> set[str]:
    return {parse_symbol_from_name(path) for path in paths}


def normalize_downloaded_frame(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    needed = ["Date", "Open", "High", "Low", "Close", "Volume"]
    if "Adj Close" in df.columns:
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]
    else:
        df = df[[col for col in needed if col in df.columns]]

    if list(df.columns) != needed:
        raise ValueError(f"Unexpected columns: {df.columns.tolist()}")

    return df.reset_index(drop=True)


def download_missing_symbols(existing_symbols: set[str]) -> list[Path]:
    NEW_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    downloaded_paths = []
    seen = set(existing_symbols)

    for symbol in EXPANDED_INDIAN_STOCKS:
        if symbol in seen:
            continue

        dest = NEW_DOWNLOAD_DIR / f"{symbol.replace('.', '_')}_raw.csv"
        if dest.exists():
            downloaded_paths.append(dest)
            seen.add(symbol)
            continue

        try:
            data = yf.download(symbol, start=DOWNLOAD_START, progress=False, auto_adjust=False)
            if data is None or data.empty:
                continue

            normalized = normalize_downloaded_frame(data.reset_index())
            if len(normalized) < 200:
                continue

            normalized.to_csv(dest, index=False)
            downloaded_paths.append(dest)
            seen.add(symbol)
            print(f"Downloaded {symbol} -> {len(normalized)} rows")
        except Exception as exc:
            print(f"Skipped {symbol}: {str(exc)[:80]}")

    return downloaded_paths


def stable_seed(text: str) -> int:
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:8], 16)


def augment_frame(df: pd.DataFrame, variant_index: int, source_name: str) -> pd.DataFrame:
    out = df.copy()
    seed = stable_seed(f"{source_name}:{variant_index}")
    rng = np.random.default_rng(seed)

    price_cols = [col for col in ["Open", "High", "Low", "Close"] if col in out.columns]
    if not price_cols:
        return out

    base_scale = 0.93 + (variant_index % 13) * 0.012
    wave = np.sin(np.linspace(0, math.pi * 4, len(out))) * (0.0025 + (variant_index % 5) * 0.0007)
    jitter = rng.normal(0, 0.0015 + (variant_index % 7) * 0.00025, len(out))
    multiplier = np.clip(base_scale + wave + jitter, 0.65, 1.45)

    for col in price_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce") * multiplier

    if "Volume" in out.columns:
        volume_scale = 0.82 + (variant_index % 9) * 0.045
        volume_noise = np.abs(1 + rng.normal(0, 0.04, len(out)))
        out["Volume"] = (
            pd.to_numeric(out["Volume"], errors="coerce").fillna(0)
            * volume_scale
            * volume_noise
        ).round()

    if all(col in out.columns for col in ["Open", "Close"]):
        high_floor = out[["Open", "Close"]].max(axis=1)
        low_ceiling = out[["Open", "Close"]].min(axis=1)
        if "High" in out.columns:
            out["High"] = np.maximum(pd.to_numeric(out["High"], errors="coerce"), high_floor)
        if "Low" in out.columns:
            out["Low"] = np.minimum(pd.to_numeric(out["Low"], errors="coerce"), low_ceiling)

    return out


def csv_row_count(path: Path) -> int:
    try:
        return max(sum(1 for _ in path.open("r", encoding="utf-8", errors="ignore")) - 1, 0)
    except OSError:
        return 0


def gather_manifest_rows() -> list[ManifestRow]:
    rows: list[ManifestRow] = []
    for category, directory, source_name in [
        ("existing_copy", EXISTING_COPY_DIR, "existing_project_india"),
        ("downloaded_raw", NEW_DOWNLOAD_DIR, "yfinance"),
        ("augmented", AUGMENTED_DIR, "generated"),
    ]:
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*.csv")):
            rows.append(
                ManifestRow(
                    category=category,
                    symbol=parse_symbol_from_name(path),
                    file_path=str(path.relative_to(ROOT)),
                    rows=csv_row_count(path),
                    size_bytes=file_size(path),
                    source=source_name,
                    parent=path.stem.split("_aug")[0] if "_aug" in path.stem else "",
                )
            )
    return rows


def write_manifest() -> list[ManifestRow]:
    manifest_rows = gather_manifest_rows()
    frame = pd.DataFrame([row.__dict__ for row in manifest_rows])
    frame.to_csv(MANIFEST_PATH, index=False)
    return manifest_rows


def write_summary(manifest_rows: list[ManifestRow]) -> None:
    summary = {
        "generated_at": datetime.now().isoformat(),
        "target_size_gb": TARGET_SIZE_GB,
        "actual_size_gb": round(directory_size(TARGET_ROOT) / (1024 ** 3), 3),
        "total_files": len(manifest_rows),
        "existing_copy_files": sum(1 for row in manifest_rows if row.category == "existing_copy"),
        "downloaded_raw_files": sum(1 for row in manifest_rows if row.category == "downloaded_raw"),
        "augmented_files": sum(1 for row in manifest_rows if row.category == "augmented"),
        "unique_symbols": len({row.symbol for row in manifest_rows}),
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def create_augmented_bundle(seed_files: list[Path]) -> int:
    AUGMENTED_DIR.mkdir(parents=True, exist_ok=True)
    current_size = directory_size(TARGET_ROOT)
    if current_size >= TARGET_SIZE_BYTES:
        return 0

    created = 0
    seed_files = [path for path in seed_files if path.exists()]
    seed_files.sort(key=file_size, reverse=True)

    variant_index = 1
    while current_size < TARGET_SIZE_BYTES and seed_files:
        for seed_path in seed_files:
            if current_size >= TARGET_SIZE_BYTES:
                break

            try:
                df = pd.read_csv(seed_path)
                augmented = augment_frame(df, variant_index, seed_path.name)
                out_name = f"{seed_path.stem}_aug{variant_index:05d}.csv"
                out_path = AUGMENTED_DIR / out_name
                augmented.to_csv(out_path, index=False)
                current_size += file_size(out_path)
                created += 1
                variant_index += 1

                if created % 100 == 0:
                    size_gb = current_size / (1024 ** 3)
                    print(f"Augmented files created: {created} | bundle size: {size_gb:.2f} GB")
            except Exception as exc:
                print(f"Augmentation skipped for {seed_path.name}: {str(exc)[:80]}")

    return created


def build_dataset() -> None:
    TARGET_ROOT.mkdir(parents=True, exist_ok=True)

    copied = copy_existing_dataset()
    current_symbols = existing_symbol_set(copied)
    downloaded = download_missing_symbols(current_symbols)

    seed_files = copied + downloaded
    created = create_augmented_bundle(seed_files)
    manifest_rows = write_manifest()
    write_summary(manifest_rows)

    total_size_gb = directory_size(TARGET_ROOT) / (1024 ** 3)
    print("\n" + "=" * 90)
    print("INDIA 20 GB DATASET BUILD COMPLETE")
    print("=" * 90)
    print(f"Bundle location : {TARGET_ROOT}")
    print(f"Total size      : {total_size_gb:.2f} GB")
    print(f"Existing copied : {len(copied)} files")
    print(f"New downloads   : {len(downloaded)} files")
    print(f"Augmented files : {created}")
    print(f"Manifest        : {MANIFEST_PATH}")
    print(f"Summary         : {SUMMARY_PATH}")


if __name__ == "__main__":
    build_dataset()
