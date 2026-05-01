#!/usr/bin/env python3
"""Render charts for the paper from bench.csv and a Monitor JSONL log.

Outputs (into ./paper/figures/):
  detection_latency.png     — detection latency vs N, faceted by mode/detector
  rss.png                   — Monitor peak RSS vs N
  state_transitions.png     — timeline of state transitions for one log file

Usage:
  scripts/plot.py --csv bench.csv --log demo-phi.jsonl --outdir paper/figures
"""
import argparse
import csv
import json
import os
from collections import defaultdict
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load_bench(path):
    rows = []
    with open(path) as f:
        header = f.readline().strip().split(",")
        for line in f:
            parts = line.strip().split(",")
            row = dict(zip(header, parts))
            for k in ("N", "peak_rss_kb"):
                row[k] = int(row[k]) if row[k] != "NA" else None
            for k in ("cpu_secs", "detection_latency_ms"):
                row[k] = float(row[k]) if row[k] != "NA" else None
            rows.append(row)
    return rows


def plot_detection_latency(rows, outpath):
    by_combo = defaultdict(list)
    for r in rows:
        if r["detection_latency_ms"] is None:
            continue
        by_combo[(r["mode"], r["detector"])].append((r["N"], r["detection_latency_ms"]))
    plt.figure()
    for (mode, det), pts in sorted(by_combo.items()):
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        plt.plot(xs, ys, marker="o", label=f"{mode}/{det}")
    plt.xscale("log")
    plt.xlabel("number of workers")
    plt.ylabel("detection latency (ms)")
    plt.title("Detection latency vs scale")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def plot_rss(rows, outpath):
    by_mode = defaultdict(list)
    for r in rows:
        if r["peak_rss_kb"] is None:
            continue
        by_mode[r["mode"]].append((r["N"], r["peak_rss_kb"] / 1024.0))
    plt.figure()
    for mode, pts in sorted(by_mode.items()):
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        plt.plot(xs, ys, marker="o", label=mode)
    plt.xscale("log")
    plt.xlabel("number of workers")
    plt.ylabel("Monitor peak RSS (MB)")
    plt.title("Monitor memory footprint vs scale")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def plot_state_transitions(log_path, outpath):
    transitions = []  # (ts, worker, to)
    t0 = None
    with open(log_path) as f:
        for line in f:
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("type") != "state":
                continue
            ts_str = e["ts"].rstrip("Z")
            # Truncate fractional seconds to 6 digits (Python max for %f).
            if "." in ts_str:
                head, frac = ts_str.split(".", 1)
                ts_str = head + "." + frac[:6]
            ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f") if "." in ts_str else datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
            if t0 is None:
                t0 = ts
            transitions.append(((ts - t0).total_seconds(), e["worker"], e["to"]))
    if not transitions:
        return
    workers = sorted({w for _, w, _ in transitions})
    yidx = {w: i for i, w in enumerate(workers)}
    color = {"ALIVE": "tab:green", "MISSING": "tab:orange", "DEAD": "tab:red"}

    plt.figure(figsize=(10, max(2, 0.5 * len(workers))))
    for ts, w, to in transitions:
        plt.scatter(ts, yidx[w], c=color.get(to, "gray"), s=80)
    plt.yticks(range(len(workers)), workers)
    plt.xlabel("seconds since start")
    plt.title("State transitions")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def plot_phi_sweep(csv_path, outpath):
    rows = []
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            r["phi_dead"] = int(r["phi_dead"])
            r["worker4_false_dead_count"] = int(r["worker4_false_dead_count"])
            rows.append(r)

    scenarios = sorted({r["scenario"] for r in rows})
    phis = sorted({r["phi_dead"] for r in rows})

    width = 0.35
    x = list(range(len(phis)))
    plt.figure()
    for i, sc in enumerate(scenarios):
        ys = [next(r["worker4_false_dead_count"] for r in rows
                   if r["phi_dead"] == p and r["scenario"] == sc) for p in phis]
        offsets = [xi + (i - (len(scenarios)-1)/2) * width for xi in x]
        plt.bar(offsets, ys, width=width, label=f"scenario={sc}")
    plt.xticks(x, [str(p) for p in phis])
    plt.xlabel("Φ_dead threshold")
    plt.ylabel("false-positive DEAD count (60s window)")
    plt.title("Phi false-positive rate vs Φ_dead\n(jittery-but-alive worker)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="bench.csv")
    ap.add_argument("--log", default="demo-phi.jsonl")
    ap.add_argument("--phi-csv", default="phi_sweep.csv")
    ap.add_argument("--outdir", default="paper/figures")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    if os.path.exists(args.csv):
        rows = load_bench(args.csv)
        plot_detection_latency(rows, os.path.join(args.outdir, "detection_latency.png"))
        plot_rss(rows, os.path.join(args.outdir, "rss.png"))
    if os.path.exists(args.log):
        plot_state_transitions(args.log, os.path.join(args.outdir, "state_transitions.png"))
    if os.path.exists(args.phi_csv):
        plot_phi_sweep(args.phi_csv, os.path.join(args.outdir, "phi_sweep.png"))
    print("done")


if __name__ == "__main__":
    main()
