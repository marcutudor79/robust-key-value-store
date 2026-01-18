#!/usr/bin/env python3

import argparse
import re
from collections import defaultdict

class Operation:
    def __init__(self, pid, seq):
        self.pid = pid
        self.seq = seq
        self.type = None  # 'put' or 'get'
        self.value = None  # for put: written value; for get: returned value
        self.start_ts = None
        self.end_ts = None
        self.start_idx = None
        self.end_idx = None
    def __repr__(self):
        return f"Op(pid={self.pid},seq={self.seq},type={self.type},val={self.value},start={self.start_ts},end={self.end_ts})"

# Regex patterns for log lines (from Process.java logging)
PATTERN_INVOKE = re.compile(r".*p(\d+):\s+Invoke\s+(write|get)\s+start_ts=(\d+)\s+seq=(\d+)")
PATTERN_PUT_DONE = re.compile(r".*p(\d+):\s+Put\s+value:\s+(\d+)\s+operation\s+duration:\s+\d+ns\s+end_ts=(\d+)\s+seq=(\d+)")
PATTERN_GET_DONE = re.compile(r".*p(\d+):\s+Get\s+return\s+value:\s+(\d+)\s+operation\s+duration:\s+\d+ns\s+end_ts=(\d+)\s+seq=(\d+)")

# Fallback patterns without timestamps/seq (best-effort)
PATTERN_INVOKE_FB = re.compile(r".*p(\d+):\s+Invoke\s+(write|get)")
PATTERN_PUT_DONE_FB = re.compile(r".*p(\d+):\s+Put\s+value:\s+(\d+)")
PATTERN_GET_DONE_FB = re.compile(r".*p(\d+):\s+Get\s+return\s+value:\s+(\d+)")


def parse_log(path):
    ops = {}
    ops_by_pid = defaultdict(dict)
    line_idx = 0

    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for raw in f:
            line = raw.strip()
            line_idx += 1

            m = PATTERN_INVOKE.search(line)
            if m:
                pid = int(m.group(1))
                typ = m.group(2)
                start_ts = int(m.group(3))
                seq = int(m.group(4))
                key = (pid, seq)
                op = ops_by_pid[pid].get(seq) or Operation(pid, seq)
                op.type = 'put' if typ == 'write' else 'get'
                op.start_ts = start_ts
                op.start_idx = line_idx
                ops_by_pid[pid][seq] = op
                ops[key] = op
                continue

            m = PATTERN_PUT_DONE.search(line)
            if m:
                pid = int(m.group(1))
                val = int(m.group(2))
                end_ts = int(m.group(3))
                seq = int(m.group(4))
                key = (pid, seq)
                op = ops_by_pid[pid].get(seq) or Operation(pid, seq)
                op.type = 'put'
                op.value = val
                op.end_ts = end_ts
                op.end_idx = line_idx
                ops_by_pid[pid][seq] = op
                ops[key] = op
                continue

            m = PATTERN_GET_DONE.search(line)
            if m:
                pid = int(m.group(1))
                val = int(m.group(2))
                end_ts = int(m.group(3))
                seq = int(m.group(4))
                key = (pid, seq)
                op = ops_by_pid[pid].get(seq) or Operation(pid, seq)
                op.type = 'get'
                op.value = val
                op.end_ts = end_ts
                op.end_idx = line_idx
                ops_by_pid[pid][seq] = op
                ops[key] = op
                continue

            # Fallback parsing without timestamps/seq
            m = PATTERN_INVOKE_FB.search(line)
            if m:
                pid = int(m.group(1))
                typ = m.group(2)
                # Use incrementing sequence per pid as fallback
                seq = max(ops_by_pid[pid].keys(), default=0) + 1
                key = (pid, seq)
                op = Operation(pid, seq)
                op.type = 'put' if typ == 'write' else 'get'
                op.start_idx = line_idx
                ops_by_pid[pid][seq] = op
                ops[key] = op
                continue

            m = PATTERN_PUT_DONE_FB.search(line)
            if m:
                pid = int(m.group(1))
                val = int(m.group(2))
                seq = max(ops_by_pid[pid].keys(), default=0)
                key = (pid, seq)
                op = ops_by_pid[pid].get(seq) or Operation(pid, seq)
                op.type = 'put'
                op.value = val
                op.end_idx = line_idx
                ops_by_pid[pid][seq] = op
                ops[key] = op
                continue

            m = PATTERN_GET_DONE_FB.search(line)
            if m:
                pid = int(m.group(1))
                val = int(m.group(2))
                seq = max(ops_by_pid[pid].keys(), default=0)
                key = (pid, seq)
                op = ops_by_pid[pid].get(seq) or Operation(pid, seq)
                op.type = 'get'
                op.value = val
                op.end_idx = line_idx
                ops_by_pid[pid][seq] = op
                ops[key] = op
                continue

    # Finalize operations list: only complete ones
    complete_ops = [op for op in ops.values() if op.type and op.value is not None and (op.end_ts is not None or op.end_idx is not None) and (op.start_ts is not None or op.start_idx is not None)]
    return complete_ops


def build_precedence(ops):
    # Precedence: op_i -> op_j if end_i <= start_j
    def start(op):
        return op.start_ts if op.start_ts is not None else op.start_idx
    def end(op):
        return op.end_ts if op.end_ts is not None else op.end_idx

    preds = {op: set() for op in ops}
    succs = {op: set() for op in ops}

    for i in range(len(ops)):
        for j in range(len(ops)):
            if i == j:
                continue
            oi, oj = ops[i], ops[j]
            if end(oi) <= start(oj):
                preds[oj].add(oi)
                succs[oi].add(oj)
    return preds, succs


def linearizable(ops, default_value=0):
    preds, succs = build_precedence(ops)

    # Backtracking search
    placed = []
    remaining = set(ops)
    current_value = default_value

    # Cache predecessors count
    pred_count = {op: len(preds[op]) for op in ops}

    # Track dynamic predecessors satisfied as we place ops
    def can_place(op):
        # All predecessors must be placed
        return all(p in placed for p in preds[op])

    def search(current_value):
        if not remaining:
            return True, placed

        # Choose next candidates with no unsatisfied predecessors
        candidates = [op for op in list(remaining) if can_place(op)]
        # Simple heuristic: try puts before gets to satisfy more gets
        candidates.sort(key=lambda op: 0 if op.type == 'put' else 1)

        for op in candidates:
            # Semantics check
            if op.type == 'get':
                if op.value != current_value:
                    continue  # cannot place here
                # Place get
                placed.append(op)
                remaining.remove(op)
                ok, seq = search(current_value)
                if ok:
                    return True, seq
                # backtrack
                remaining.add(op)
                placed.pop()
            else:  # put
                prev = current_value
                placed.append(op)
                remaining.remove(op)
                ok, seq = search(op.value)
                if ok:
                    return True, seq
                # backtrack
                remaining.add(op)
                placed.pop()
                current_value = prev
        return False, None

    ok, seq = search(current_value)
    return ok, seq


def main():
    parser = argparse.ArgumentParser(description="Linearizability checker for single-key MWMR register histories (N=3,M=3). Parses AKKA logs and validates linearizability.")
    parser.add_argument('--log', required=True, help='Path to log file produced by running the Java actors')
    parser.add_argument('--default', type=int, default=0, help='Default value for missing key (implementation uses 0)')
    args = parser.parse_args()

    ops = parse_log(args.log)
    if not ops:
        print("No complete operations parsed. Ensure logging includes seq/start_ts/end_ts or use fallback.")
        return

    ok, seq = linearizable(ops, default_value=args.default)
    if ok:
        print("Linearizable: YES")
        # Print a simple sequentialization order
        print("Sequential order (pid#seq type val):")
        for op in seq:
            print(f"p{op.pid}#{op.seq} {op.type} {op.value}")
    else:
        print("Linearizable: NO")

if __name__ == '__main__':
    main()
