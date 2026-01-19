#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
from datetime import datetime
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
# Accept both 'get' and 'read' for read operations
PATTERN_INVOKE = re.compile(r".*p(\d+):\s+Invoke\s+(write|get|read)\s+start_ts=(\d+)\s+seq=(\d+)")
PATTERN_PUT_DONE = re.compile(r".*p(\d+):\s+Put\s+value:\s+(\d+)\s+operation\s+duration:\s+\d+ns\s+end_ts=(\d+)\s+seq=(\d+)")
PATTERN_GET_DONE = re.compile(r".*p(\d+):\s+Get\s+return\s+value:\s+(\d+)\s+operation\s+duration:\s+\d+ns\s+end_ts=(\d+)\s+seq=(\d+)")

# Fallback patterns without timestamps/seq (best-effort)
PATTERN_INVOKE_FB = re.compile(r".*p(\d+):\s+Invoke\s+(write|get|read)")
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
                # Try to match by exact seq, then seq-1 (Java logs completion with seq after increment)
                op = ops_by_pid[pid].get(seq)
                if not op and seq - 1 >= 0:
                    op = ops_by_pid[pid].get(seq - 1)
                if not op:
                    candidates = [o for o in ops_by_pid[pid].values() if o.type == 'put' and o.end_ts is None and o.end_idx is None]
                    if candidates:
                        op = sorted(candidates, key=lambda o: (o.start_ts if o.start_ts is not None else -1, o.start_idx if o.start_idx is not None else -1))[-1]
                    else:
                        # Create with presumed seq-1 to align with invoke-if present later
                        op = Operation(pid, max(seq - 1, 0))
                op.type = 'put'
                op.value = val
                op.end_ts = end_ts
                op.end_idx = line_idx
                # Persist under the op's own sequence to avoid aliasing
                ops_by_pid[pid][op.seq] = op
                ops[(pid, op.seq)] = op
                continue

            m = PATTERN_GET_DONE.search(line)
            if m:
                pid = int(m.group(1))
                val = int(m.group(2))
                end_ts = int(m.group(3))
                seq = int(m.group(4))
                # Try to match by exact seq, then seq-1
                op = ops_by_pid[pid].get(seq)
                if not op and seq - 1 >= 0:
                    op = ops_by_pid[pid].get(seq - 1)
                if not op:
                    candidates = [o for o in ops_by_pid[pid].values() if o.type == 'get' and o.end_ts is None and o.end_idx is None]
                    if candidates:
                        op = sorted(candidates, key=lambda o: (o.start_ts if o.start_ts is not None else -1, o.start_idx if o.start_idx is not None else -1))[-1]
                    else:
                        op = Operation(pid, max(seq - 1, 0))
                op.type = 'get'
                op.value = val
                op.end_ts = end_ts
                op.end_idx = line_idx
                ops_by_pid[pid][op.seq] = op
                ops[(pid, op.seq)] = op
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


def repo_root_from_this_script():
    # tools/linearizability_checker.py -> repo root is two levels up
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_java_and_capture(n, faults, m, project_dir, capture_path, kvlog_path=None):
    compile_cmd = ['mvn', '-q', 'clean', 'compile']
    exec_cmd = [
        'mvn', '-q', 'exec:java',
        f'-Dexec.mainClass=keyValueStore.Main',
        f'-Dexec.args={n} {faults} {m}'
    ]
    # Prefer environment variable to avoid quoting issues with spaces
    # Ensure parent directory exists for capture
    parent = os.path.dirname(capture_path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

    # Run and capture stdout
    try:
        # Ensure we compile successfully before running
        env = os.environ.copy()
        if kvlog_path:
            env['KV_LOG'] = kvlog_path
        subprocess.run(compile_cmd, cwd=project_dir, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        res = subprocess.run(
            exec_cmd,
            cwd=project_dir,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
    except FileNotFoundError:
        raise RuntimeError("'mvn' not found. Please install Maven or add it to PATH.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Maven run failed with exit code {e.returncode}. Output:\n{e.stdout}")

    # Write captured output
    with open(capture_path, 'w', encoding='utf-8') as fh:
        ts = datetime.now().isoformat()
        fh.write(f"# Capture created: {ts} N={n} f={faults} M={m}\n")
        fh.write(res.stdout)
    return capture_path


def generate_sample_log(path):
    # Ensure parent directory exists
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

    # Create a simple, valid, linearizable history matching the regex patterns
    # Two writes and two reads with clear non-overlapping timestamps
    lines = [
        # p1 writes 1
        "p1: Invoke write start_ts=100 seq=1",
        "p1: Put value: 1 operation duration: 50ns end_ts=150 seq=1",
        # p2 reads 1 after p1's write
        "p2: Invoke get start_ts=200 seq=1",
        "p2: Get return value: 1 operation duration: 10ns end_ts=210 seq=1",
        # p3 writes 2
        "p3: Invoke write start_ts=220 seq=1",
        "p3: Put value: 2 operation duration: 30ns end_ts=250 seq=1",
        # p2 reads 2 after p3's write
        "p2: Invoke get start_ts=260 seq=2",
        "p2: Get return value: 2 operation duration: 10ns end_ts=270 seq=2",
    ]

    with open(path, 'w', encoding='utf-8') as f:
        for ln in lines:
            f.write(ln + "\n")
    return path


def build_precedence(ops):
    # Deduplicate operations to avoid self-edges caused by duplicate references
    seen = set()
    unique_ops = []
    for o in ops:
        if id(o) in seen:
            continue
        seen.add(id(o))
        unique_ops.append(o)

    # Precedence: op_i -> op_j if end_i <= start_j
    def start(op):
        return op.start_ts if op.start_ts is not None else op.start_idx
    def end(op):
        return op.end_ts if op.end_ts is not None else op.end_idx

    preds = {op: set() for op in unique_ops}
    succs = {op: set() for op in unique_ops}

    for i in range(len(unique_ops)):
        for j in range(len(unique_ops)):
            oi, oj = unique_ops[i], unique_ops[j]
            # Skip exact same object to avoid self-loops from duplicates
            if oi is oj:
                continue
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


def fast_linearizable(ops, default_value=0):
    # Scalable check for single-register histories: for each get r
    # - Let prev_max be the max value among writes completing before r.start
    #   If prev_max > r.value -> violation
    # - Ensure there exists at least one write of r.value completing before r.end
    #   If not -> violation
    def start(op):
        return op.start_ts if op.start_ts is not None else op.start_idx
    def end(op):
        return op.end_ts if op.end_ts is not None else op.end_idx

    # Dedup
    uniq = []
    seen = set()
    for o in ops:
        i = id(o)
        if i in seen:
            continue
        seen.add(i)
        uniq.append(o)

    writes = [o for o in uniq if o.type == 'put']
    reads = [o for o in uniq if o.type == 'get']
    # Sort writes by end time
    writes_sorted = sorted(writes, key=lambda o: end(o))

    # Prefix maxima of write values by end time
    ends = [end(w) for w in writes_sorted]
    vals = [w.value for w in writes_sorted]
    pref_max = []
    mx = default_value
    for v in vals:
        mx = max(mx, v)
        pref_max.append(mx)

    # Helper: max write value with end <= t
    def max_before(t):
        # binary search on ends
        lo, hi = 0, len(ends) - 1
        pos = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if ends[mid] <= t:
                pos = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return pref_max[pos] if pos >= 0 else default_value

    # Helper: exists write of value v with end <= t
    # Build map of value -> sorted list of end times
    from collections import defaultdict as _dd
    val_to_ends = _dd(list)
    for w in writes_sorted:
        val_to_ends[w.value].append(end(w))
    for k in val_to_ends:
        val_to_ends[k].sort()

    def exists_write_val_before(v, t):
        arr = val_to_ends.get(v)
        if not arr:
            return False
        # binary search for any end <= t
        lo, hi = 0, len(arr) - 1
        pos = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if arr[mid] <= t:
                pos = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return pos >= 0

    # Validate all reads
    for r in reads:
        prev = max_before(start(r))
        if prev > r.value:
            return False, None
        if r.value == default_value:
            # OK if no higher write completed before its end
            # i.e., max_before(end(r)) == default_value
            if max_before(end(r)) > default_value:
                return False, None
        else:
            if not exists_write_val_before(r.value, end(r)):
                return False, None
    # If all reads pass, accept and return a topo-like order (puts then gets)
    order = sorted(writes, key=lambda o: end(o)) + sorted(reads, key=lambda o: end(o))
    return True, order


def main():
    parser = argparse.ArgumentParser(description="Linearizability checker for single-key MWMR register histories (N=3,M=3). Parses AKKA logs and validates linearizability.")
    parser.add_argument('--log', required=False, help='Path to log file produced by running the Java actors. If missing, a sample log is generated unless --run-java is used.')
    parser.add_argument('--default', type=int, default=0, help='Default value for missing key (implementation uses 0)')
    parser.add_argument('--out', type=str, default=None, help='Optional path to write linearizability result summary.')
    parser.add_argument('--debug', action='store_true', help='Print parsed operations and a simple diagnostic when non-linearizable.')
    parser.add_argument('--run-java', action='store_true', help='Run the Java program (keyValueStore.Main) and capture output to a log file before checking linearizability.')
    parser.add_argument('-N', '--n', type=int, default=3, help='Number of processes N when running Java (default 3).')
    parser.add_argument('-f', '--faults', type=int, default=1, help='Max faulty processes f when running Java (default 1).')
    parser.add_argument('-M', '--m', type=int, default=3, help='Operations per process M when running Java (default 3).')
    parser.add_argument('--project-dir', type=str, default=None, help='Path to Maven project directory containing pom.xml (default: <repo_root>/code/project).')
    parser.add_argument('--capture', type=str, default=None, help='Path to write captured Java stdout (default: <repo_root>/logs_N< N >_M< M >.txt when --run-java).')
    parser.add_argument('--kvlog', type=str, default=None, help='If provided, pass -Dkv.log to Java and parse this file instead of stdout.')
    args = parser.parse_args()

    if args.run_java:
        repo_root = repo_root_from_this_script()
        project_dir = args.project_dir or os.path.join(repo_root, 'code', 'project')
        if not os.path.exists(os.path.join(project_dir, 'pom.xml')):
            raise SystemExit(f"Could not find pom.xml in project dir: {project_dir}")
        capture_path = args.capture or os.path.join(repo_root, f"logs_N{args.n}_M{args.m}.txt")
        kvlog_path = None
        if args.kvlog:
            kvlog_path = os.path.join(repo_root, args.kvlog) if not os.path.isabs(args.kvlog) else args.kvlog
        print(f"Running Java in {project_dir} (N={args.n}, f={args.faults}, M={args.m}) and capturing to {capture_path}{' with kvlog '+kvlog_path if kvlog_path else ''} ...")
        run_java_and_capture(args.n, args.faults, args.m, project_dir, capture_path, kvlog_path=kvlog_path)
        # Prefer kvlog as source if provided
        args.log = kvlog_path or capture_path

    if not args.log:
        repo_root = repo_root_from_this_script()
        sample_path = os.path.join(repo_root, 'logs_sample.txt')
        print(f"No --log provided. Generating a sample log at '{sample_path}'...")
        generate_sample_log(sample_path)
        print(f"Sample log created at '{sample_path}'. Proceeding with check.")
        args.log = sample_path

    if not os.path.exists(args.log):
        print(f"Log file not found at '{args.log}'. Generating a sample log...")
        generate_sample_log(args.log)
        print(f"Sample log created at '{args.log}'. Proceeding with check.")

    ops = parse_log(args.log)
    if not ops:
        print("No complete operations parsed. Ensure logging includes seq/start_ts/end_ts or use fallback.")
        return

    if args.debug:
        print(f"Parsed {len(ops)} complete operations")
        by_pid = defaultdict(int)
        for op in ops:
            by_pid[op.pid] += 1
        print("Ops per pid:", dict(sorted(by_pid.items())))
        # Show first few ops windows
        def s(o):
            return o.start_ts if o.start_ts is not None else o.start_idx
        def e(o):
            return o.end_ts if o.end_ts is not None else o.end_idx
        for op in ops[:10]:
            print(f"p{op.pid}#{op.seq} {op.type} val={op.value} [{s(op)}..{e(op)}]")

    mode = 'exact'
    if args.debug:
        mode = 'exact'
    # Auto-switch to fast for large histories
    if len(ops) > 300:
        mode = 'fast'
    try:
        ok, seq = (linearizable if mode == 'exact' else fast_linearizable)(ops, default_value=args.default)
    except RecursionError:
        ok, seq = fast_linearizable(ops, default_value=args.default)
        mode = 'fast'

    lines = []
    if ok:
        lines.append("Linearizable: YES")
        lines.append("Sequential order (pid#seq type val):")
        for op in seq:
            lines.append(f"p{op.pid}#{op.seq} {op.type} {op.value}")
    else:
        lines.append("Linearizable: NO")
        lines.append(f"Mode used: {mode}")
        if args.debug:
            # Very simple diagnostic: show gets that return a value not achievable from any prefix respecting precedence
            preds, _ = build_precedence(ops)
            placed = set()
            current_value = args.default
            progress = True
            while progress:
                progress = False
                # Place any put whose predecessors are all placed
                puts = [o for o in ops if o.type == 'put' and o not in placed and all(p in placed for p in preds[o])]
                if puts:
                    # choose earliest end time to advance state
                    pnext = sorted(puts, key=lambda o: (o.end_ts if o.end_ts is not None else o.end_idx))[0]
                    placed.add(pnext)
                    current_value = pnext.value
                    progress = True
                    continue
                # If only gets remain with no preds, check mismatches
                gets = [o for o in ops if o.type == 'get' and o not in placed and all(p in placed for p in preds[o])]
                mismatches = [g for g in gets if g.value != current_value]
                if mismatches:
                    g = sorted(mismatches, key=lambda o: (o.end_ts if o.end_ts is not None else o.end_idx))[0]
                    lines.append(f"Diagnostic: get p{g.pid}#{g.seq} returns {g.value} but current_value={current_value} at its placement window.")
                break

    for ln in lines:
        print(ln)

    if args.out:
        out_dir = os.path.dirname(args.out)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines) + "\n")

if __name__ == '__main__':
    main()
