# kore-scheduler binds full-core `pin` Pods onto nodes with no whole free cores

**Component:** `pkg/scheduler` (Filter + Score)
**Severity:** Pods stuck in `CreateContainerError` (`KoreAllocationFailed`) even though a viable node exists in the same candidate set.

## TL;DR

The scheduler models per-zone capacity in **logical CPUs** and only checks
that the requested count is *divisible* by threads-per-core
(`AlignFullCore`). It never checks that the free logical CPUs form enough
**whole physical cores** (all SMT siblings free). For a `full-core`
exclusive (`kore.zjusct.io/pin`) request this means:

1. **Filter** wrongly admits a node whose only free CPUs are orphan SMT
   siblings (their partners are occupied), and
2. **Score** (`ScoreFit`, a binpack `100·need/free`) gives that nearly-full
   node the **maximum** score, actively steering the pin Pod onto it —
   over an almost-empty node in the same set.

The agent then applies full-core allocation, finds no whole free core, and
fails at NRI time.

## Environment

- Candidate set = Kueue ResourceFlavor `intels4` (`hpc.zjusct.io/partition=intels4`) = **m700 + m701**. Both amd64, kore-agent healthy, no `agent-down` taint.
- Pod: `pin=true`, `numa-policy=single`, `cpu=2` (integer, `limits==requests`), `schedulerName: kore-scheduler`.

`korenodetopology.status.freeCpus` at the time:

| node | numa0 free | numa1 free | note |
|------|-----------|-----------|------|
| m700 | `70-71` | `94-95` | interactive pool `hpc101-m700` (92c) holds the rest |
| m701 | `0-23,48-71` | `24-47,72-95` | ~96c free |

**Observed:** kore-scheduler picks **m700 every time**; agent NRI fails:
```
KoreAllocationFailed: insufficient free cpus: no single NUMA zone with 2 free cpus
Pod → CreateContainerError
```
Same Pod on m701 pins fine (verified elsewhere: 920b-2 → 60-61, riscv-rv00 → 1-2).

## Root cause (with citations)

For this Pod: `st.need = Σ container CPUs = 2` (`plugin.go:83-86`), policy
`single`, SMT `full-core` (`plugin.go:192`).

The capacity model is **logical-CPU only**:
- `ZoneCap.Free` is a logical `cpuset`; `SMTSiblings` is reduced to a scalar
  `TPC` and the sibling grouping is discarded — `capacity.go:14-34`.
- `TotalFree` / `FitSingle` / `FitPreferred` count `Free.Size()` (logical) —
  `capacity.go:85-120`.
- `AlignFullCore` only checks `need % tpc == 0`; it does **not** verify whole
  cores are free — `capacity.go:132-141`.
- `ScoreFit = 100·need/denom`, `denom = logical free of the chosen zone` —
  `capacity.go:143-168` (binpack: tighter fit ⇒ higher score).
- `Filter` (`plugin.go:192-208`) gates on `AlignFullCore` + `FitSingle`, both
  logical.

### Why m700 is admitted and wins

- **Filter(m700):** `AlignFullCore(need=2)`: `2 % 2 == 0` ✓. `FitSingle(need=2)`:
  numa0 `Free={70,71}` size `2 ≥ 2` ✓ → **admitted** (should be rejected).
- **Score(m700):** default branch → `FitSingle`=numa0, `denom=2`,
  `ScoreFit = 100·2/2 = 100`.
- **Score(m701):** zones have `48` free, `denom=48`,
  `ScoreFit = 100·2/48 = 4`.
- ⇒ m700 (100) beats m701 (4). Deterministic "always m700".

### Why the agent then fails

`{70,71}` are **orphan SMT siblings of two different physical cores** whose
partners sit in the `hpc101-m700` pool. Free logical CPUs = 2, but **whole
free physical cores = 0**. Under `full-core` the allocator needs whole cores
⇒ `insufficient free cpus`.

## Pin counting semantics (the附注, answered)

`need` is in **logical CPUs** (`cpu=2` ⇒ `need=2`). Under `full-core` the
allocator only hands out whole physical cores, so the request must map onto
whole cores. The scheduler validates *divisibility* (`AlignFullCore`) but not
*whole-core availability*. So two free logical CPUs that are orphan siblings
(0 whole cores) pass the scheduler yet fail the agent. (If `{70,71}` had been
the two siblings of a *single* core, that would be 1 whole core = fits — the
bug is that the scheduler can't tell the difference.)

## Suggested fix

For `full-core` (non-`logical`) requests, model per-zone capacity in **whole
physical cores** instead of logical CPUs:

- In `ZonesFromCR`, keep the sibling groups (from `z.SMTSiblings`), not just
  `TPC`. Compute each zone's **free-core set** = physical cores whose *every*
  sibling is in `Free`.
- Route `full-core` requests through free-core counts (`FitSingle` /
  `FitPreferred` / `FitSpread` / `ScoreFit`) in core units (or `cores·tpc`
  logical-equivalent), leaving `logical` requests on the current path.

Result: `Filter(m700)` sees numa0 free-cores `= 0 < 1` ⇒ **Unschedulable** ⇒
the pin falls through to m701; and `ScoreFit` no longer awards 100 to a node
that only has fragmented half-cores.

**Optional quick mitigation** (until the core-aware model lands): make the
agent's allocatability the source of truth by having Filter reject a node
when the free set contains no whole core for a full-core request — a smaller
change than reworking `ScoreFit`, and it at least stops the mis-bind (Pod
retries onto m701) even if scoring still mildly prefers fuller nodes.

## Repro

1. Fill a node's cores via a shared pool so only orphan SMT siblings remain
   free in each NUMA zone (e.g. m700: free `70-71`, `94-95`).
2. Leave a second node in the same partition mostly empty (m701).
3. Submit a `pin=true, numa-policy=single, cpu=2` Pod with
   `schedulerName: kore-scheduler` selecting that partition.
4. Scheduler binds the full node; agent NRI → `KoreAllocationFailed`.
