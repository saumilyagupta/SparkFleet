# Advanced demonstrations

These four extras go beyond the assignment. Each one answers a question an evaluator is likely to ask. Every result quoted here comes from a real run on our 4 lab machines. The raw files are in `results/`, and copies are kept in `docs/evidence/`.

| Demo | The question it answers | Command |
|------|------------------------|---------|
| History Server + live load | "Can you prove what ran, and show the cluster working?" | `clusterctl tunnel` → http://localhost:19080, dashboard **Live load** |
| Scaling benchmark | "Why use a cluster at all? How well does it scale?" | `clusterctl bench` |
| Advanced fault scenarios | "What else can Spark survive besides losing one worker?" | `clusterctl demo shuffle`, `demo straggler`, `demo cascade` |
| Master high availability | "What if the *master* dies? Isn't that a single point of failure?" | `clusterctl demo master-failover` |

---

## 1. History Server and live load

**What it is**
- The Spark History Server runs on the master as the systemd service `spark-history`. It rebuilds the full Spark UI for finished applications from the event logs the driver writes to `/opt/spark-data/events`.
- The dashboard's **Live load** panel samples every machine every 3 seconds through SSH, reading `/proc/stat`, `/proc/meminfo` and `/proc/loadavg`. It plots each machine's CPU next to the number of cores Spark is using on it, taken from the master's `/json/`.

**What to show**
1. Run `clusterctl tunnel` and open http://localhost:19080. Every application that ever ran is listed, including the failure demos.
2. Open a failure run, go to **Stages**, and show the failed and retried task attempts in the task table and the event timeline.
3. Run Word Count from the dashboard. In **Live load**, all 3 workers jump to 24 Spark cores and their CPU rises, then both drop back to idle when the job ends.

**What to say**
- *"The live Spark UI on port 4040 disappears when a job ends. The History Server keeps every run auditable afterwards."*
- *"The load charts are our own monitoring. They come from the kernel's counters on each machine."*

---

## 2. Scaling benchmark

**What it does**
- `clusterctl bench` runs Word Count on inputs of 100, 500, 1000 and 2000 MB.
  - Each input is generated **on the master** with a fixed seed, then copied to the workers over the LAN.
- For each size it uses 1, 2 and 3 workers, with 3 repeats each.
- The number of workers is chosen without stopping any service: `--executor-cores 24 --total-executor-cores 24·k` places exactly k executors on k machines.
- For each combination it records the median time, then computes:
  - speedup S(k) = T(1)/T(k)
  - efficiency S(k)/k
  - throughput in MB/s
- Every run's output is checked against a single-machine count of the same file.

**Results**

Measured on 27 Nov 2026: 36 runs, all matching the single-machine count (`docs/results/bench-20261127-020353.json`):

| Input | 1 worker | 2 workers | 3 workers | Speedup (3 w) | Efficiency |
|-------|---------|-----------|-----------|---------------|-----------|
| 100 MB | 9.1 s | 10.0 s | 8.7 s | 1.05× | 35% |
| 500 MB | 13.2 s | 12.2 s | 9.7 s | 1.36× | 45% |
| 1000 MB | 22.5 s | 17.5 s | 11.1 s | 2.03× | 68% |
| 2000 MB | 39.0 s | 28.1 s | 17.1 s | **2.28×** | **76%** |

**What to say**
- **Amdahl's law:** *"A Spark job has a fixed cost: starting executors, scheduling, the shuffle and collecting results. That cost doesn't shrink when you add machines. On 100 MB the work is so small that the fixed cost dominates, so 3 machines are barely faster than 1. On 2 GB the parallel part dominates, so speedup approaches the ideal."*
- *"Efficiency below 100% is the price of coordination and of the shuffle crossing the network."*

---

## 3. Advanced fault scenarios

Each scenario:
- checks the final answer byte for byte against a single-machine count,
- puts the cluster back the way it was,
- writes `results/demo-<scenario>-<stamp>.md` and `.log`.

### 3a. `demo shuffle`: losing a worker *during the shuffle*

**What happens**
1. The map stage finishes, and each executor keeps its shuffle files on its own local disk.
2. While the reduce stage is reading those files, we kill a worker.
3. Its shuffle files are now gone, so reduce tasks that need them fail with `FetchFailed`.
4. Spark's DAGScheduler does not just retry the reduce task. It **resubmits the earlier map stage**, but only for the map partitions that were lost. It recomputes them from the input file using lineage, and then the reduce stage continues.

**Proof:** the log contains `FetchFailed`, `Resubmitting ShuffleMapStage 0 … due to fetch failure` and `Resubmitted ShuffleMapTask`, and the result is identical.

**Measured:** system-02 was killed 0.7 s after the reduce stage began. That produced 14 `FetchFailed` lines, all for one lost map output, and 1 `Resubmitting ShuffleMapStage 0`. The job finished in 51 s with an identical result (`docs/results/demo-shuffle-20261127-022456.md`).

**What to say:** *"Task retry isn't enough here, because the data the task needs is gone. Spark walks the lineage back across the stage boundary."*

### 3b. `demo straggler`: speculative execution

**What happens**
1. We run with `spark.speculation=true` on a 2 GB input, with CPU-heavy tasks.
2. Once tasks are running, we throttle one worker to **one CPU core** with a real Linux cgroup quota: `systemctl set-property --runtime spark-worker CPUQuota=100%`. Its 24 tasks now share one core, so that worker is a genuine straggler. We don't fake it with sleeps.
3. Spark notices those tasks are much slower than the median and marks them **speculatable**.
4. It launches copies on the healthy workers. Whichever copy finishes first wins, and the other is killed.
5. The quota is always removed at the end, inside a `finally` block.

**Proof:** the log contains `Marking task … as speculatable` and `Killing attempt 0 … as the attempt 1 succeeded on …`.

**Measured:** on 2 GB split into 432 partitions, system-02 was throttled at +3.6 s. Spark marked **97 tasks as speculatable**, and **64 speculative copies won**. The job took 36.9 s, against 17.1 s on a healthy cluster. The result was identical, and the throttle was removed (`docs/results/demo-straggler-20261127-022619.md`). Its "lost tasks" count is the losing duplicate copies that Spark killed; they are not failures.

**What to say:** *"Fault tolerance isn't only about crashes. A slow node, from a failing disk or a noisy neighbour, would hold the whole job back. Speculation hides it."*

### 3c. `demo cascade`: two failures

**What happens:** we kill one busy worker, then a second one 20 seconds later. The job finishes on the last machine alone, and the answer is identical. Afterwards both workers restart and rejoin.

**Measured:** system-02 was stopped at +5.7 s and system-03 at +28.8 s. There were 2 lost executors and 48 lost tasks. 48 completed map tasks were resubmitted because their outputs had died with the machines. The job finished on system-04 alone at +111 s, with an identical result (`docs/results/demo-cascade-20261127-022713.md`).

---

## 4. Master high availability

**The problem:** in standalone mode with one master, the master is the cluster's **single point of failure**. Running jobs keep going if it dies, but no new application or worker can register.

**Our fix**
- A **ZooKeeper** ensemble runs on system-02, 03 and 04. It is 3 nodes, so it keeps a majority even if one of them is lost.
- A **standby master** runs on system-02.
- Both masters run with `spark.deploy.recoveryMode=ZOOKEEPER`. ZooKeeper elects one master **ALIVE**, and the other stays **STANDBY**.
- Workers and drivers are given *both* masters: `spark://172.22.68.221:7077,172.22.68.216:7077`.

**What happens in `demo master-failover`**
1. Start a Word Count.
2. `systemctl stop spark-master` on system-01, the primary.
3. ZooKeeper's session for system-01 expires, and the standby on system-02 wins the election. It recovers the list of workers and applications from ZooKeeper and becomes **ALIVE**.
4. The workers and the running application re-register with the new master. The job finishes, and the result is identical.
5. system-01 is restarted and comes back as **STANDBY**.

`clusterctl ha` shows each master's role and each ZooKeeper node's mode (leader or follower).

**Measured:** system-02 (ALIVE) was stopped at +18.9 s. ZooKeeper elected **system-01, which was ALIVE 33 s later while the job was still running**. The job finished under the new master at +144 s. A brand-new application then ran on it at +157 s. Both results were identical, and system-02 rejoined as STANDBY (`docs/results/demo-master-failover-20261127-023317.md`). The rollback was also tested: with HA off, provision removed ZooKeeper and the standby master, and Word Count stayed correct.

**Reversibility:** set `ha_enabled: false` in `inventory.yaml` and run `clusterctl provision`. That removes ZooKeeper and the standby master from the machines, because provision deletes any managed service a host no longer needs.

**Honest limit:** the **driver** still runs in client mode on system-01. HA protects the *master* service, not the machine the driver runs on. For PySpark, the standalone cluster manager does not support cluster deploy mode, which would be needed to supervise drivers. In production you would run the driver on a separate gateway machine.
