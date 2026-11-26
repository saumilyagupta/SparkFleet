# Project report: Spark cluster on physical machines and Word Count

Group project, IBD course. Team 2.

## 1. Goal

1. Set up an Apache Spark cluster on physical machines. No VMs and no single-node local setup.
2. Run Word Count on that cluster.
3. Show the cluster working, explain the setup and the code, and show how Spark recovers when a node fails.

## 2. Architecture

```
            laptop (control node: clusterctl + dashboard)
                 │ SSH (keys)  ─── tunnelled via jump host QWERTY-GPU-clg
                 ▼
  ┌──────────────────────────── lab LAN 172.22.68.0/24 ────────────────────────────┐
  │                                                                                 │
  │   system-01 172.22.68.221           system-02 .216   system-03 .217   system-04 .218
  │   ┌───────────────────────┐         ┌────────────┐   ┌────────────┐   ┌────────────┐
  │   │ spark-master :7077    │◄────────┤ Worker     │   │ Worker     │   │ Worker     │
  │   │ master UI    :8080    │◄────────┤ 24 cores   │   │ 24 cores   │   │ 24 cores   │
  │   │ driver (spark-submit) │────────►│ Executor   │   │ Executor   │   │ Executor   │
  │   │   :7078, UI :4040     │◄───────►│  (JVM+py)  │◄─►│  (JVM+py)  │◄─►│  (JVM+py)  │
  │   └───────────────────────┘ tasks / └────────────┘   └────────────┘   └────────────┘
  │                             results     shuffle blocks between executors (:7079+)
  └─────────────────────────────────────────────────────────────────────────────────┘
```

- **Cluster manager:** Spark standalone. The master tracks workers through heartbeats and hands out executors.
- **Deploy mode:** client. The driver (`spark-submit`) runs on system-01, next to the master.
- **Storage:** each node has the input at the same local path. `clusterctl data push` copies it there and checks it with SHA-256. We have no HDFS.
- **Software versions:**
  - Spark 3.5.10 (Hadoop 3 build)
  - OpenJDK 17
  - Python 3.12, the same version on every node
  - Ubuntu
- **Resources:** 3 workers with 24 cores each, 72 cores in total, and 43.9 GB of memory.

## 3. Setup automation (`clusterctl`)

We automated the whole setup with a Python tool, so the cluster can be rebuilt with one command (`clusterctl up`) and grown by adding lines to a file. [SETUP.md](SETUP.md) covers each step in detail.

| Module | Responsibility |
|--------|----------------|
| `inventory.py` | Validated, immutable model of `inventory.yaml`: hosts, roles and cluster settings |
| `vault.py` | Encrypted password store (scrypt + Fernet) |
| `systems_import.py` | Turns the lab's `systems.json` into the inventory and the vault, and picks the master (never the laptop) |
| `jump.py`, `ssh.py` | Paramiko sessions through the jump host: `sudo` with the password sent on stdin, SFTP, and pinned host keys |
| `keys.py`, `bootstrap.py` | Installs the control key, creates a key on each node, and keeps the key registry and audit log |
| `provision.py` | Idempotent steps: preflight, apt packages, Python consistency check, Spark install verified by SHA-512, config templates, systemd units, `/etc/hosts`, ufw |
| `cluster_ops.py` | Start, stop, verify, and status (reads the master's `/json/` endpoint) |
| `workload.py` | Distributes data, runs Word Count, and runs the failure demo |
| `tunnel.py` | Forwards the Spark web UIs to the laptop |
| `dashboard/` | Local web UI over the same functions |

Design choices:

- **Secrets never reach git.** Passwords are only in the encrypted vault, and after bootstrap all access uses keys. `.gitignore` blocks the vault, the keys, `systems.json`, CSV files and the real inventory.
- **Every step can be re-run.** Each step checks the current state before changing anything, so running `up` twice is safe and adding a node touches only that node.
- **Daemons run under systemd** instead of `start-all.sh`. They restart on crash, start at boot, and the master does not need SSH access to the workers to start them.

## 4. The Word Count program

[`jobs/wordcount.py`](../jobs/wordcount.py) uses the RDD API:

```python
lines  = sc.textFile("file:///opt/spark-data/input/corpus.txt", 72)   # 72 partitions
counts = (lines.flatMap(tokenize)                 # line  -> words      (narrow)
               .map(lambda w: (w, 1))            # word  -> (word, 1)  (narrow)
               .reduceByKey(lambda a, b: a + b)  # shuffle by word, sum (wide)
               .sortBy(lambda kv: (-kv[1], kv[0])))  # sort by count desc, word asc
results = counts.collect()                        # bring the sorted counts to the driver
```

- `tokenize` lower-cases each line and keeps runs of letters and digits. An apostrophe is kept inside a word, so "don't" stays one word.
- Spark builds stages from this plan:
  - **Stage 0** (map side): `textFile → flatMap → map`, plus a partial `reduceByKey` that combines counts on each executor first, which keeps the shuffle small.
  - **Stage 1:** reads the shuffled data, finishes the reduce, and samples the data to set up range partitioning for the sort.
  - **Stage 2:** the sorted `collect`.
- The driver writes the results as a TSV file on the master, and `clusterctl` downloads it.
- **Correctness check:** [`jobs/local_count.py`](../jobs/local_count.py) counts the same file on a single machine with the same tokenizer. Its output was **byte-identical** to the cluster's.

The clean run (`clusterctl run --input corpus.txt --partitions 72`):

```
Executor added: app-20261126180357-0000/0 on worker-...-172.22.68.218 with 24 core(s)
Executor added: app-20261126180357-0000/1 on worker-...-172.22.68.217 with 24 core(s)
Executor added: app-20261126180357-0000/2 on worker-...-172.22.68.216 with 24 core(s)
WORDCOUNT_DONE app_id=app-20261126180357-0000 driver=cselab2pc36
input=file:///opt/spark-data/input/corpus.txt partitions=72 executors=3
total_words=25327776 distinct_words=99 elapsed_s=6.5
top: the 4893021, of 2448671, and 1630966, to 1222605, in 978175 ...
```

The input is a deterministic 100 MB synthetic text (`jobs/make_corpus.py`, fixed seed). Its word frequencies follow a Zipf-like distribution, so a rerun produces exactly the same expected answer.

## 5. Failure recovery

### 5.1 What we did

We ran `python -m clusterctl demo failure --input corpus.txt`. The command does the following:

1. Runs a **clean baseline** Word Count.
2. Starts a second Word Count with 288 partitions. That is 72 cores × 4 waves.
   - Each partition first sleeps 12 s (`--delay-ms`). This is the only difference from a normal run.
   - The sleep makes the tasks run long enough for us to kill a node while they are running.
3. Polls the driver's REST API (`:4040/api/v1/.../executors`) until a worker has **active tasks**, then picks that worker as the victim.
4. Runs `systemctl stop spark-worker` on the victim. Because of `KillMode=control-group`, this kills the worker JVM **and** its executor JVM. To the cluster, this is the same as losing the machine.
5. Waits for the job to finish, downloads the result, and compares it byte for byte with the baseline.
6. Restarts the worker and checks that it registers with the master again.

### 5.2 What happened (real run, 26 Nov 2026)

| time | event |
|------|-------|
| +13.4 s | job started: 288 partitions, 3 executors × 24 cores |
| +16.3 s | tasks are running on system-02. We inject the failure |
| +17.3 s | `spark-worker` stopped on **system-02 (172.22.68.216)**, killing its worker JVM and executor JVM |
| +20.6 s | the master no longer reports system-02 as ALIVE. The job continues on system-03 and system-04 |
| +102.8 s | the job finishes: `total_words=25327776 distinct_words=99` |
| +102.8 s | the result is **identical** to the baseline (same SHA-256: `12af4f834e0941eb…`) |
| +106.9 s | system-02 restarted and re-registered. The cluster is back to 3/3 workers |

The full report is in `results/demo-20261126-180441.md`. These are the key lines from the driver log:

```
18:04:58 INFO  Executor updated: app-...-0002/2 is now EXITED (Worker shutting down)
18:04:58 ERROR TaskSchedulerImpl: Lost executor 2 on 172.22.68.216: Worker shutting down
18:04:58 WARN  TaskSetManager: Lost task 23.0 in stage 0.0 (TID 23) (172.22.68.216 executor 2):
               ExecutorLostFailure (executor 2 exited caused by one of the running tasks)
               ... 24 tasks lost in total (all tasks that were running on executor 2)
18:04:58 INFO  DAGScheduler: Executor lost: 2 (epoch 0)
18:04:58 INFO  Executor updated: app-...-0002/3 is now LOST (worker lost: 172.22.68.216 got disassociated)
18:04:58 INFO  TaskSetManager: Starting task 0.1 in stage 0.0 (TID 48) (172.22.68.217, executor 1, partition 0)
18:04:58 INFO  TaskSetManager: Starting task 23.1 in stage 0.0 (TID 71) (172.22.68.217, executor 1, partition 23)
18:05:13 INFO  TaskSetManager: Finished task 0.1 in stage 0.0 (TID 48) in 14865 ms on 172.22.68.217
18:06:15 INFO  DAGScheduler: ShuffleMapStage 0 (reduceByKey ...) finished in 78.942 s
18:06:17 INFO  DAGScheduler: Job 0 finished: sortBy ..., took 80.923785 s
```

`task 0.1` means attempt 1 of partition 0. The first attempt (`0.0`) was lost on system-02, and Spark ran the second attempt on system-03 within the same millisecond.

### 5.3 Why Spark recovered

1. **Failure detection.**
   - Each executor keeps an RPC connection to the driver.
   - When system-02's executor JVM died, the connection dropped ("disassociated"). `TaskSchedulerImpl` marked executor 2 as lost, and every task running on it failed with `ExecutorLostFailure`.
   - The master noticed separately that the worker's connection had closed and removed it from the list of ALIVE workers.
   - The heartbeat timeout (`spark.worker.timeout`, 60 s) is a backstop for silent failures such as a pulled network cable.
2. **Task retry.**
   - A failed task goes back into the task set as a new attempt, here `0.0 → 0.1`, and is scheduled on any free core of a surviving executor.
   - `spark.task.maxFailures=4` allows up to 4 attempts per task before the job is given up.
   - Losing an executor is not counted against the task as its own fault. It counts as an `ExecutorLostFailure` caused by the executor, so a dead node cannot use up the retry budget.
3. **RDD lineage.**
   - An RDD partition is not stored data. It is a *recipe*: `textFile(split i) → flatMap → map → partial reduce`.
   - The input split still exists on the surviving nodes, because every node has the file at the same path.
   - So Spark recomputes the lost partitions from the source instead of trying to recover them from the dead machine.
   - Nothing that was only on system-02 was needed to produce the answer.
4. **Stage resubmission (the case this run did not need).**
   - If a node dies *after* the map stage, the shuffle files on its disk are lost too.
   - The reduce side then hits a `FetchFailed` error. The `DAGScheduler` resubmits the parent `ShuffleMapStage` only for the missing map outputs, using lineage again, and then retries the reduce stage.
   - In our run the failure came during stage 0, so plain task retry was enough.
5. **Determinism → the same answer.**
   - Every transformation in Word Count is deterministic.
   - `reduceByKey` uses `+`, which is associative and commutative, so the order of partial sums does not matter.
   - Re-running a partition therefore gives the same output, and the final result is byte-identical to the clean run.
6. **Cluster self-healing.**
   - The cluster lost 24 of its 72 cores for the rest of the job, and the job still finished.
   - It took 85 s instead of about 77 s, because the remaining waves ran on 48 cores.
   - Afterwards we ran `systemctl start spark-worker`. The worker registered again with a new worker ID, and the cluster was back to 72 cores without restarting the master.
   - If a worker *crashes* instead of being stopped, systemd's `Restart=on-failure` brings it back automatically.

### 5.4 What Spark does not recover from in this setup

- **Master failure.** Standalone mode with one master has no automatic master failover. Running jobs keep going, but no new applications or workers can register until the master restarts. systemd restarts it on a crash. The production fix is ZooKeeper-based standby masters (`spark.deploy.recoveryMode=ZOOKEEPER`).
- **Driver failure.** In client mode the driver is the `spark-submit` process on system-01. If it dies, the job is lost. `--deploy-mode cluster --supervise` would make the master restart the driver.

## 6. How to demo during evaluation

1. Run `python -m clusterctl status` to show 4 physical machines, the master ALIVE and 3 workers ALIVE.
2. Run `python -m clusterctl tunnel` and open http://localhost:18080 to show the master UI with 3 workers and 72 cores.
3. Run `python -m clusterctl run --input corpus.txt --partitions 72`. Then open http://localhost:18080 and the application's event timeline to show the tasks spread over all 3 workers.
4. Run `python -m clusterctl demo failure --input corpus.txt`. While it runs, watch http://localhost:18080: the killed worker disappears from the ALIVE list. Watch http://localhost:14040 for the failed tasks being re-run. At the end, show `results/demo-*.md`.
5. Optionally run `python -m clusterctl dashboard` and do the same from the web UI.
