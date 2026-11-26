# Evaluation day runbook

This runbook is a 10–12 minute script for showing the evaluator the working cluster. Run every command from `C:\Users\saumi\Desktop\IBD` in PowerShell. Your laptop must be on the network that can reach the jump host `QWERTY-GPU-clg`.

## What you are proving

| Requirement | What proves it | Where to show it |
|-------------|----------------|------------------|
| A Spark cluster on **physical** machines | 4 HP Pro Tower 280 G9 desktops. `systemd-detect-virt` = `none` on all of them | Dashboard **Physical nodes**, `clusterctl facts`, or report §3 |
| The cluster is up | Master ALIVE, 3/3 workers, 72 cores | Dashboard hero row, master UI http://localhost:18080 |
| Word Count runs on the cluster | 3 executors (one per worker), 25.3 M words in about 8 s | Dashboard **Word Count**, job UI http://localhost:14040 |
| The answer is correct | Output is byte-identical to a single-machine count | The "✓ identical" badge, or report §4 |
| Spark recovers from a failure | A worker is killed mid-job, 24 tasks are lost and re-run, and the result is identical | Dashboard **Failure recovery**, report §5, master UI |

## 30 minutes before the evaluation

Open 3 terminals.

**Terminal 1: health check, then keep it free for commands**
```powershell
.venv\Scripts\python -m clusterctl status
```
You should see `master: ALIVE  workers alive: 3  cores: 72`.

If a worker is missing, run:
```powershell
.venv\Scripts\python -m clusterctl start
```
If a whole machine was rebooted, systemd starts Spark on it automatically. Run `status` again after a minute.

**Terminal 2: tunnel to the Spark web UIs (leave it running)**
```powershell
.venv\Scripts\python -m clusterctl tunnel
```

**Terminal 3: dashboard (leave it running)**
```powershell
.venv\Scripts\python -m clusterctl dashboard
```
Open the `http://127.0.0.1:8765/?token=...` link it prints.

**Browser tabs, in this order:**
1. Dashboard
2. http://localhost:18080: Spark master
3. http://localhost:14040: running job. It is empty until a job runs, which is normal.
4. The latest `results\evidence-*.html`. To create a fresh one, run `.venv\Scripts\python -m clusterctl evidence`.

## The demo, step by step

### 1. The cluster and the machines (2 min)
- Dashboard **Cluster at a glance**: master ALIVE, 3/3 workers, 72 cores, 4/4 physical.
- **Physical nodes** cards: the hostnames (cselab2pc36, cselab4pc27, …), HP Pro Tower 280 G9, i7-13700 with 24 cores, 15.3 GB, and the "bare metal" badge (`virt=none`).
- Say: *"systemd-detect-virt would print kvm, vmware or docker inside a VM or container. All four print none."*
- If the evaluator wants to see it live on a machine, run:
  ```powershell
  ssh -J QWERTY-GPU-clg -i keys/control_ed25519 -o IdentitiesOnly=yes -o UserKnownHostsFile=keys/known_hosts ibd-team2-01@172.22.68.221 "hostname; systemd-detect-virt; systemctl status spark-master --no-pager | head -5"
  ```
- Switch to the master UI (localhost:18080): 3 workers ALIVE, each with 24 cores, at the 3 different IPs.

### 2. How it was set up (2 min)
Explain the pipeline using README.md and SETUP.md:

1. `systems.json` goes into an encrypted vault and an inventory file.
2. `bootstrap` sets up SSH keys and records them in a key registry.
3. `provision` installs Java 17 and Spark 3.5.10 (SHA-512 verified) and installs systemd services.
4. `up` does all of the above in one command, and adding a machine is one line plus `up --limit`.

Show the dashboard's **Key registry** and **Audit log** panels.

### 3. Word Count live (2 min)
- Dashboard, then **Run Word Count now**. You can also run `.venv\Scripts\python -m clusterctl run --input corpus.txt --partitions 72` in a terminal.
- While it runs, switch to **localhost:14040** (the job UI). Show the stages and the Executors tab: 3 executors on 3 machines.
- When it finishes, the dashboard **Word Count** panel shows:
  - total words, the top-20 chart, and the per-worker executors
  - the **✓ identical to single-machine count** badge
- Explain the code in `jobs/wordcount.py`. See REPORT.md §4:

  ```
  textFile → flatMap(tokenize) → map(word,1) → reduceByKey(+) → sortBy
  ```

  `reduceByKey` is the shuffle boundary, so the job runs as 2 stages.

### 4. Failure recovery live (3 min)
- Dashboard, then **Run failure demo now**. You can also run `.venv\Scripts\python -m clusterctl demo failure --input corpus.txt`. It takes about 2 minutes:
  1. a clean baseline run
  2. a slowed-down run (12 s per partition, so there is time to watch)
  3. after about 3 s, the busiest worker is killed with `systemctl stop spark-worker`
- **While it runs**, keep **localhost:18080** on screen:
  - The killed worker disappears from the ALIVE workers, and the running application drops from 72 to 48 cores.
  - The job keeps going.
- Optionally show localhost:14040, Stages. The failed tasks appear, then succeed as attempt 1.
- When the demo finishes, the dashboard **Failure recovery** panel shows:
  - the timeline: killed, detected, finished, identical, rejoined
  - **24 lost / 24 retried**
  - the **✓ identical to baseline** checksums
  - the key driver log lines: `Lost executor`, `ExecutorLostFailure`, `Starting task N.1`
- Explain *why* it recovered, using REPORT.md §5.3:
  - **Detection:** the RPC connection to the executor dropped.
  - **Task retry:** each lost task ran again as attempt `.1` on a live executor.
  - **Lineage:** the lost partitions were recomputed from the input file, which is on every node.
  - **Determinism:** the recomputed results are the same, so the final answer is the same.
  - **Rejoin:** the worker was restarted and the cluster is back to 72 cores.
- **Job history** panel: the failure runs are highlighted with 48 cores, against 72 for normal runs.

### 5. The evidence report (1 min)
- Dashboard, then **Generate evidence report**, then open it. You can also run `.venv\Scripts\python -m clusterctl evidence`.
- It is one offline page with every proof: status, topology, machines, Word Count, recovery, job history and setup. You can print it to PDF and submit it.

## If something goes wrong on the day

| Problem | What to do |
|---------|------------|
| Jump host or network is down | You can't run anything live. Show the recorded evidence in `docs/evidence/`. It is committed to the repo, so it also works from GitHub: `dashboard.png` (the live dashboard), `evidence-report.html` (open it in a browser), `failure-demo.md` and `failure-demo-driver.log`. Also generate a fresh report **the day before** and keep a PDF copy |
| A worker is not ALIVE | Run `clusterctl start`, then `clusterctl status`. If a machine is powered off, the cluster still runs with the others. Explain this as fault tolerance |
| The localhost:18080 tab shows something else | Another program took the port. Look at the port the `tunnel` terminal actually printed |
| The localhost:14040 tab says "not available" | No job is running. Start Word Count or the demo first |
| A dashboard action says "vault locked" | Unlock it in the **Vault** panel (the master password is in `.vault-pass`), or run the CLI command instead |
| The demo fails | Run `clusterctl status`. Restart the victim with `clusterctl start --limit system-02`, then run the demo again |

## Day-before checklist

- [ ] `clusterctl status` shows 3/3 workers
- [ ] `clusterctl run --input corpus.txt` shows the ✓ identical message
- [ ] `clusterctl demo failure --input corpus.txt` shows identical: True
- [ ] `clusterctl evidence` produced a report. Open it and print it to PDF as a backup
- [ ] The dashboard opens, and every panel shows data
- [ ] `systems.json` is deleted (its passwords are already in the vault) and `.vault-pass` is backed up
