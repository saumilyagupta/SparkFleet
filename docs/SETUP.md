# How we built the Spark cluster

This guide walks through every step we took to build our Spark cluster, in order. It also shows what each `clusterctl` command does on the machines, so you can rebuild the cluster by hand if you need to.

## 1. Hardware and network

| Name | IP | Role | User |
|------|----|------|------|
| system-01 | 172.22.68.221 | **Spark master** and driver | ibd-team2-01 |
| system-02 | 172.22.68.216 | worker | ibd-team2-02 |
| system-03 | 172.22.68.217 | worker | ibd-team2-03 |
| system-04 | 172.22.68.218 | worker | ibd-team2-04 |

- These are four physical lab PCs running Ubuntu with Python 3.12. Together the workers offer 72 cores (24 each) and 43.9 GB of memory to Spark.
- The control laptop is not part of the cluster. The master must be one of the lab machines.
- The laptop cannot reach `172.22.68.0/24` directly. It reaches the lab through a jump host, `QWERTY-GPU-clg` in `~/.ssh/config`, which can reach all four PCs.
  - We checked this from the laptop: port 22 on each PC was unreachable directly, but reachable from the jump host.
  - `clusterctl` opens an SSH `direct-tcpip` channel through the jump host, the same thing `ssh -J` does.
  - The session to each PC is encrypted end to end, so lab passwords never appear on the jump host.

## 2. Storing machines and passwords

The lab gave us a `systems.json` file containing each PC's IP, username and password. We imported it once:

```powershell
python -m clusterctl init --systems systems.json --jump QWERTY-GPU-clg
```

This command did four things:

1. Generated an ed25519 **control key** on the laptop: `keys/control_ed25519`.
2. Created `secrets.vault`, an encrypted store unlocked by one master password.
   - The key is derived with scrypt (N=2^15, r=8, p=1).
   - The data is encrypted with Fernet (AES-128-CBC plus HMAC-SHA256).
   - All 4 passwords are stored in it.
3. Wrote `inventory.yaml`, which holds names, IPs, users and roles but no passwords. The first system became the master. Pass `--master <name|ip>` to choose a different one.
4. Refused to make the laptop the master. The importer rejects any master IP that belongs to the control machine.

After the import, `systems.json` can be deleted. It is in `.gitignore`, together with the vault, keys and real inventory, so none of them can be committed by mistake.

## 3. Bootstrap: SSH keys and the key log

```powershell
python -m clusterctl bootstrap
```

Bootstrap does the following on every node, in parallel:

1. Logs in once with the password from the vault and pins the node's SSH host key in `keys/known_hosts`. From then on, a changed host key is rejected.
2. Adds the laptop's control public key to `~/.ssh/authorized_keys`.
3. Creates a key on the node (`~/.ssh/id_ed25519`). Its private key never leaves the node.
4. Logs in again using only the key, to prove that key login works.

After all nodes are done, it authorizes the master's node key on every worker and adds the workers' host keys to the master's `known_hosts`. The master needs this for the LAN fan-out in step 6 and for Spark's own `sbin/start-all.sh`.

Every key is recorded in `keys/registry.json` with:

- its fingerprint
- its type: control or node
- its owner
- which hosts it is installed on
- its status: active or revoked

Every event is also appended to `keys/audit.log`. To view or revoke keys:

```powershell
python -m clusterctl keys list
python -m clusterctl keys audit
python -m clusterctl keys revoke SHA256:...
```

`keys revoke` removes the key from every host it was installed on.

## 4. Provisioning: Java and Spark

```powershell
python -m clusterctl provision
```

Provisioning runs in phases. Each phase runs in parallel on all hosts, and the next phase starts only if every host succeeded.

| Phase | What happens on each node |
|-------|---------------------------|
| preflight | Checks that `sudo` works with the vault password (sent on stdin, never on the command line), the OS is Debian-family, at least 2 GB of disk is free, and the node can reach every other node on port 22 |
| packages | Runs `apt-get install openjdk-17-jdk-headless python3 procps`. It falls back to JDK 21 or 11 if 17 is not available |
| python check | Compares the `python3` minor version across **all** nodes. PySpark fails if the driver and the executors use different versions. All our nodes have 3.12 |
| spark install | Checks the release's SHA-512 from apache.org, then the node downloads `spark-3.5.10-bin-hadoop3.tgz` itself and verifies it. Only if the download fails does the laptop upload its own copy. Spark is unpacked to `/opt/spark-3.5.10-bin-hadoop3`, `/opt/spark` is symlinked to it, and `/opt/spark-data/{input,jobs,output,events,logs,work}` is created |
| config | Generates `conf/spark-env.sh` and `conf/spark-defaults.conf` from `clusterctl/templates/` |
| services | Installs `spark-master.service` on the master and `spark-worker.service` on each worker, then runs `systemctl enable` |
| hosts + firewall | Writes a managed block into `/etc/hosts` on every node. If `ufw` is active, opens the ports to cluster peers only. On our machines ufw was inactive |

Key settings in the generated config:

- `SPARK_LOCAL_IP` is set to the node's LAN IP. Without it, Ubuntu's `127.0.1.1 hostname` entry would make Spark bind to loopback, and the workers would be unreachable.
- `spark.driver.host` is the IP of the node where the driver runs. In client mode the driver runs on the master, and executors connect back to it.
- `spark.driver.port` is 7078 and `spark.blockManager.port` is 7079, so the ports are fixed and can be opened in a firewall.
- Event logging writes to `/opt/spark-data/events`.

Both systemd units run the Spark daemons in the foreground (`SPARK_NO_DAEMONIZE`) as the lab user, and both use `Restart=on-failure`. As a result:

- The cluster comes back on its own after a reboot.
- A crashed daemon is restarted.
- A deliberate `systemctl stop`, such as the one in the failure demo, is not undone.

## 5. Start and verify

```powershell
python -m clusterctl start     # master first, then workers
python -m clusterctl status
```

The status output after starting:

```
master: ALIVE  workers alive: 3  cores: 72  memory: 43932 MB
  system-01    172.22.68.221    master  up     spark-master=active        worker=-
  system-02    172.22.68.216    worker  up     spark-worker=active        worker=ALIVE
  system-03    172.22.68.217    worker  up     spark-worker=active        worker=ALIVE
  system-04    172.22.68.218    worker  up     spark-worker=active        worker=ALIVE
```

`verify` reads the master's JSON endpoint, `http://172.22.68.221:8080/json/`. It runs that request on the master over SSH, so it works even though the laptop has no route to port 8080.

To open the web UIs in the laptop's browser, start the tunnel:

```powershell
python -m clusterctl tunnel
```

It forwards these addresses through the jump host:

- `localhost:18080`: master UI
- `localhost:14040`: running job UI (only while a job runs)
- `localhost:18081` to `18083`: worker UIs

The local ports are the Spark ports plus 10000, so they don't clash with programs already using
8080 on the laptop (for example a local Postgres/Apache install). If a port is still busy, the next
free one is used and printed.

## 6. Word Count

```powershell
python jobs/make_corpus.py --out corpus.txt --size-mb 100   # deterministic 100 MB text
python -m clusterctl data push corpus.txt                   # same path on every node
python -m clusterctl run --input corpus.txt --partitions 72
```

We have no HDFS, so the input must be at the same local path on every node (`file:///opt/spark-data/input/corpus.txt`). `data push` gets it there:

1. The laptop uploads the file once, to the master.
2. The master copies it to the workers over the LAN.
3. Each copy is checked with SHA-256.

`run` then does the following:

1. Uploads `jobs/wordcount.py` to the master.
2. Runs `spark-submit --master spark://172.22.68.221:7077` there.
3. Streams the driver log back to the laptop.
4. Downloads the result to `results/`.

The result of our run:

```
Executor added: app-20261126180357-0000/0 on worker-...-172.22.68.218 with 24 core(s)
Executor added: app-20261126180357-0000/1 on worker-...-172.22.68.217 with 24 core(s)
Executor added: app-20261126180357-0000/2 on worker-...-172.22.68.216 with 24 core(s)
WORDCOUNT_DONE app_id=app-20261126180357-0000 driver=cselab2pc36
input=file:///opt/spark-data/input/corpus.txt partitions=72 executors=3
total_words=25327776 distinct_words=99 elapsed_s=6.5
```

To check correctness, we counted the same file on one machine with `jobs/local_count.py`. Its output is byte-identical to the cluster's output.

## 7. Failure-recovery demo

```powershell
python -m clusterctl demo failure --input corpus.txt
```

See [REPORT.md](REPORT.md#failure-recovery) for the timeline of our run, the driver log lines, and an explanation of how Spark recovered.

## 8. Adding more machines

1. Add the machines with one of these:
   - `inventory add`
   - `inventory import hosts.csv --delete-after`
   - a longer `systems.json`
2. Run `python -m clusterctl up --limit <new names>`.

Every step can be run again safely. Running `up` a second time on an existing node changes nothing.

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `host key for X CHANGED` | The node was reinstalled, or a different machine now has its IP. Remove its line from `keys/known_hosts` and run `bootstrap` again |
| `no password stored for 'x'` | Run `python -m clusterctl vault set x` |
| Workers never become ALIVE | Run `journalctl -u spark-worker -n 50` on the worker. This usually means the firewall is blocking 7077, or the worker is bound to loopback |
| `Python versions differ` | Install the same `python3.X` on all nodes and set `cluster.python_bin` in `inventory.yaml` |
| Job hangs at "Initial job has not accepted any resources" | The executors cannot connect back to the driver. Check that ports 7078–7110 are open to the master |
