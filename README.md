# SparkFleet

SparkFleet sets up an Apache Spark standalone cluster on physical Ubuntu/Debian machines. You run one tool on your laptop, `clusterctl`, and it does the setup over SSH. Once the cluster is up, you run a distributed Word Count on it and can run a demo that shows Spark recovering when a worker fails mid-job.

The same setup works for 4 machines or for 100. Add a machine to a file and run one command.

```
 laptop (control node, never part of the cluster)
   │  SSH, optionally tunnelled through a jump host (ssh -J equivalent)
   ▼
 ┌─────────────┐   spark://master:7077   ┌─────────────┐
 │ system-01   │◄────────────────────────┤ system-02   │ worker
 │ Spark master│◄──────────┐             └─────────────┘
 │ + driver    │           │             ┌─────────────┐
 └─────────────┘           ├─────────────┤ system-03   │ worker
                           │             └─────────────┘
                           │             ┌─────────────┐
                           └─────────────┤ system-04   │ worker
                                         └─────────────┘
```

## Where things are stored

| File | Contents | In git? |
|------|----------|---------|
| `inventory.yaml` | Each host's name, IP, user and role, plus cluster settings. No secrets. | no (lab IPs). `examples/inventory.example.yaml` is committed instead |
| `secrets.vault` | Host passwords, encrypted with scrypt and Fernet (AES + HMAC) under one master password | **never** |
| `.vault-pass` | Optional. Holds the vault master password so commands can run unattended. Delete it if you want to type the password each time | **never** |
| `keys/control_ed25519` | The laptop's SSH key. After bootstrap, every connection uses it | **never** |
| `keys/registry.json` | Every public key placed on the cluster: fingerprint, owner, which hosts it is on, and status | **never** |
| `keys/audit.log` | Append-only JSON log of key events (registered, installed, revoked) | **never** |
| `keys/known_hosts` | Pinned host keys of the cluster machines. A changed key is rejected | **never** |

Passwords are used only twice: on first contact, to install the laptop's key, and for `sudo`. From then on, logins use keys.

## Quick start

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt

# 1. Import machines and passwords. systems.json looks like examples/systems.example.json.
#    The master is the first system unless you pass --master (name, IP, label or username).
#    The laptop can never be the master.
.venv\Scripts\python -m clusterctl init --systems systems.json --jump QWERTY-GPU-clg
#    Then delete systems.json. Its passwords are now in secrets.vault.

# 2. One command: install keys, then Java, Spark and systemd services. Start and verify.
.venv\Scripts\python -m clusterctl up

# 3. Check health
.venv\Scripts\python -m clusterctl status

# 4. Word Count
.venv\Scripts\python jobs\make_corpus.py --out corpus.txt --size-mb 200
.venv\Scripts\python -m clusterctl data push corpus.txt
.venv\Scripts\python -m clusterctl run --input corpus.txt

# 5. Failure-recovery demo: kills a busy worker mid-job, then checks the result is unchanged
.venv\Scripts\python -m clusterctl demo failure --input corpus.txt

# Spark web UIs in your browser, tunnelled through the jump host
.venv\Scripts\python -m clusterctl tunnel      # http://localhost:18080 (master), :14040 (job), :18081.. (workers)

# Web dashboard
.venv\Scripts\python -m clusterctl dashboard   # prints http://127.0.0.1:8765/?token=...
```

Leave out `--jump` if your laptop can reach the machines directly.

## Adding machines later (5 or 100)

You can add machines in any of these ways:

```powershell
# one machine (prompts for its password, which goes straight into the vault)
python -m clusterctl inventory add node-05 192.168.1.14 student05

# many at once from a CSV with name,ip,user,role,password (see examples/hosts.example.csv)
python -m clusterctl inventory import new-hosts.csv --delete-after

# or a bigger systems.json
python -m clusterctl import-systems systems.json
```

Then run `up` on the new machines only:

```powershell
python -m clusterctl up --limit node-05,node-06
```

`up --limit` does the heavy installation only on the new hosts. It still refreshes `/etc/hosts` and the firewall rules on every node, so all nodes know about each other. Every step can be re-run safely; running `up` a second time changes nothing.

## Commands

| Command | What it does |
|---------|--------------|
| `init [--systems F] [--master X] [--jump ALIAS]` | Creates the control key, vault and inventory |
| `import-systems F` | Imports a systems.json into the inventory and the vault |
| `inventory list/add/remove/import` | Manages hosts |
| `vault init/list/set/remove` | Manages the encrypted passwords. `list` shows host names only |
| `keys list/audit/revoke FP` | Shows the key registry and audit log. `revoke` removes a key from every host |
| `bootstrap` | Pins host keys, installs the control key and creates a key on each node |
| `provision` | Runs preflight, installs Java and Python, checks that Python versions match, installs Spark with SHA-512 verification, writes the config, installs systemd units, updates /etc/hosts and ufw |
| `start / stop / restart` | Starts or stops the Spark services (master first on start, last on stop) |
| `up` | Runs bootstrap, provision, start and verify in one go |
| `status [--json]` | Shows per-host health and the master's view of its workers |
| `data push F` | Copies an input file to `/opt/spark-data/input/` on every node |
| `run --input F` | Runs Word Count on the cluster and saves the result to `results/` |
| `demo failure --input F [--mode worker/executor]` | Runs the failure-recovery demo. Writes a report to `results/demo-*.md` |
| `tunnel` | Forwards the Spark UIs to localhost |
| `dashboard` | Starts the local web UI on 127.0.0.1 with a per-launch access token |

## What gets installed on each node

- OpenJDK 17 (falls back to 21 or 11 if 17 is not available), `python3` and `procps`, from apt
- Spark 3.5.10 (`bin-hadoop3`) in `/opt/spark-3.5.10-bin-hadoop3`, with a symlink at `/opt/spark`
- `/opt/spark-data/{input,jobs,output,events,logs,work}`, owned by the node's user
- `/opt/spark/conf/spark-env.sh` and `spark-defaults.conf`, generated from `clusterctl/templates/`
- The systemd unit `spark-master.service` on the master and `spark-worker.service` on each worker. Both are enabled at boot and restart on crash
- A managed block in `/etc/hosts`, and ufw rules if ufw is active

See [docs/SETUP.md](docs/SETUP.md) for the step-by-step setup and [docs/REPORT.md](docs/REPORT.md) for the architecture, a walkthrough of the code, and how Spark recovers from failure.

## Tests

```powershell
.venv\Scripts\python -m pytest -q
```
