# Run the tests on EC2 — complete beginner's guide

This walks you through, click by click, renting one temporary Amazon Linux
server (an "EC2 instance"), running the whole project's tests + a live demo on
it, seeing the result, and shutting it down so you're not charged. No prior AWS
experience assumed.

**Time:** ~30 minutes of your attention (plus ~15 min the server spends building).
**Cost:** roughly **$0.10–$0.30 total** if you terminate it when done (a
`t3.large` is about $0.083/hour). It is **not** free-tier — so the one rule that
matters is **Step 9: terminate the instance when you're finished.**

---

## What you need before starting

1. An **AWS account** (console.aws.amazon.com — signing up needs a credit card).
2. The project code. Easiest is to have it on **GitHub** (Step 2 below). If you
   don't use GitHub, there's an upload alternative in Step 6.
3. A web browser. That's it — we'll use AWS's built-in browser terminal so you
   don't need to install SSH keys or PuTTY.

---

## Step 1 — Pick a region

Top-right of the AWS console shows a region (e.g. "N. Virginia"). Click it and
choose one near you (e.g. **us-east-1 / N. Virginia**). Just remember which one —
everything you create lives in that region.

---

## Step 2 — Put the code on GitHub (recommended)

On your own computer, in the project folder (`otel-data-staleness`):

```bash
git init
git add .
git commit -m "otel-data-staleness"
```

Then create an empty repo at github.com (green "New" button, name it
`otel-data-staleness`, leave it empty), and follow its "push an existing
repository" lines, which look like:

```bash
git remote add origin https://github.com/<your-username>/otel-data-staleness.git
git branch -M main
git push -u origin main
```

If it's a **private** repo, that's fine — you'll paste a GitHub token as the
password when cloning later, or make it public for the duration of the test.
(Prefer not to use GitHub at all? Skip this and use the upload method in Step 6.)

---

## Step 3 — Create a login key (key pair)

We'll use the browser terminal to connect, but AWS still wants a key on the
instance.

1. In the console search bar type **EC2** and open it.
2. Left menu → **Key Pairs** (under "Network & Security").
3. **Create key pair** → Name it `staleness-key` → Type **RSA** → Format **.pem**
   → **Create**. Your browser downloads `staleness-key.pem`. Keep it; you may not
   even need it, but it's required to launch.

---

## Step 4 — Launch the server

1. EC2 left menu → **Instances** → orange **Launch instances**.
2. **Name:** `staleness-test`.
3. **Application and OS Image:** click **Ubuntu**, then in the dropdown pick
   **Ubuntu Server 24.04 LTS** (make sure it says 64-bit x86).
4. **Instance type:** click the box and choose **t3.large**. (Smaller types run
   out of memory building the collector.)
5. **Key pair:** select `staleness-key` from Step 3.
6. **Network settings** → click **Edit**:
   - Leave **Allow SSH traffic from** set to **Anywhere (0.0.0.0/0)**. This is
     required for the browser terminal in Step 5 to work (it connects from
     Amazon's network, not your home IP). It's an acceptable risk here because
     the server is disposable, protected by the key, and you terminate it in ~30
     minutes. (Advanced users can instead restrict to the *EC2 Instance Connect*
     IP range for your region.)
   - Check **Allow HTTP** is *off* (we don't need it).
7. **Configure storage:** change the size from 8 to **30 GiB** (gp3). The build
   needs the space.
8. Click **Launch instance**. Then **View all instances**.
9. Wait ~1 minute until **Instance state** shows **Running** and **Status check**
   shows "2/2 checks passed".

> 💡 You just started paying (~$0.083/hr). It's fine — just don't forget Step 9.

---

## Step 5 — Open the browser terminal

1. On the Instances list, tick the checkbox next to `staleness-test`.
2. Click **Connect** (top).
3. Stay on the **EC2 Instance Connect** tab → **Connect**.
4. A black terminal opens **in your browser**, logged in as `ubuntu`. This is the
   server. Everything from here is typed into that terminal.

---

## Step 6 — Get the code onto the server

**If you used GitHub (Step 2):**

```bash
git clone https://github.com/<your-username>/otel-data-staleness.git
cd otel-data-staleness
```

(For a private repo it'll prompt for username + a GitHub *personal access token*
as the password.)

**If you did NOT use GitHub — copy it from your computer with `scp`:**
The browser terminal can't receive file uploads, so use your own computer's
terminal instead (on Windows, open **PowerShell** — it has `scp` built in).
First, on the AWS Instances page copy the instance's **Public IPv4 address**.
Then, from the folder that contains `otel-data-staleness`, run (replace the path
to your key and the IP):

```powershell
# zip it first (PowerShell):
Compress-Archive -Path .\otel-data-staleness -DestinationPath .\proj.zip
# copy it up (use the staleness-key.pem you downloaded in Step 3):
scp -i .\staleness-key.pem .\proj.zip ubuntu@<PUBLIC-IP>:~/
```

Then back in the **browser terminal**:

```bash
sudo apt-get update -y && sudo apt-get install -y unzip
unzip proj.zip && cd otel-data-staleness
```

(If `scp` complains the key is "too open", right-click `staleness-key.pem` →
Properties → Security and remove inherited permissions so only you can read it —
or just use the GitHub method, which avoids this entirely.)

---

## Step 7 — Run everything with one command

```bash
bash scripts/ec2-bootstrap.sh
```

This does it all: installs the tools (Go, Docker, Python), runs **every** test
suite, builds and validates the custom Collector, starts a live demo with real
Postgres + Kafka, and checks that staleness metrics actually appear. It prints a
banner before each stage.

**This takes ~10–15 minutes** (mostly the Collector build and pulling Docker
images). It's normal for it to sit quietly during the "Build" and
"Start the demo" stages.

You may be asked once to confirm a package install — press **Enter/Y** if so.

### What success looks like

- The test stages print things like `52 passed`, `PASS — 0 mismatch(es)`, and
  `ok  github.com/otel-data-staleness/...`.
- The final smoke test prints, after a minute or two:

  ```
  PASS ✅  data.staleness.* metrics are flowing and a stale source is breaching its SLA.
  ```

If you see that line, **the project works end-to-end on a real machine.** That's
the whole test.

---

## Step 8 — (Optional) See the pretty dashboard

The PASS line already proves it works. If you also want to *see* the Grafana
dashboard with staleness climbing:

1. Back in the AWS console: EC2 → **Security Groups** → click the group attached
   to your instance → **Edit inbound rules** → **Add rule**:
   - Type **Custom TCP**, Port **3000**, Source **My IP**. Add another for port
     **9090**. **Save rules.**
2. On the Instances page, copy your instance's **Public IPv4 address**.
3. In your browser go to `http://<that-ip>:3000` → log in `admin` / `admin` →
   open the **"Data Staleness"** dashboard. Within a minute the *"Sources
   breaching SLA"* number rises as the stale sources cross their 60-second SLA,
   while the fresh ones stay near zero.
4. `http://<that-ip>:9090` is Prometheus — try typing `data_staleness_age` in the
   query box and press Execute.

> These ports are open only to your IP. You'll remove the whole thing in Step 9
> anyway.

---

## Step 9 — ⚠️ Shut it down (do NOT skip)

This is the step that stops the charges.

1. AWS console → EC2 → **Instances**.
2. Tick `staleness-test` → **Instance state** → **Terminate (delete) instance**
   → confirm.
3. The instance moves to "Shutting down" then "Terminated". Once terminated it
   costs nothing and cannot be restarted (which is what we want — it was
   disposable).

That's it. Total spend for a ~30-minute test is well under a dollar.

---

## What this proves for an OpenTelemetry submission

The EC2 run above is the *evidence* that backs an OpenTelemetry submission — it
does not by itself make the convention an accepted standard (only the SIG can do
that). When `ec2-bootstrap.sh` exits `0`, every item below has been demonstrated
on a clean machine — the kind of prototype-backed evidence the Semantic
Conventions SIG values:

| # | Requirement the SIG cares about | How this test proves it |
|---|--------------------------------|-------------------------|
| 1 | **A written spec exists** | `spec/semantic-conventions.md` (v0.4.0) defines every `data.staleness.*` metric + attribute. |
| 2 | **A machine-readable model** | `model/registry/data-staleness.yaml` is the OTEL **Weaver** model — the format the SIG models conventions in. |
| 3 | **Language-agnostic conformance** | `conformance/runner.py` replays vectors and prints `PASS — 0 mismatch(es)`; proves the numbers aren't one library's opinion. |
| 4 | **A working reference implementation** | Python SDK (**52 tests**) + Go Collector receiver (**41**) + processor (**9**) all build, vet, and test green. |
| 5 | **Zero-code adoption path** | The custom Collector builds via OCB and `validate`s the demo config — infra teams need no code changes. |
| 6 | **It actually emits the metric on real systems** | The live stack scrapes **real** Postgres + Kafka + files; the smoke test asserts `data.staleness.age` is flowing. |
| 7 | **It's honest under failure** | Stale sources cross their 60s SLA and `data.staleness.sla.breached == 1`; empty/timeouts surface as `probe.errors`, never a fabricated `0`. |

**Your one-line proof to paste into the SIG discussion:** *"Full reference
implementation (Python SDK + Go Collector receiver/processor), a Weaver model,
and a language-agnostic conformance suite — all validated end-to-end on a clean
Ubuntu EC2 box against real Postgres + Kafka. Bootstrap script exits 0."*

What this test does **not** cover (be upfront about it — the SIG respects candor):
the AWS-native scrapers (**Kinesis, MSK IAM auth, Confluent Schema Registry**)
need those managed services to exercise live; their *logic* is unit-tested
against hermetic fakes, but the EC2 demo uses Postgres/Kafka/files only. And the
honest remaining gap is **adoption** (stars, downloads, named users) — code
readiness is necessary but not sufficient; socialize via `otep/` next.

---

## If something goes wrong

- **"Permission denied (publickey)" or can't connect:** use the **EC2 Instance
  Connect** tab (Step 5), not "SSH client". If Instance Connect fails, your
  security group's SSH rule may not include your current IP — re-add an inbound
  SSH rule with Source **My IP**.
- **The script stops with a red error in the test stages:** copy the last ~20
  lines and send them to me — that's a real failure worth seeing.
- **Smoke test says FAIL:** the SQL and file sources are the most reliable; the
  usual culprit is Kafka taking longer to start. Re-run just the check:
  `bash scripts/smoke-test.sh`. If it still fails, run
  `sudo docker compose -f deploy/ec2-demo/docker-compose.yaml logs otel-collector | tail -50`
  and share the output.
- **"no space left on device":** you launched with the default 8 GiB instead of
  30. Terminate and relaunch with 30 GiB storage (Step 4.7).
- **Ran out of time / want to pause:** you can **Stop** (not terminate) the
  instance to pause billing for compute, but you still pay a little for storage.
  Simplest is just to terminate and redo — it's cheap.

---

## Just want to check the tests pass, without the AWS dashboard?

Everything except the live Docker demo also runs on your own machine (Windows
via WSL2, or Mac/Linux) if you have Go 1.25, Python 3, and Docker installed —
same command: `bash scripts/ec2-bootstrap.sh`. EC2 just gives you a clean,
disposable Linux box so nothing touches your laptop.
