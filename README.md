# Invoice Action Agent — deployment guide (Render + Neon + Gemini)

You need three free accounts: **GitHub**, **Render**, and **Neon** (database), plus a **Gemini API key**.
Total time: about 20–30 minutes. No coding needed.

---

## Step 1 — Get a Gemini API key

1. Go to https://aistudio.google.com/apikey and sign in with Google.
2. Click **Create API key** and copy it. Keep it private; you will paste it into Render only.

## Step 2 — Create the free database (Neon)

Render's free disk is wiped whenever the app restarts, so tasks must live in a real database.

1. Go to https://neon.tech and sign up.
2. Create a project. Pick the region **AWS Asia Pacific (Singapore)**.
3. On the dashboard, find **Connection string**. Copy the one starting with `postgresql://`.
   It should end with `?sslmode=require`.

## Step 3 — Put the code on GitHub

1. Go to https://github.com/new. Name the repo `invoice-agent`, choose **Private**, and click **Create repository**.
2. On the new repo page, click **uploading an existing file**.
3. Drag in these files: `app.py`, `requirements.txt`, `render.yaml`, `README.md`, `test_agent.py`.
4. Click **Commit changes**.

## Step 4 — Deploy on Render

1. Go to https://render.com, sign up with your GitHub account.
2. Click **New → Web Service** and pick your `invoice-agent` repo.
3. Fill in:
   - **Region:** Singapore
   - **Runtime:** Python 3
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `uvicorn app:app --host 0.0.0.0 --port $PORT`
   - **Instance type:** Free
4. Under **Environment Variables**, add:

   | Key | Value |
   |---|---|
   | `GEMINI_API_KEY` | your Gemini key from Step 1 |
   | `DATABASE_URL` | your Neon connection string from Step 2 |
   | `PYTHON_VERSION` | `3.12.7` |

5. Click **Deploy Web Service** and wait for **Live** (2–5 minutes).
6. Copy your app address from the top of the page, e.g. `https://invoice-agent-ab12.onrender.com`.

## Step 5 — Your A2A interface base URL

Add `/a2a/` to the end of your address:

```
https://invoice-agent-ab12.onrender.com/a2a/
```

**That is what you paste into the assignment page.** Include the trailing slash.

Optional but safer: in Render, add one more environment variable
`PUBLIC_BASE_URL` = that exact URL (with `/a2a/`), then let it redeploy. This guarantees the
Agent Card advertises exactly what you submit.

Quick check in your browser: open
`https://invoice-agent-ab12.onrender.com/.well-known/agent-card.json`
You should see JSON whose `supportedInterfaces` → `url` equals your base URL exactly.

## Step 6 — Test before spending a Check

On your computer (needs Python): open a terminal in the folder with `test_agent.py`, then run

```
pip install requests
python test_agent.py https://invoice-agent-ab12.onrender.com/a2a/
```

You want `0 failed`. The two sample invoices should come out as `settle_invoice` and
`request_approval`. This uses 2 Gemini calls and none of your grader Checks.

## Step 7 — Run a Check on the assignment page

- **Wake the app first.** Free Render apps sleep after 15 minutes idle, and waking takes up to a minute —
  longer than the grader's 45-second limit. Open `https://YOUR-APP.onrender.com/healthz` in a
  browser, wait for `ok`, then click Check.
  (Optional: a free monitor at https://uptimerobot.com pinging `/healthz` every 5 minutes keeps it awake.)
- You have 10 Checks per 24 hours, and the last one is reserved for Save. Read the feedback codes after
  each Check before trying again.
- The first Check is the slowest because every package goes to Gemini. Later Checks reuse saved
  decisions for the core packages, and only the new audit packages call Gemini.

---

## If the feedback shows problems

| Feedback is about | Try |
|---|---|
| Evidence references | Set env var `EVIDENCE_KEEP_BRACKETS=1` (returns `[REF-1]` with brackets instead of `REF-1`). Change only this one thing, then Check again. |
| Timeouts on the first Check | Set `GEMINI_THINKING_BUDGET=0` (faster, slightly less careful). |
| Decision accuracy | Set `GEMINI_THINKING_BUDGET=4096` (slower, more careful) — only if timing is fine. |
| Gemini model name errors | Check the Render **Logs** tab for `[gemini]` lines; set `GEMINI_MODEL` to a current model name from https://ai.google.dev/gemini-api/docs/models |
| Authentication | If the assignment page gives you a specific token, you can set `ALLOWED_TOKENS` to it. By default every token is accepted and treated as its own separate user, which the isolation tests expect. |

Important: the saved decisions are cached in the database. If you change a setting that affects
decisions (brackets, thinking, model), clear the old ones in the Neon **SQL Editor**:

```sql
DELETE FROM decisions;
```

Never delete the `tasks` or `msgs` tables during grading.

## What each file does

- `app.py` — the whole agent: Agent Card, routes, authentication, task storage, duplicate-request
  protection, Gemini decisions, receipts, cancellation.
- `requirements.txt` — the Python packages Render installs.
- `render.yaml` — optional Render blueprint with the same settings as Step 4.
- `test_agent.py` — the pre-Check smoke test.
