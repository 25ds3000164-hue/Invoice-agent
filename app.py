"""
Invoice Action Agent - A2A 1.0 HTTP+JSON server.

Layers:
  1. Protocol (routing, auth, version, media types, envelopes)
  2. Storage  (tasks, message idempotency records, decision cache)
  3. Decision (Gemini, one batched call per batch, cached per package content)
"""
import asyncio
import datetime
import hashlib
import json
import os
import re
import sqlite3
import time
import uuid
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

# ---------------------------------------------------------------- constants
A2A_MT = "application/a2a+json"
BATCH_MT = "application/vnd.ga5.invoice-claim-batch+json"
RESULTS_MT = "application/vnd.ga5.invoice-action-results+json"
PROPOSALS_MT = "application/vnd.ga5.invoice-action-proposals+json"
RECEIPTS_MT = "application/vnd.ga5.invoice-action-receipts+json"

ACTIONS = ["settle_invoice", "request_approval", "hold_invoice",
           "reject_duplicate", "open_exception"]

S_INPUT = "TASK_STATE_INPUT_REQUIRED"
S_DONE = "TASK_STATE_COMPLETED"
S_CANCEL = "TASK_STATE_CANCELED"
TERMINAL = {S_DONE, S_CANCEL, "TASK_STATE_FAILED", "TASK_STATE_REJECTED"}

MAX_RESPONSE = 512 * 1024
REQUEST_BUDGET = float(os.environ.get("REQUEST_BUDGET_SECONDS", "40"))

GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_FALLBACK_MODEL = os.environ.get("GEMINI_FALLBACK_MODEL", "gemini-2.5-flash-lite")
GEMINI_THINKING = int(os.environ.get("GEMINI_THINKING_BUDGET", "1024"))
KEEP_BRACKETS = os.environ.get("EVIDENCE_KEEP_BRACKETS", "0") == "1"
ALLOWED_TOKENS = {t.strip() for t in os.environ.get("ALLOWED_TOKENS", "").split(",") if t.strip()}


def _base_url() -> str:
    u = os.environ.get("PUBLIC_BASE_URL", "").strip()
    if not u:
        ext = os.environ.get("RENDER_EXTERNAL_URL", "http://localhost:8000")
        u = ext.rstrip("/") + "/a2a/"
    return u


BASE_URL = _base_url()
BASE_PREFIX = "/" + urlparse(BASE_URL).path.strip("/")
if BASE_PREFIX == "/":
    BASE_PREFIX = ""


def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- storage
class Store:
    """Tiny SQL store. Postgres when DATABASE_URL is set, otherwise SQLite."""

    def __init__(self):
        self.url = os.environ.get("DATABASE_URL", "").strip()
        self.pg = self.url.startswith("postgres")
        self.conn = None
        self._connect()
        self._schema()

    def _connect(self):
        if self.pg:
            import psycopg
            self.conn = psycopg.connect(self.url, autocommit=True, connect_timeout=10)
        else:
            path = os.environ.get("SQLITE_PATH", "agent.db")
            self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")

    def _sql(self, q):
        return q.replace("?", "%s") if self.pg else q

    def _with_retry(self, fn):
        try:
            return fn()
        except Exception:
            if not self.pg:
                raise
            # Neon may drop idle connections; reconnect once.
            try:
                self.conn.close()
            except Exception:
                pass
            self._connect()
            return fn()

    def _schema(self):
        stmts = [
            "CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, principal TEXT NOT NULL, "
            "context_id TEXT, state TEXT, created DOUBLE PRECISION, data TEXT NOT NULL)",
            "CREATE INDEX IF NOT EXISTS tasks_principal ON tasks (principal, created)",
            "CREATE TABLE IF NOT EXISTS msgs (principal TEXT NOT NULL, message_id TEXT NOT NULL, "
            "hash TEXT NOT NULL, task_id TEXT NOT NULL, PRIMARY KEY (principal, message_id))",
            "CREATE TABLE IF NOT EXISTS decisions (key TEXT PRIMARY KEY, data TEXT NOT NULL)",
        ]
        for s in stmts:
            self._with_retry(lambda s=s: self.conn.execute(s))

    def query(self, q, params=()):
        def run():
            cur = self.conn.execute(self._sql(q), params)
            return cur.fetchall()
        return self._with_retry(run)

    def tx(self, statements):
        """Run several (sql, params) atomically."""
        def run():
            if self.pg:
                with self.conn.transaction():
                    for q, p in statements:
                        self.conn.execute(self._sql(q), p)
            else:
                self.conn.execute("BEGIN IMMEDIATE")
                try:
                    for q, p in statements:
                        self.conn.execute(q, p)
                    self.conn.execute("COMMIT")
                except Exception:
                    self.conn.execute("ROLLBACK")
                    raise
        self._with_retry(run)

    # task helpers
    def get_task(self, principal, task_id):
        rows = self.query("SELECT data FROM tasks WHERE id = ? AND principal = ?", (task_id, principal))
        return json.loads(rows[0][0]) if rows else None

    @staticmethod
    def task_stmt(principal, task, created=None):
        return ("INSERT INTO tasks (id, principal, context_id, state, created, data) VALUES (?,?,?,?,?,?) "
                "ON CONFLICT (id) DO UPDATE SET state = excluded.state, data = excluded.data",
                (task["id"], principal, task["contextId"], task["status"]["state"],
                 created if created is not None else time.time(), json.dumps(task, ensure_ascii=False)))

    @staticmethod
    def msg_stmt(principal, message_id, h, task_id):
        return ("INSERT INTO msgs (principal, message_id, hash, task_id) VALUES (?,?,?,?)",
                (principal, message_id, h, task_id))

    def get_msg(self, principal, message_id):
        rows = self.query("SELECT hash, task_id FROM msgs WHERE principal = ? AND message_id = ?",
                          (principal, message_id))
        return rows[0] if rows else None

    def list_tasks(self, principal):
        rows = self.query("SELECT data FROM tasks WHERE principal = ? ORDER BY created DESC", (principal,))
        return [json.loads(r[0]) for r in rows]

    def get_decision(self, key):
        rows = self.query("SELECT data FROM decisions WHERE key = ?", (key,))
        return json.loads(rows[0][0]) if rows else None

    @staticmethod
    def decision_stmt(key, data):
        return ("INSERT INTO decisions (key, data) VALUES (?,?) ON CONFLICT (key) DO NOTHING",
                (key, json.dumps(data, ensure_ascii=False)))


STORE = Store()
LOCKS = defaultdict(asyncio.Lock)

# ---------------------------------------------------------------- decision layer
REF_RE = re.compile(r"\[([^\[\]\r\n]{1,120})\]")
CURRENCY_EXP = {"JPY": 0, "KRW": 0, "VND": 0, "CLP": 0, "ISK": 0, "KWD": 3, "BHD": 3, "OMR": 3,
                "JOD": 3, "TND": 3, "LYD": 3, "IQD": 3}


def norm_ref(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    return s


def out_ref(s: str) -> str:
    return f"[{s}]" if KEEP_BRACKETS else s


def dedupe(xs):
    seen, out = set(), []
    for x in xs:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def render_package(pkg):
    """Flatten a package into labelled text. Paragraphs get <<Pn>> labels so the
    model can point at the decisive one and we copy its references exactly."""
    lines, paras = [], {}
    counter = [0]

    def add_para(text):
        text = text.strip()
        if not text:
            return
        counter[0] += 1
        pid = f"P{counter[0]}"
        refs = dedupe([norm_ref(m.group(0)) for m in REF_RE.finditer(text)])
        paras[pid] = {"text": text, "refs": refs}
        lines.append(f"  <<{pid}>> {text}")

    def walk(v, path):
        if isinstance(v, dict):
            for k, x in v.items():
                walk(x, f"{path}.{k}" if path else str(k))
        elif isinstance(v, list):
            for i, x in enumerate(v):
                walk(x, f"{path}[{i}]")
        elif isinstance(v, str):
            if "\n" in v or len(v) > 160 or REF_RE.search(v):
                lines.append(f"{path}:")
                if re.search(r"\n\s*\n", v):
                    chunks = re.split(r"\n\s*\n", v)
                elif "\n" in v and len(v) > 600:
                    chunks = v.split("\n")
                else:
                    chunks = [v]
                for c in chunks:
                    add_para(c)
            else:
                lines.append(f"{path}: {v}")
        else:
            lines.append(f"{path}: {json.dumps(v)}")

    walk(pkg, "")
    return "\n".join(lines), paras


def all_refs(paras):
    s = set()
    for p in paras.values():
        s.update(p["refs"])
    return s


def parse_amount_minor(amount, currency, fallback=None):
    exp = CURRENCY_EXP.get((currency or "").upper(), 2)
    if isinstance(amount, (int, float)) and not isinstance(amount, bool):
        try:
            return int((Decimal(str(amount)) * (Decimal(10) ** exp)).quantize(Decimal(1)))
        except InvalidOperation:
            return fallback
    s = re.sub(r"[^0-9.,\-]", "", str(amount or ""))
    if not re.search(r"\d", s):
        return fallback
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) == 2:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    try:
        return int((Decimal(s) * (Decimal(10) ** exp)).quantize(Decimal(1)))
    except InvalidOperation:
        return fallback


def find_field(pkg, *needles):
    """Find a short scalar value whose key contains all needles (fallback facts)."""
    found = []

    def walk(v, key):
        if isinstance(v, dict):
            for k, x in v.items():
                walk(x, str(k).lower())
        elif isinstance(v, list):
            for x in v:
                walk(x, key)
        elif v is not None and not isinstance(v, bool) and all(n in key for n in needles):
            if not isinstance(v, str) or len(v) < 120:
                found.append(v)
    walk(pkg, "")
    return found[0] if found else None


SYSTEM_PROMPT = """You are an accounts-payable control agent. For EACH invoice package choose exactly one action:

- settle_invoice: invoice is valid, reconciled (PO / receipt / invoice agree), within the agent's autonomous payment authority under the CURRENT policy, and nothing requires a hold, approval, duplicate rejection or exception.
- request_approval: commercially valid and reconciled, but outside delegated/autonomous authority (e.g. amount above the current policy limit, or policy explicitly requires a named approver).
- hold_invoice: payment must pause until a specific stated verification completes (e.g. bank-detail change awaiting call-back, tax/registration check pending, goods receipt pending confirmation).
- reject_duplicate: the SAME commercial invoice (same vendor, same invoice number, same amount) was ALREADY PAID. A payment for a different invoice number or different amount is NOT a duplicate.
- open_exception: material records conflict (e.g. invoice vs PO/receipt amount, quantity, vendor or bank mismatch that is not explained) and need an exception workflow.

Traps to ignore:
- Archived/historical examples, training material, sample or template text, "for illustration" cases, and quoted older policy revisions are decoys. Only the live facts of THIS invoice and the CURRENT policy revision count.
- Negation matters: "no hold is required", "this is not a duplicate", "approval is not needed" mean the opposite of the action word they contain.
- Action words in irrelevant contexts (e.g. a subject line saying "pay now", a cover sheet summary) do not decide anything.
- The cover sheet is a summary only; never choose the cover-sheet paragraph as decisive.

Paragraphs are labelled <<P1>>, <<P2>> ... Pick the ONE paragraph that actually determines the action (the decisive live fact). It normally contains exactly three bracketed references like [XYZ-123]. Copy those three references exactly into evidenceRefs.

Facts: vendorName and invoiceNumber exactly as on the live invoice; amountMajor = the invoice total in major units as a plain decimal string without separators (e.g. "125000.50"); currency as an ISO 4217 code.
reason: 1-3 sentences explaining why this action applies, in your own words.
Return one decision per package key, using the package keys given (K1, K2, ...)."""

RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "decisions": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "packageKey": {"type": "STRING"},
                    "action": {"type": "STRING", "enum": ACTIONS},
                    "decisiveParagraph": {"type": "STRING"},
                    "evidenceRefs": {"type": "ARRAY", "items": {"type": "STRING"}},
                    "vendorName": {"type": "STRING"},
                    "invoiceNumber": {"type": "STRING"},
                    "amountMajor": {"type": "STRING"},
                    "currency": {"type": "STRING"},
                    "reason": {"type": "STRING"},
                },
                "required": ["packageKey", "action", "decisiveParagraph", "evidenceRefs",
                             "vendorName", "invoiceNumber", "amountMajor", "currency", "reason"],
            },
        }
    },
    "required": ["decisions"],
}


async def call_gemini(model, prompt, timeout, thinking):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    gen = {"responseMimeType": "application/json", "responseSchema": RESPONSE_SCHEMA, "temperature": 0}
    if thinking is not None:
        gen["thinkingConfig"] = {"thinkingBudget": thinking}
    body = {"systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": gen}
    import httpx  # imported lazily so the server starts even if httpx is missing locally
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, json=body, headers={"x-goog-api-key": GEMINI_KEY})
        if r.status_code == 400 and thinking is not None and "thinking" in r.text.lower():
            gen.pop("thinkingConfig", None)
            r = await client.post(url, json=body, headers={"x-goog-api-key": GEMINI_KEY})
        r.raise_for_status()
        data = r.json()
    parts = data["candidates"][0]["content"]["parts"]
    text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
    return json.loads(text)["decisions"]


def heuristic_decision(paras):
    """Keyword fallback used only if the model is unavailable. Not cached."""
    bad = re.compile(r"archiv|training|example|illustrat|sample|template|cover sheet|superseded", re.I)
    cands = [(pid, p) for pid, p in paras.items() if len(p["refs"]) == 3 and not bad.search(p["text"])]
    if not cands:
        cands = [(pid, p) for pid, p in paras.items() if p["refs"]]
    rules = [("reject_duplicate", r"already paid|duplicate"),
             ("open_exception", r"mismatch|conflict|discrepanc|does not match"),
             ("hold_invoice", r"verif|pending|hold|call-?back"),
             ("request_approval", r"exceed|above .*limit|approval|approver")]
    for action, rx in rules:
        for pid, p in cands:
            if re.search(rx, p["text"], re.I) and not re.search(r"\bnot\b|\bno\b", p["text"], re.I):
                return action, pid
    return "settle_invoice", (cands[0][0] if cands else None)


def choose_refs(paras, decisive, model_refs):
    p = paras.get((decisive or "").strip().strip("<>"))
    known = all_refs(paras)
    mrefs = dedupe([norm_ref(r) for r in (model_refs or []) if norm_ref(r) in known])
    if p and len(p["refs"]) == 3:
        return p["refs"]
    if p and len(mrefs) == 3 and all(r in p["refs"] for r in mrefs):
        return mrefs
    if len(mrefs) == 3:
        return mrefs
    if p and p["refs"]:
        inter = [r for r in mrefs if r in p["refs"]]
        return (inter + [r for r in p["refs"] if r not in inter])[:3]
    return mrefs[:3]


def build_rationale(action, reason, refs):
    shown = [out_ref(r) for r in refs]
    reason = re.sub(r"\s+", " ", (reason or "")).strip()
    head = f"Proposed action: {action}. "
    tail = f" Decisive evidence: {', '.join(shown)}." if shown else ""
    room = 1500 - len(head) - len(tail) - 5
    if len(reason) > room:
        reason = reason[:room].rstrip() + "..."
    text = head + reason + tail
    if len(text) < 60:
        text += " The decision follows the live invoice records and the current policy revision."
    return text[:1500]


def package_cache_key(policy_revision, pkg):
    stripped = {k: v for k, v in pkg.items() if k not in ("packageId", "id")}
    return sha(canon({"policyRevision": policy_revision, "package": stripped}))


async def decide_batch(batch, deadline):
    """Return {packageId: decision dict}. Cached per canonical package content."""
    policy = batch.get("policyRevision")
    packages = batch["packages"]
    results, todo, new_cache = {}, [], []
    rendered = {}
    for pkg in packages:
        pid = pkg["packageId"]
        key = package_cache_key(policy, pkg)
        cached = STORE.get_decision(key)
        if cached:
            results[pid] = cached
        else:
            text, paras = render_package(pkg)
            rendered[pid] = (text, paras, key, pkg)
            todo.append(pid)

    model_out = {}
    if todo and GEMINI_KEY:
        extra = {k: v for k, v in batch.items() if k not in ("packages",)}
        blocks = [f"Batch context: {canon(extra)}"]
        keymap = {}
        for i, pid in enumerate(todo, 1):
            k = f"K{i}"
            keymap[k] = pid
            blocks.append(f"\n===== PACKAGE {k} =====\n{rendered[pid][0]}")
        prompt = "\n".join(blocks)
        attempts = [(GEMINI_MODEL, GEMINI_THINKING), (GEMINI_FALLBACK_MODEL, 0)]
        for model, thinking in attempts:
            remaining = deadline - time.monotonic()
            if remaining < 6:
                break
            try:
                decs = await call_gemini(model, prompt, timeout=remaining - 2, thinking=thinking)
                for d in decs:
                    pid = keymap.get(str(d.get("packageKey", "")).strip())
                    if pid and d.get("action") in ACTIONS:
                        model_out[pid] = d
                if model_out:
                    break
            except Exception as e:  # noqa: BLE001
                print(f"[gemini] {model} failed: {type(e).__name__}: {str(e)[:200]}", flush=True)

    for pid in todo:
        text, paras, key, pkg = rendered[pid]
        d = model_out.get(pid)
        if d:
            action = d["action"]
            refs = choose_refs(paras, d.get("decisiveParagraph"), d.get("evidenceRefs"))
            currency = (d.get("currency") or "").strip().upper()[:3]
            amount = parse_amount_minor(d.get("amountMajor"), currency)
            facts = {"vendorName": (d.get("vendorName") or "").strip(),
                     "invoiceNumber": (d.get("invoiceNumber") or "").strip(),
                     "amountMinor": amount if amount is not None else 0,
                     "currency": currency}
            reason = d.get("reason", "")
            dec = {"action": action, "facts": facts, "evidenceRefs": refs, "reason": reason}
            if facts["vendorName"] and facts["invoiceNumber"] and len(refs) == 3:
                new_cache.append(Store.decision_stmt(key, dec))
        else:
            action, para = heuristic_decision(paras)
            refs = choose_refs(paras, para, [])
            def scan(rx):
                m = re.search(rx, text, re.I)
                return m.group(1).strip() if m else None
            currency = str(find_field(pkg, "currency") or scan(r"\b(INR|USD|EUR|GBP|SGD|AED|JPY)\b")
                           or "INR").upper()[:3]
            amt = (find_field(pkg, "amount") or find_field(pkg, "total")
                   or scan(r"(?:total|amount)[^0-9\n]{0,20}([0-9][0-9,]*(?:\.[0-9]+)?)"))
            facts = {"vendorName": str(find_field(pkg, "vendor", "name") or scan(r"vendor(?: name)?\s*[:\-]\s*([^\n]+)")
                                       or "unknown"),
                     "invoiceNumber": str(find_field(pkg, "invoice", "number") or find_field(pkg, "invoice", "no")
                                          or scan(r"invoice (?:number|no\.?|#)\s*[:\-]?\s*([A-Za-z0-9\-/]+)")
                                          or "unknown"),
                     "amountMinor": parse_amount_minor(amt, currency, 0) or 0,
                     "currency": currency}
            dec = {"action": action, "facts": facts, "evidenceRefs": refs,
                   "reason": "Fallback rule-based review of the decisive paragraph because the model was unavailable."}
        results[pid] = dec
    return results, new_cache


# ---------------------------------------------------------------- protocol helpers
def a2a_json(body, status=200):
    return JSONResponse(body, status_code=status, media_type=A2A_MT)


def err(status, code, message):
    return JSONResponse({"code": code, "message": message,
                         "error": {"code": code, "status": status, "message": message}},
                        status_code=status, media_type=A2A_MT)


def not_found():
    return err(404, "TASK_NOT_FOUND", "Task not found.")


def principal_of(request: Request):
    h = request.headers.get("authorization", "")
    m = re.fullmatch(r"[Bb]earer ([^\s]+)", h.strip())
    if not m:
        return None
    token = m.group(1)
    if ALLOWED_TOKENS and token not in ALLOWED_TOKENS:
        return None
    return sha("principal:" + token)


def check_common(request: Request, needs_body: bool):
    principal = principal_of(request)
    if principal is None:
        return None, err(401, "UNAUTHENTICATED", "Missing or invalid bearer token.")
    if request.headers.get("a2a-version", "").strip() != "1.0":
        return None, err(400, "VERSION_NOT_SUPPORTED", "A2A-Version 1.0 is required.")
    if needs_body:
        ct = request.headers.get("content-type", "").split(";")[0].strip().lower()
        if ct != A2A_MT:
            return None, err(415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/a2a+json.")
    return principal, None


def view(task, history_length=None):
    t = dict(task)
    if history_length is not None:
        h = task.get("history", [])
        t["history"] = [] if history_length <= 0 else h[-history_length:]
    return t


def parse_hl(v):
    try:
        return int(v) if v is not None and str(v) != "" else None
    except ValueError:
        return None


def agent_card():
    return {
        "name": "Invoice Action Agent",
        "description": "Reads invoice claim packages, proposes one evidence-backed business action per invoice, "
                       "and executes only grader-accepted actions with durable, replay-safe receipts.",
        "version": "1.0.0",
        "supportedInterfaces": [{"url": BASE_URL, "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"}],
        "provider": {"organization": "Invoice Agent", "url": BASE_URL},
        "capabilities": {"streaming": False, "pushNotifications": False, "extendedAgentCard": False},
        "securitySchemes": {"bearer": {"httpAuthSecurityScheme": {"scheme": "Bearer"}}},
        "securityRequirements": [{"schemes": {"bearer": {"list": []}}}],
        "defaultInputModes": [BATCH_MT, RESULTS_MT],
        "defaultOutputModes": [PROPOSALS_MT, RECEIPTS_MT],
        "skills": [{
            "id": "invoice_action_agent",
            "name": "Invoice action agent",
            "description": "Chooses settle, approval, hold, duplicate rejection or exception for each invoice package, "
                           "cites decisive evidence, and records receipts for accepted actions only.",
            "tags": ["invoice", "accounts-payable", "approval", "duplicate-payment", "evidence"],
            "inputModes": [BATCH_MT, RESULTS_MT],
            "outputModes": [PROPOSALS_MT, RECEIPTS_MT],
        }],
    }


# ---------------------------------------------------------------- handlers
def get_data_part(message, media_type):
    parts = message.get("parts")
    if not isinstance(parts, list) or len(parts) != 1:
        return None
    p = parts[0]
    if not isinstance(p, dict) or p.get("mediaType") != media_type or not isinstance(p.get("data"), dict):
        return None
    return p["data"]


async def handle_send(request: Request, principal: str):
    try:
        body = json.loads(await request.body())
    except Exception:
        return err(400, "INVALID_REQUEST", "Body must be JSON.")
    if not isinstance(body, dict) or not isinstance(body.get("message"), dict):
        return err(400, "INVALID_REQUEST", "A message object is required.")
    message = body["message"]
    config = body.get("configuration") if isinstance(body.get("configuration"), dict) else {}
    hl = parse_hl(config.get("historyLength"))
    message_id = message.get("messageId")
    if not isinstance(message_id, str) or not message_id:
        return err(400, "INVALID_REQUEST", "messageId is required.")
    if message.get("role") != "ROLE_USER":
        return err(400, "INVALID_REQUEST", "role must be ROLE_USER.")
    msg_hash = sha(canon(message))

    async with LOCKS[f"msg:{principal}:{message_id}"]:
        seen = STORE.get_msg(principal, message_id)
        if seen:
            if seen[0] != msg_hash:
                return err(409, "IDEMPOTENCY_CONFLICT", "messageId was already used with different content.")
            task = STORE.get_task(principal, seen[1])
            if task is None:
                return not_found()
            return a2a_json({"task": view(task, hl)})

        if message.get("taskId"):
            return await continue_task(principal, message, msg_hash, hl)
        return await new_task(principal, message, msg_hash, hl)


async def new_task(principal, message, msg_hash, hl):
    deadline = time.monotonic() + REQUEST_BUDGET
    batch = get_data_part(message, BATCH_MT)
    if batch is None:
        return err(400, "INVALID_REQUEST", "Expected one invoice-claim-batch data part.")
    pkgs = batch.get("packages")
    if not isinstance(batch.get("batchId"), str) or not isinstance(pkgs, list) or not pkgs:
        return err(400, "INVALID_REQUEST", "batchId and packages are required.")
    ids = [p.get("packageId") if isinstance(p, dict) else None for p in pkgs]
    if any(not isinstance(i, str) or not i for i in ids) or len(set(ids)) != len(ids):
        return err(400, "INVALID_REQUEST", "Each package needs a unique packageId.")

    decisions, cache_stmts = await decide_batch(batch, deadline)

    task_id = uuid.uuid4().hex
    context_id = message.get("contextId") if isinstance(message.get("contextId"), str) and message.get(
        "contextId") else uuid.uuid4().hex
    proposals = []
    for pkg in pkgs:
        d = decisions[pkg["packageId"]]
        refs = [out_ref(r) for r in d["evidenceRefs"]]
        proposals.append({
            "packageId": pkg["packageId"],
            "actionId": f"act_{uuid.uuid4().hex}",
            "action": d["action"],
            "facts": d["facts"],
            "evidenceRefs": refs,
            "rationale": build_rationale(d["action"], d.get("reason"), d["evidenceRefs"]),
        })
    stored_msg = dict(message)
    stored_msg["taskId"] = task_id
    stored_msg["contextId"] = context_id
    task = {
        "id": task_id,
        "contextId": context_id,
        "status": {
            "state": S_INPUT,
            "timestamp": now_iso(),
            "message": {"messageId": f"status_{task_id}", "role": "ROLE_AGENT", "taskId": task_id,
                        "contextId": context_id,
                        "parts": [{"text": "Proposals ready. Send action results to continue."}]},
        },
        "artifacts": [{
            "artifactId": f"proposals_{task_id}",
            "name": "invoice-action-proposals",
            "parts": [{"mediaType": PROPOSALS_MT, "data": {"batchId": batch["batchId"], "proposals": proposals}}],
        }],
        "history": [stored_msg],
        "metadata": {"batchId": batch["batchId"]},
    }
    STORE.tx(cache_stmts + [Store.task_stmt(principal, task),
                            Store.msg_stmt(principal, message["messageId"], msg_hash, task_id)])
    return a2a_json({"task": view(task, hl)})


async def continue_task(principal, message, msg_hash, hl):
    task_id = message.get("taskId")
    async with LOCKS[f"task:{task_id}"]:
        task = STORE.get_task(principal, task_id)
        if task is None:
            return not_found()
        if message.get("contextId") != task["contextId"]:
            return err(400, "INVALID_REQUEST", "contextId does not match the task.")
        if task["status"]["state"] in TERMINAL:
            return err(409, "TASK_NOT_CONTINUABLE", "Task is in a terminal state.")
        if task["status"]["state"] != S_INPUT:
            return err(409, "TASK_NOT_CONTINUABLE", "Task is not awaiting input.")
        data = get_data_part(message, RESULTS_MT)
        if data is None:
            return err(400, "INVALID_REQUEST", "Expected one invoice-action-results data part.")
        prop_data = task["artifacts"][0]["parts"][0]["data"]
        if data.get("batchId") != prop_data["batchId"]:
            return err(400, "INVALID_REQUEST", "batchId does not match the task.")
        results = data.get("results")
        if not isinstance(results, list) or not results:
            return err(400, "INVALID_REQUEST", "results are required.")
        by_pkg = {p["packageId"]: p for p in prop_data["proposals"]}
        seen, executions = set(), []
        for r in results:
            if not isinstance(r, dict):
                return err(400, "INVALID_REQUEST", "Malformed result.")
            pid = r.get("packageId")
            prop = by_pkg.get(pid)
            if prop is None or pid in seen:
                return err(400, "INVALID_REQUEST", "Unknown or repeated packageId.")
            seen.add(pid)
            if r.get("actionId") != prop["actionId"] or r.get("action") != prop["action"]:
                return err(400, "INVALID_REQUEST", "actionId or action does not match the proposal.")
            outcome = r.get("outcome")
            if outcome not in ("ACCEPTED", "REJECTED"):
                return err(400, "INVALID_REQUEST", "outcome must be ACCEPTED or REJECTED.")
            if outcome == "ACCEPTED":
                nonce = r.get("receiptNonce")
                if not isinstance(nonce, str) or not nonce:
                    return err(400, "INVALID_REQUEST", "Accepted results need a receiptNonce.")
                executions.append({
                    "packageId": pid, "actionId": prop["actionId"], "action": prop["action"],
                    "receiptNonce": nonce, "facts": prop["facts"], "evidenceRefs": prop["evidenceRefs"],
                })
        stored_msg = dict(message)
        task = json.loads(json.dumps(task))
        task["history"].append(stored_msg)
        task["artifacts"].append({
            "artifactId": f"receipts_{task_id}",
            "name": "invoice-action-receipts",
            "parts": [{"mediaType": RECEIPTS_MT, "data": {"batchId": prop_data["batchId"],
                                                          "executions": executions}}],
        })
        task["status"] = {"state": S_DONE, "timestamp": now_iso()}
        STORE.tx([Store.task_stmt(principal, task),
                  Store.msg_stmt(principal, message["messageId"], msg_hash, task_id)])
        return a2a_json({"task": view(task, hl)})


async def handle_cancel(principal, task_id):
    async with LOCKS[f"task:{task_id}"]:
        task = STORE.get_task(principal, task_id)
        if task is None:
            return not_found()
        if task["status"]["state"] in TERMINAL:
            return err(409, "TASK_NOT_CANCELABLE", "Task is in a terminal state.")
        task["status"] = {"state": S_CANCEL, "timestamp": now_iso()}
        STORE.tx([Store.task_stmt(principal, task)])
        return a2a_json(task)


def handle_list(request: Request, principal):
    q = request.query_params
    tasks = STORE.list_tasks(principal)
    if q.get("contextId"):
        tasks = [t for t in tasks if t["contextId"] == q["contextId"]]
    if q.get("status"):
        tasks = [t for t in tasks if t["status"]["state"] == q["status"]]
    total = len(tasks)
    try:
        page_size = max(1, min(100, int(q.get("pageSize", 50))))
    except ValueError:
        page_size = 50
    try:
        offset = int(q.get("pageToken") or 0)
    except ValueError:
        offset = 0
    hl = parse_hl(q.get("historyLength"))
    out, size, i = [], 200, offset
    for t in tasks[offset:offset + page_size]:
        v = view(t, hl)
        if q.get("includeArtifacts") in ("false", "0"):
            v.pop("artifacts", None)
        s = len(json.dumps(v, ensure_ascii=False).encode())
        if out and size + s > MAX_RESPONSE - 16 * 1024:
            break
        out.append(v)
        size += s
        i += 1
    next_token = str(i) if i < total else ""
    return a2a_json({"tasks": out, "nextPageToken": next_token, "pageSize": page_size, "totalSize": total})


# ---------------------------------------------------------------- app
async def dispatch(request: Request):
    path = re.sub(r"/+", "/", "/" + request.path_params.get("full_path", ""))
    method = request.method

    if path == "/.well-known/agent-card.json" and method in ("GET", "HEAD"):
        return JSONResponse(agent_card(), media_type="application/json")
    if path in ("/", "/healthz") and method in ("GET", "HEAD"):
        return PlainTextResponse("ok")

    if not (path == BASE_PREFIX or path.startswith(BASE_PREFIX + "/")):
        return err(404, "NOT_FOUND", "Not found.")
    rest = path[len(BASE_PREFIX):].strip("/")

    if rest == "message:send" and method == "POST":
        principal, e = check_common(request, needs_body=True)
        return e or await handle_send(request, principal)
    if rest == "tasks" and method == "GET":
        principal, e = check_common(request, needs_body=False)
        return e or handle_list(request, principal)
    if rest.startswith("tasks/"):
        tail = rest[len("tasks/"):]
        if method == "POST" and tail.endswith(":cancel") and "/" not in tail:
            raw = await request.body()
            principal, e = check_common(request, needs_body=bool(raw.strip()))
            return e or await handle_cancel(principal, tail[:-len(":cancel")])
        if method == "GET" and tail and "/" not in tail and ":" not in tail:
            principal, e = check_common(request, needs_body=False)
            if e:
                return e
            task = STORE.get_task(principal, tail)
            if task is None:
                return not_found()
            return a2a_json(view(task, parse_hl(request.query_params.get("historyLength"))))
    return err(404, "NOT_FOUND", "Not found.")


app = Starlette(routes=[Route("/{full_path:path}", dispatch,
                              methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"])])
