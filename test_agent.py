"""
Smoke test for the invoice agent. Run it BEFORE spending a grader Check.

    python test_agent.py https://YOUR-APP.onrender.com/a2a/

It uses throwaway test tokens and two tiny synthetic packages (one model call per new task).
"""
import concurrent.futures
import json
import sys
import uuid
from urllib.parse import urlparse

import requests

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/a2a/").rstrip("/") + "/"
ORIGIN = "{0.scheme}://{0.netloc}".format(urlparse(BASE))
TOKEN = "smoke-" + uuid.uuid4().hex
OUTSIDER = "outsider-" + uuid.uuid4().hex
c = requests.Session()
c.request = (lambda f: (lambda *a, **k: f(*a, timeout=k.pop('timeout', 60), **k)))(c.request)
ok_count = fail_count = 0


def H(token=TOKEN, version="1.0", ct="application/a2a+json"):
    h = {"A2A-Version": version, "Content-Type": ct}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def check(name, cond, extra=""):
    global ok_count, fail_count
    if cond:
        ok_count += 1
        print(f"  PASS  {name}")
    else:
        fail_count += 1
        print(f"  FAIL  {name} {extra}")


def pkg(pid, inv, amount, decisive):
    return {"packageId": pid, "documents": [
        {"type": "cover_sheet", "text": f"Cover sheet for invoice {inv}. Ref [CVR-{pid}]. Please process."},
        {"type": "invoice", "text": f"Vendor: Acme Supplies Pvt Ltd\nInvoice number: {inv}\nTotal: INR {amount}"},
        {"type": "archive", "text": "Archived training example from 2019: invoice X-1 was a duplicate "
                                    "[OLD-1] [OLD-2] [OLD-3]. Do not use for live decisions."},
        {"type": "review_notes", "text": decisive},
    ]}


batch = {"batchId": "smoke-" + uuid.uuid4().hex[:8], "policyRevision": "rev-smoke",
         "packages": [
             pkg("p1", "INV-1001", "45,000.00",
                 "Three-way match passed [PO-77] [GRN-77] [INV-1001-L]. Amount is within the INR 1,00,000 "
                 "autonomous limit and no holds apply."),
             pkg("p2", "INV-2002", "4,50,000.00",
                 "Records reconcile [PO-88] [GRN-88] [INV-2002-L], but INR 4,50,000 exceeds the INR 1,00,000 "
                 "autonomous limit, so a finance controller must approve."),
         ]}

print(f"Testing {BASE}")
r = c.get(ORIGIN + "/.well-known/agent-card.json")
card = r.json() if r.status_code == 200 else {}
check("agent card loads", r.status_code == 200)
urls = [i.get("url") for i in card.get("supportedInterfaces", [])]
check("card interface URL equals base URL", BASE in urls, f"card has {urls}")

msg = {"messageId": "m-" + uuid.uuid4().hex, "role": "ROLE_USER",
       "parts": [{"mediaType": "application/vnd.ga5.invoice-claim-batch+json", "data": batch}]}
cfg = {"returnImmediately": False, "historyLength": 20,
       "acceptedOutputModes": ["application/vnd.ga5.invoice-action-proposals+json",
                               "application/vnd.ga5.invoice-action-receipts+json"]}
send = BASE + "message:send"

check("no token -> 401/403", c.post(send, headers=H(token=None), json={"message": msg}).status_code in (401, 403))
check("wrong version -> 400", c.post(send, headers=H(version="0.3"), json={"message": msg}).status_code == 400)

r = c.post(send, headers=H(), json={"message": msg, "configuration": cfg})
check("send -> 200", r.status_code == 200, r.text[:300])
task = r.json()["task"]
check("state INPUT_REQUIRED", task["status"]["state"] == "TASK_STATE_INPUT_REQUIRED")
props = task["artifacts"][0]["parts"][0]["data"]["proposals"]
for p in props:
    print(f"        {p['packageId']}: {p['action']}  refs={p['evidenceRefs']}  facts={p['facts']}")

# replay with reordered keys + changed returnImmediately
msg_reordered = json.loads(json.dumps(msg, sort_keys=True))
cfg2 = dict(cfg, returnImmediately=True)
r2 = c.post(send, headers=H(), data=json.dumps({"configuration": cfg2, "message": msg_reordered}))
check("replay returns same task", r2.status_code == 200 and r2.json()["task"]["id"] == task["id"])

changed = json.loads(json.dumps(msg))
changed["parts"][0]["data"]["batchId"] = "different"
r3 = c.post(send, headers=H(), json={"message": changed})
check("changed content -> 409 IDEMPOTENCY_CONFLICT", r3.status_code == 409 and "IDEMPOTENCY_CONFLICT" in r3.text)

r = c.get(BASE + f"tasks/{task['id']}", headers=H(token=OUTSIDER))
check("outsider read -> 403/404", r.status_code in (403, 404) and task["id"] not in r.text)
r = c.get(BASE + "tasks", headers=H(token=OUTSIDER))
check("outsider list empty", r.status_code == 200 and r.json()["tasks"] == [])
r = c.get(BASE + "tasks", headers=H())
check("owner list has task", r.status_code == 200 and any(t["id"] == task["id"] for t in r.json()["tasks"]))


def results_msg(t, outcomes):
    pr = t["artifacts"][0]["parts"][0]["data"]["proposals"]
    return {"messageId": "r-" + uuid.uuid4().hex, "taskId": t["id"], "contextId": t["contextId"],
            "role": "ROLE_USER",
            "parts": [{"mediaType": "application/vnd.ga5.invoice-action-results+json",
                       "data": {"batchId": batch["batchId"], "results": [
                           {"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"],
                            "outcome": o, "receiptNonce": "nonce-" + uuid.uuid4().hex}
                           for p, o in zip(pr, outcomes)]}}]}


bad = results_msg(task, ["ACCEPTED", "REJECTED"])
bad["parts"][0]["data"]["results"][0]["actionId"] = "act_wrong_wrong_wrong"
check("wrong actionId rejected", c.post(send, headers=H(), json={"message": bad}).status_code in (400, 409, 422))
outsider_cont = results_msg(task, ["ACCEPTED", "REJECTED"])
check("outsider continuation rejected",
      c.post(send, headers=H(token=OUTSIDER), json={"message": outsider_cont}).status_code in (403, 404))

good = results_msg(task, ["ACCEPTED", "REJECTED"])
r = c.post(send, headers=H(), json={"message": good})
check("results -> COMPLETED", r.status_code == 200 and r.json()["task"]["status"]["state"] == "TASK_STATE_COMPLETED",
      r.text[:300])
done = r.json()["task"]
ex = done["artifacts"][-1]["parts"][0]["data"]["executions"]
check("exactly one execution (accepted only)", len(ex) == 1)
check("history has both messages", len(done["history"]) == 2)
r = c.post(send, headers=H(), json={"message": good})
check("results replay -> same completed task", r.status_code == 200 and r.json()["task"] == done)
check("cancel completed -> 409", c.post(BASE + f"tasks/{task['id']}:cancel", headers=H()).status_code == 409)

# cancel vs results race on a fresh task
msg2 = dict(msg, messageId="m-" + uuid.uuid4().hex)
t2 = c.post(send, headers=H(), json={"message": msg2}).json()["task"]
rm = results_msg(t2, ["ACCEPTED", "ACCEPTED"])
with concurrent.futures.ThreadPoolExecutor(2) as pool:
    f1 = pool.submit(lambda: requests.post(send, headers=H(), json={"message": rm}, timeout=60))
    f2 = pool.submit(lambda: requests.post(BASE + f"tasks/{t2['id']}:cancel", headers=H(), timeout=60))
    codes = sorted([f1.result().status_code, f2.result().status_code])
check("race: one 200, one 409", codes == [200, 409], str(codes))
final = c.get(BASE + f"tasks/{t2['id']}", headers=H()).json()
has_receipt = len(final["artifacts"]) > 1
check("race final state consistent",
      (final["status"]["state"] == "TASK_STATE_COMPLETED" and has_receipt) or
      (final["status"]["state"] == "TASK_STATE_CANCELED" and not has_receipt))

print(f"\n{ok_count} passed, {fail_count} failed")
