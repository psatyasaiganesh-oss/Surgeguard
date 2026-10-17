"""Durable local configuration controller. Nothing here deploys to a cluster."""

from contextlib import contextmanager
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time

from .errors import (Busy, Conflict, EvidenceError, InvalidState, NotFound, Paused,
                     ValidationError)
from .model import (Budget, Policy, Trace, boolean, canonical, fingerprint, from_dict,
                    integer, name, token, validate_window)
from .sizing import compare, recommend

APP_ID = 1179210822
SCHEMA = """
CREATE TABLE workloads (
 name TEXT PRIMARY KEY, cpu_millis INTEGER NOT NULL, memory_mib INTEGER NOT NULL,
 code_revision TEXT NOT NULL, opt_cpu INTEGER NOT NULL, opt_memory INTEGER NOT NULL,
 version INTEGER NOT NULL, last_tuned_s INTEGER
);
CREATE TABLE recommendations (
 id TEXT PRIMARY KEY, workload TEXT NOT NULL REFERENCES workloads(name),
 base_version INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN
 ('CREATED','VERIFIED','REJECTED','APPLIED','MONITORED','ROLLED_BACK','SUPERSEDED')),
 spec TEXT NOT NULL, verification TEXT, applied_version INTEGER,
 last_window_end_s INTEGER NOT NULL, seen_ids TEXT NOT NULL
);
CREATE TABLE observations (
 recommendation_id TEXT NOT NULL REFERENCES recommendations(id),
 digest TEXT NOT NULL, result TEXT NOT NULL,
 PRIMARY KEY(recommendation_id,digest)
);
CREATE TABLE events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, workload TEXT NOT NULL,
 kind TEXT NOT NULL, data TEXT NOT NULL
);
CREATE TABLE settings(id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL);
INSERT INTO settings VALUES(1,1);
"""


class Controller:
    def __init__(self, path, *, clock=time.time, timeout=5.0, fault=None):
        if type(timeout) not in (float, int) or not math.isfinite(timeout) or not 0 <= timeout <= 60:
            raise ValidationError("timeout must be finite and between 0 and 60 seconds")
        supplied = Path(path)
        if supplied.is_symlink():
            raise ValidationError("database path cannot be a symlink")
        self.path = str(supplied.resolve())
        self.clock, self.timeout, self.fault = clock, timeout, fault
        Path(self.path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(fd)
        con = self._connect()
        try:
            con.execute("BEGIN")
            app = con.execute("PRAGMA application_id").fetchone()[0]
            version = con.execute("PRAGMA user_version").fetchone()[0]
            tables = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
            con.rollback()
            if app == APP_ID and version == 1:
                return
            if app != 0 or version != 0 or tables:
                raise ValidationError("not a supported FitFleet database; refusing to modify it")
            con.execute("PRAGMA journal_mode=WAL")
            # Initialization is serialized, including concurrent first-open attempts.
            con.execute("BEGIN IMMEDIATE")
            app = con.execute("PRAGMA application_id").fetchone()[0]
            version = con.execute("PRAGMA user_version").fetchone()[0]
            if app == APP_ID and version == 1:
                con.rollback()
                return
            if app != 0 or version != 0 or con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone():
                raise ValidationError("database identity changed during initialization")
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    con.execute(statement)
            con.execute(f"PRAGMA application_id={APP_ID}")
            con.execute("PRAGMA user_version=1")
            con.commit()
        except sqlite3.OperationalError as exc:
            con.rollback()
            self._translate(exc)
        finally:
            con.close()

    def _connect(self):
        con = sqlite3.connect(self.path, isolation_level=None, timeout=self.timeout)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("PRAGMA synchronous=FULL")
        return con

    @staticmethod
    def _translate(exc):
        if "busy" in str(exc).lower() or "locked" in str(exc).lower():
            raise Busy("configuration database busy; retry the whole command") from exc
        raise exc

    @contextmanager
    def _tx(self, write=False):
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield con
            con.commit()
        except sqlite3.OperationalError as exc:
            con.rollback()
            self._translate(exc)
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    def _point(self, point):
        if self.fault:
            self.fault(point)

    @staticmethod
    def _workload(con, workload):
        row = con.execute("SELECT * FROM workloads WHERE name=?", (workload,)).fetchone()
        if row is None:
            raise NotFound("workload not registered")
        return dict(row)

    @staticmethod
    def _rec(con, rid):
        if not isinstance(rid, str) or not re.fullmatch(r"[0-9a-f]{64}", rid):
            raise ValidationError("recommendation ID must be 64 lowercase hexadecimal characters")
        row = con.execute("SELECT * FROM recommendations WHERE id=?", (rid,)).fetchone()
        if row is None:
            raise NotFound("recommendation not found")
        result = dict(row)
        for key in ("spec", "verification", "seen_ids"):
            result[key] = json.loads(result[key]) if result[key] is not None else None
        return result

    @staticmethod
    def _event(con, workload, kind, data):
        con.execute("INSERT INTO events(workload,kind,data) VALUES(?,?,?)", (workload, kind, canonical(data)))

    @staticmethod
    def _active_matches(workload, rec):
        spec = rec["spec"]
        return (workload["version"] == rec["applied_version"] and
                workload["code_revision"] == spec["code_revision"] and
                {"cpu_millis": workload["cpu_millis"], "memory_mib": workload["memory_mib"]} == spec["candidate"])

    def register(self, workload, budget, code_revision, *, opt_cpu=False, opt_memory=False, expected_version=None):
        name(workload)
        token(code_revision, "code_revision")
        if not isinstance(budget, Budget):
            raise ValidationError("budget must be a Budget")
        boolean(opt_cpu, "opt_cpu")
        boolean(opt_memory, "opt_memory")
        if expected_version is not None:
            integer(expected_version, "expected_version", 1)
        with self._tx(write=True) as con:
            existing = con.execute("SELECT version FROM workloads WHERE name=?", (workload,)).fetchone()
            if existing:
                if existing["version"] != expected_version:
                    raise Conflict("registering an existing workload requires its current expected_version")
                version = expected_version+1
                con.execute("""UPDATE workloads SET cpu_millis=?,memory_mib=?,code_revision=?,opt_cpu=?,
                               opt_memory=?,version=? WHERE name=?""",
                            (budget.cpu_millis, budget.memory_mib, code_revision, int(opt_cpu), int(opt_memory), version, workload))
            else:
                if expected_version is not None:
                    raise Conflict("workload does not yet exist")
                version = 1
                con.execute("INSERT INTO workloads VALUES(?,?,?,?,?,?,?,NULL)",
                            (workload, budget.cpu_millis, budget.memory_mib, code_revision, int(opt_cpu), int(opt_memory), version))
            self._event(con, workload, "registered", {"version": version, "code_revision": code_revision})
        return self.status(workload)

    def status(self, workload):
        name(workload)
        with self._tx() as con:
            result = self._workload(con, workload)
            result["enabled"] = bool(con.execute("SELECT enabled FROM settings WHERE id=1").fetchone()[0])
            result["opt_cpu"], result["opt_memory"] = bool(result["opt_cpu"]), bool(result["opt_memory"])
            result["scope"] = "local configuration registry; no cluster deployment"
            return result

    def show(self, rid):
        with self._tx() as con:
            result = self._rec(con, rid)
            # Keep execution IDs available internally but avoid huge CLI responses.
            result["seen_execution_count"] = len(result.pop("seen_ids"))
            result["spec"]["training"].pop("run_ids", None)
            return result

    def set_enabled(self, enabled):
        boolean(enabled, "enabled")
        with self._tx(write=True) as con:
            con.execute("UPDATE settings SET enabled=? WHERE id=1", (int(enabled),))
            self._event(con, "*", "activation_enabled" if enabled else "activation_paused", {})
        return {"enabled": enabled, "rollback_still_allowed": True}

    def plan(self, workload, training, policy=None):
        policy = Policy() if policy is None else policy
        if not isinstance(policy, Policy):
            raise ValidationError("policy must be a validated Policy")
        current = self.status(workload)
        baseline = Budget(current["cpu_millis"], current["memory_mib"])
        candidate, meta, stats = recommend(training, baseline, policy, opt_cpu=current["opt_cpu"],
                                            opt_memory=current["opt_memory"], workload=workload,
                                            revision=current["code_revision"])
        spec = {"schema_version": 1, "workload": workload, "code_revision": current["code_revision"],
                "base_version": current["version"], "baseline": asdict(baseline), "candidate": asdict(candidate),
                "policy": asdict(policy), "training": meta, "statistics": stats}
        rid = fingerprint(spec)
        with self._tx(write=True) as con:
            if self._workload(con, workload)["version"] != current["version"]:
                raise Conflict("configuration changed while computing recommendation")
            if con.execute("SELECT 1 FROM recommendations WHERE id=?", (rid,)).fetchone() is None:
                con.execute("INSERT INTO recommendations VALUES(?,?,?,'CREATED',?,NULL,NULL,?,?)",
                            (rid, workload, current["version"], canonical(spec), meta["end_s"], canonical(meta["run_ids"])))
                self._event(con, workload, "recommendation_created", {"id": rid, "base_version": current["version"]})
        return self.show(rid)

    def verify(self, rid, holdout):
        with self._tx() as con:
            rec = self._rec(con, rid)
        spec = rec["spec"]
        policy = from_dict(Policy, spec["policy"])
        # Replaying exactly the same verification is idempotent; new evidence cannot erase a rejection.
        if not isinstance(holdout, Trace):
            raise ValidationError("holdout must be a Trace")
        digest = holdout.provenance()["fingerprint"]
        if rec["verification"] is not None:
            if rec["verification"]["evidence"]["fingerprint"] == digest:
                return self.show(rid)
            raise Conflict("verification is immutable; create a new recommendation for different evidence")
        if rec["state"] != "CREATED":
            raise InvalidState("only CREATED recommendations can be verified")
        meta = validate_window(holdout, spec["workload"], spec["code_revision"], policy,
                               after=spec["training"]["end_s"], prior_ids=rec["seen_ids"])
        report = compare(holdout, from_dict(Budget, spec["baseline"]), from_dict(Budget, spec["candidate"]), policy)
        evidence = {k: v for k, v in meta.items() if k != "run_ids"}
        verification = {"evidence": evidence, "comparison": report}
        with self._tx(write=True) as con:
            latest = self._rec(con, rid)
            if latest["verification"] is not None:
                if latest["verification"] == verification:
                    return self.show(rid)
                raise Conflict("another verifier committed different evidence")
            if latest["state"] != "CREATED":
                raise Conflict("recommendation changed during verification")
            state = "VERIFIED" if report["passed"] else "REJECTED"
            con.execute("UPDATE recommendations SET state=?,verification=?,last_window_end_s=?,seen_ids=? WHERE id=?",
                        (state, canonical(verification), meta["end_s"], canonical(rec["seen_ids"]+meta["run_ids"]), rid))
            self._event(con, spec["workload"], "verification_completed", {"id": rid, "state": state,
                                                                        "failed_checks": report["failed_checks"]})
        return self.show(rid)

    def apply(self, rid):
        with self._tx(write=True) as con:
            rec = self._rec(con, rid)
            workload = self._workload(con, rec["workload"])
            if rec["state"] in ("APPLIED", "MONITORED"):
                if not self._active_matches(workload, rec):
                    raise Conflict("recommendation was applied, but an operator changed the active configuration")
                return {"id": rid, "state": rec["state"], "version": rec["applied_version"], "replayed": True}
            if rec["state"] != "VERIFIED" or not rec["verification"]["comparison"]["passed"]:
                raise InvalidState("only a passed VERIFIED recommendation can be applied")
            if not con.execute("SELECT enabled FROM settings WHERE id=1").fetchone()[0]:
                raise Paused("activation is paused; verification and rollback remain available")
            if workload["version"] != rec["base_version"]:
                raise Conflict("stale recommendation: active configuration revision changed")
            policy = from_dict(Policy, rec["spec"]["policy"])
            now = int(self.clock())
            if workload["last_tuned_s"] is not None and now-workload["last_tuned_s"] < policy.cooldown_s:
                raise InvalidState("cooldown has not elapsed since the last tune or rollback")
            candidate = rec["spec"]["candidate"]
            version = workload["version"]+1
            con.execute("UPDATE workloads SET cpu_millis=?,memory_mib=?,version=?,last_tuned_s=? WHERE name=?",
                        (candidate["cpu_millis"], candidate["memory_mib"], version, now, rec["workload"]))
            self._point("after_apply_config_update")
            con.execute("UPDATE recommendations SET state='APPLIED',applied_version=? WHERE id=?", (version, rid))
            self._event(con, rec["workload"], "configuration_applied_locally", {"id": rid, "version": version})
            self._point("before_apply_commit")
            return {"id": rid, "state": "APPLIED", "version": version, "replayed": False,
                    "scope": "local configuration registry only"}

    def _restore(self, con, rec):
        current = self._workload(con, rec["workload"])
        if not self._active_matches(current, rec):
            con.execute("UPDATE recommendations SET state='SUPERSEDED' WHERE id=?", (rec["id"],))
            self._event(con, rec["workload"], "rollback_skipped_operator_change", {"id": rec["id"]})
            return "SUPERSEDED"
        baseline = rec["spec"]["baseline"]
        con.execute("UPDATE workloads SET cpu_millis=?,memory_mib=?,version=version+1,last_tuned_s=? WHERE name=?",
                    (baseline["cpu_millis"], baseline["memory_mib"], int(self.clock()), rec["workload"]))
        self._point("after_rollback_config_update")
        con.execute("UPDATE recommendations SET state='ROLLED_BACK' WHERE id=?", (rec["id"],))
        self._event(con, rec["workload"], "configuration_rolled_back_locally", {"id": rec["id"]})
        return "ROLLED_BACK"

    def rollback(self, rid):
        with self._tx(write=True) as con:
            rec = self._rec(con, rid)
            if rec["state"] in ("ROLLED_BACK", "SUPERSEDED"):
                return {"state": rec["state"], "replayed": True}
            if rec["state"] not in ("APPLIED", "MONITORED"):
                raise InvalidState("only an active recommendation can be rolled back")
            return {"state": self._restore(con, rec), "replayed": False}

    def observe(self, rid, trace):
        if not isinstance(trace, Trace):
            raise ValidationError("observation must be a Trace")
        digest = trace.provenance()["fingerprint"]
        with self._tx(write=True) as con:
            rec = self._rec(con, rid)
            previous = con.execute("SELECT result FROM observations WHERE recommendation_id=? AND digest=?", (rid, digest)).fetchone()
            if previous:
                return {**json.loads(previous[0]), "replayed": True}
            if rec["state"] not in ("APPLIED", "MONITORED"):
                raise InvalidState("only active recommendations accept new observation windows")
            if not self._active_matches(self._workload(con, rec["workload"]), rec):
                state = self._restore(con, rec)
                return {"state": state, "rolled_back": False, "reason": "operator configuration changed", "replayed": False}
        spec = rec["spec"]
        policy = from_dict(Policy, spec["policy"])
        meta = validate_window(trace, rec["workload"], spec["code_revision"], policy,
                               after=rec["last_window_end_s"], prior_ids=rec["seen_ids"])
        if len(rec["seen_ids"])+len(meta["run_ids"]) > 200000:
            raise EvidenceError("execution provenance retention limit reached; archive this lab database")
        comparison = compare(trace, from_dict(Budget, spec["baseline"]), from_dict(Budget, spec["candidate"]),
                             policy, require_saving=False)
        with self._tx(write=True) as con:
            latest = self._rec(con, rid)
            previous = con.execute("SELECT result FROM observations WHERE recommendation_id=? AND digest=?", (rid, digest)).fetchone()
            if previous:
                return {**json.loads(previous[0]), "replayed": True}
            if latest["state"] not in ("APPLIED", "MONITORED") or latest["last_window_end_s"] != rec["last_window_end_s"]:
                raise Conflict("another observer changed this recommendation; reload before retrying")
            current = self._workload(con, rec["workload"])
            if not self._active_matches(current, latest):
                state = self._restore(con, latest)
            elif comparison["passed"]:
                state = "MONITORED"
                con.execute("UPDATE recommendations SET state='MONITORED' WHERE id=?", (rid,))
            else:
                state = self._restore(con, latest)
            result = {"state": state, "rolled_back": state == "ROLLED_BACK", "comparison": comparison,
                      "evidence_fingerprint": digest, "replayed": False,
                      "scope": "local replay evidence; not production traffic"}
            con.execute("UPDATE recommendations SET last_window_end_s=?,seen_ids=? WHERE id=?",
                        (meta["end_s"], canonical(rec["seen_ids"]+meta["run_ids"]), rid))
            con.execute("INSERT INTO observations VALUES(?,?,?)", (rid, digest, canonical(result)))
            self._event(con, rec["workload"], "observation_recorded", {"id": rid, "state": state,
                                                                      "failed_checks": comparison["failed_checks"]})
            return result

    def events(self, workload, *, after=0, limit=100):
        name(workload)
        integer(after, "after")
        integer(limit, "limit", 1, 1000)
        with self._tx() as con:
            rows = con.execute("SELECT * FROM events WHERE workload IN (?, '*') AND seq>? ORDER BY seq LIMIT ?", (workload, after, limit))
            return [{"seq": r["seq"], "kind": r["kind"], "data": json.loads(r["data"])} for r in rows]

    def manifest(self, workload, *, image, namespace="default"):
        if not isinstance(image, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}", image):
            raise ValidationError("invalid container image reference")
        name(namespace)
        current = self.status(workload)
        resources = {"cpu": f"{current['cpu_millis']}m", "memory": f"{current['memory_mib']}Mi"}
        container = {
            "name": workload, "image": image,
            "resources": {"requests": resources, "limits": dict(resources)},
            "volumeMounts": [{"name": "scratch", "mountPath": "/tmp"}],
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                "seccompProfile": {"type": "RuntimeDefault"}, "runAsNonRoot": True,
                                "runAsUser": 10001, "runAsGroup": 10001,
                                "capabilities": {"drop": ["ALL"]}},
        }
        pod = {"restartPolicy": "Never", "automountServiceAccountToken": False,
               "securityContext": {"fsGroup": 10001},
               "volumes": [{"name": "scratch", "emptyDir": {"sizeLimit": "128Mi"}}],
               "containers": [container]}
        return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {
                    "name": workload, "namespace": namespace, "annotations": {
                        "fitfleet.io/config-version": str(current["version"]),
                        "fitfleet.io/scope": "Export only; review and test before deployment"}},
                "spec": {"backoffLimit": 0, "template": {"spec": pod}}}
