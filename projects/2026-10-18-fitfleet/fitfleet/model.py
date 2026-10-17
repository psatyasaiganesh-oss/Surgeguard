"""Bounded, strict input schemas. Times, CPU, and memory use explicit units."""

from dataclasses import asdict, dataclass, fields
import hashlib
import json
from pathlib import Path
import re

from .errors import EvidenceError, ValidationError

MAX_RUNS = 50000
MAX_FILE_BYTES = 16 * 1024 * 1024
TOKEN = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")


def integer(value, name, low=0, high=2**53-1):
    if type(value) is not int or not low <= value <= high:
        raise ValidationError(f"{name} must be an integer in [{low}, {high}]")
    return value


def token(value, name="identifier"):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        raise ValidationError(f"{name} must be 1..128 ASCII identifier characters")
    return value


def name(value):
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise ValidationError("workload name must be a lowercase DNS label of 1..63 characters")
    return value


def boolean(value, label):
    if type(value) is not bool:
        raise ValidationError(f"{label} must be boolean")
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def reject_constant(value):
    raise ValidationError(f"non-finite JSON value: {value}")


def load_json(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_FILE_BYTES + 1)
    if len(raw) > MAX_FILE_BYTES:
        raise ValidationError("JSON input exceeds 16 MiB")
    # Bound nesting BEFORE decoding; interpreter recursion limits are environment-dependent.
    # Brackets inside quoted strings, including escaped quotes, are not structural.
    depth, in_string, escaped = 0, False, False
    for byte in raw:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                in_string = False
        elif byte == 34:
            in_string = True
        elif byte in (91, 123):
            depth += 1
            if depth > 32:
                raise ValidationError("JSON nesting exceeds 32 levels")
        elif byte in (93, 125):
            depth -= 1
    try:
        return json.loads(raw, object_pairs_hook=strict_pairs, parse_constant=reject_constant)
    except (RecursionError, UnicodeDecodeError) as exc:
        raise ValidationError("JSON uses invalid encoding or excessive nesting") from exc


def from_dict(cls, value):
    allowed = {f.name for f in fields(cls)}
    if not isinstance(value, dict) or set(value)-allowed:
        raise ValidationError(f"invalid {cls.__name__} fields; allowed: {sorted(allowed)}")
    try:
        return cls(**value)
    except TypeError as exc:
        raise ValidationError(f"missing or invalid {cls.__name__} fields") from exc


@dataclass(frozen=True)
class Budget:
    cpu_millis: int
    memory_mib: int

    def __post_init__(self):
        integer(self.cpu_millis, "cpu_millis", 250, 64000)
        integer(self.memory_mib, "memory_mib", 128, 65536)


@dataclass(frozen=True)
class Run:
    run_id: str
    timestamp_s: int
    workload: str
    code_revision: str
    cpu_peak_millis: int
    memory_peak_mib: int
    cpu_work_ms: int
    fixed_ms: int
    deadline_ms: int
    censored: bool = False

    def __post_init__(self):
        token(self.run_id, "run_id")
        integer(self.timestamp_s, "timestamp_s")
        name(self.workload)
        token(self.code_revision, "code_revision")
        integer(self.cpu_peak_millis, "cpu_peak_millis", 1, 64000)
        integer(self.memory_peak_mib, "memory_peak_mib", 1, 65536)
        integer(self.cpu_work_ms, "cpu_work_ms", 0, 86400000)
        integer(self.fixed_ms, "fixed_ms", 1, 86400000)
        integer(self.deadline_ms, "deadline_ms", 1, 86400000)
        boolean(self.censored, "censored")


@dataclass(frozen=True)
class Trace:
    runs: tuple[Run, ...]

    def __post_init__(self):
        if not isinstance(self.runs, tuple) or not 1 <= len(self.runs) <= MAX_RUNS:
            raise ValidationError(f"trace must have 1..{MAX_RUNS} runs")
        if not all(isinstance(r, Run) for r in self.runs):
            raise ValidationError("trace entries must be validated Run objects")
        if len({r.run_id for r in self.runs}) != len(self.runs):
            raise ValidationError("duplicate run_id in trace")
        if len({(r.workload, r.code_revision) for r in self.runs}) != 1:
            raise EvidenceError("one trace must contain exactly one workload and code revision")
        object.__setattr__(self, "runs", tuple(sorted(self.runs, key=lambda r: (r.timestamp_s, r.run_id))))

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict) or set(value) != {"schema_version", "runs"}:
            raise ValidationError("trace requires exactly schema_version and runs")
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValidationError("unsupported trace schema_version")
        if not isinstance(value["runs"], list) or not 1 <= len(value["runs"]) <= MAX_RUNS:
            raise ValidationError("invalid run count")
        return cls(tuple(from_dict(Run, r) for r in value["runs"]))

    def document(self):
        return {"schema_version": 1, "runs": [asdict(r) for r in self.runs]}

    def provenance(self):
        return {"fingerprint": fingerprint(self.document()), "run_ids": [r.run_id for r in self.runs],
                "start_s": self.runs[0].timestamp_s, "end_s": self.runs[-1].timestamp_s,
                "count": len(self.runs), "workload": self.runs[0].workload,
                "code_revision": self.runs[0].code_revision}


@dataclass(frozen=True)
class Policy:
    min_training_samples: int = 100
    min_validation_samples: int = 50
    min_window_s: int = 3600
    cpu_quantile_bp: int = 9900
    memory_quantile_bp: int = 9990
    cpu_headroom_bp: int = 1000
    memory_headroom_bp: int = 2500
    max_cpu_reduction_bp: int = 5000
    max_memory_reduction_bp: int = 1500
    min_score_saving_bp: int = 500
    max_p95_runtime_ratio_bp: int = 10500
    max_single_runtime_ratio_bp: int = 12500
    max_retry_rate_bp: int = 200
    memory_score_weight_bp: int = 2500
    retry_memory: bool = False
    memory_guard_bp: int = 9000
    retry_wasted_work_bp: int = 2000
    max_retries: int = 3
    cooldown_s: int = 86400

    def __post_init__(self):
        for field in fields(self):
            if field.name != "retry_memory":
                integer(getattr(self, field.name), field.name, 0, 86400000)
        boolean(self.retry_memory, "retry_memory")
        for key in ("min_training_samples", "min_validation_samples"):
            integer(getattr(self, key), key, 1, MAX_RUNS)
        for key in ("cpu_quantile_bp", "memory_quantile_bp"):
            integer(getattr(self, key), key, 5000, 10000)
        for key in ("cpu_headroom_bp", "memory_headroom_bp"):
            integer(getattr(self, key), key, 0, 10000)
        for key in ("max_cpu_reduction_bp", "max_memory_reduction_bp", "min_score_saving_bp", "max_retry_rate_bp"):
            integer(getattr(self, key), key, 0, 9000)
        integer(self.max_p95_runtime_ratio_bp, "max_p95_runtime_ratio_bp", 10000, 30000)
        integer(self.max_single_runtime_ratio_bp, "max_single_runtime_ratio_bp", self.max_p95_runtime_ratio_bp, 50000)
        integer(self.memory_score_weight_bp, "memory_score_weight_bp", 1, 10000)
        integer(self.memory_guard_bp, "memory_guard_bp", 5000, 9900)
        integer(self.retry_wasted_work_bp, "retry_wasted_work_bp", 1, 10000)
        integer(self.max_retries, "max_retries", 0, 5)
        integer(self.cooldown_s, "cooldown_s", 0, 30*86400)


def validate_window(trace, workload, revision, policy, *, training=False, after=None, prior_ids=()):
    if not isinstance(trace, Trace):
        raise ValidationError("trace must be a validated Trace")
    meta = trace.provenance()
    minimum = policy.min_training_samples if training else policy.min_validation_samples
    if meta["count"] < minimum or meta["end_s"]-meta["start_s"] < policy.min_window_s:
        raise EvidenceError("insufficient sample count or observation span")
    if (meta["workload"], meta["code_revision"]) != (workload, revision):
        raise EvidenceError("evidence workload or code revision does not match configuration")
    if after is not None and meta["start_s"] <= after:
        raise EvidenceError("evidence must be strictly newer than the previous window")
    if set(meta["run_ids"]).intersection(prior_ids):
        raise EvidenceError("evidence reuses earlier execution IDs")
    if any(r.censored for r in trace.runs):
        raise EvidenceError("censored or incomplete execution demand cannot support tuning")
    return meta
