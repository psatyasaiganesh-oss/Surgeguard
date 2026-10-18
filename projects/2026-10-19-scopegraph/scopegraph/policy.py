"""Bounded iterative evaluation over one caller-owned SQLite snapshot."""

from collections import deque
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str
    revision: int
    proof: tuple = ()
    visited_edges: int = 0
    cache_hit: bool = False

    def to_dict(self):
        return asdict(self)


class Exhausted(Exception):
    pass


class Evaluator:
    def __init__(self, conn, tenant, now, max_edges=2000, max_depth=32):
        self.conn, self.tenant, self.now = conn, tenant, now
        self.max_edges, self.max_depth, self.edges = max_edges, max_depth, 0
        self.members = {}

    def tuples(self, resource, relation=None):
        remaining = self.max_edges - self.edges
        sql = ("SELECT resource,relation,subject,expires_at FROM tuples WHERE tenant=? "
               "AND resource=? AND (expires_at IS NULL OR expires_at>?)")
        args = [self.tenant, resource, self.now]
        if relation is not None:
            sql += " AND relation=?"
            args.append(relation)
        sql += " ORDER BY relation,subject LIMIT ?"
        args.append(remaining + 1)
        rows = self.conn.execute(sql, args).fetchall()
        self.edges += len(rows)
        if self.edges > self.max_edges:
            raise Exhausted
        return rows

    @staticmethod
    def witness(row):
        return row["resource"] + "#" + row["relation"] + "@" + row["subject"]

    def match(self, who, user):
        if who.startswith("user:"):
            return () if who == user else None
        if who in self.members:
            return self.members[who]
        queue = deque([(who[:-7], (), 0)])
        seen = set()
        while queue:
            group, path, depth = queue.popleft()
            if group in seen:
                continue
            seen.add(group)
            if depth > self.max_depth:
                raise Exhausted
            for row in self.tuples(group, "member"):
                next_path = path + (self.witness(row),)
                if row["subject"] == user:
                    self.members[who] = next_path
                    return next_path
                if row["subject"].endswith("#member"):
                    queue.append((row["subject"][:-7], next_path, depth + 1))
        self.members[who] = None
        return None

    def evaluate(self, resource, permission, user, revision):
        if self.conn.execute("SELECT 1 FROM objects WHERE tenant=? AND ref=?",
                             (self.tenant, resource)).fetchone() is None:
            return Decision(False, "not_found", revision)
        if resource.startswith("group:"):
            return Decision(False, "no_grant", revision)
        try:
            # Build the whole ancestor chain before considering any allow. A deny on
            # any ancestor overrides even a direct owner grant in this fixed policy.
            levels, current, seen, path = [], resource, set(), ()
            while current:
                if current in seen or len(levels) > self.max_depth:
                    raise Exhausted
                seen.add(current)
                rows = self.tuples(current)
                levels.append((rows, path))
                parents = [row for row in rows if row["relation"] == "parent"]
                if len(parents) > 1:
                    raise Exhausted
                current = parents[0]["subject"] if parents else None
                if parents:
                    path += (self.witness(parents[0]),)
            for rows, path in levels:
                for row in rows:
                    if row["relation"] == "blocked":
                        match = self.match(row["subject"], user)
                        if match is not None:
                            return Decision(False, "explicit_deny", revision,
                                            path + (self.witness(row),) + match, self.edges)
            roles = {"view": ("owner", "editor", "viewer"),
                     "edit": ("owner", "editor"), "share": ("owner",)}[permission]
            for rows, path in levels:
                for row in rows:
                    if row["relation"] in roles:
                        match = self.match(row["subject"], user)
                        if match is not None:
                            return Decision(True, "grant", revision,
                                            path + (self.witness(row),) + match, self.edges)
            return Decision(False, "no_grant", revision, visited_edges=self.edges)
        except Exhausted:
            return Decision(False, "budget_exceeded", revision, visited_edges=self.edges)
