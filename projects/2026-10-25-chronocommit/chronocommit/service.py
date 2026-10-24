"""Bank-transfer use case built on the generic transaction coordinator."""

from __future__ import annotations

from typing import Any, Iterable

from .coordinator import Coordinator
from .errors import ConflictError, InsufficientFunds, IntegrityError, NotFoundError, ValidationError
from .model import CommitResult, Mutation, VersionedValue, validate_id


MAX_BALANCE_CENTS = 10**15


def _validate_amount(value: int, field: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} must be an integer number of cents")
    minimum = 0 if allow_zero else 1
    if value < minimum or value > MAX_BALANCE_CENTS:
        raise ValidationError(
            f"{field} must be between {minimum} and {MAX_BALANCE_CENTS} cents"
        )
    return value


def _balance(version: VersionedValue, account: str) -> int:
    if version.deleted or version.commit_ts == 0:
        raise NotFoundError(f"account {account!r} does not exist")
    value = version.value
    if not isinstance(value, dict) or set(value) != {"balance_cents"}:
        raise IntegrityError(f"account {account!r} has an invalid stored value")
    balance = value["balance_cents"]
    if isinstance(balance, bool) or not isinstance(balance, int):
        raise IntegrityError(f"account {account!r} balance is not an integer")
    if balance < 0 or balance > MAX_BALANCE_CENTS:
        raise IntegrityError(f"account {account!r} balance is outside safety bounds")
    return balance


class AccountService:
    def __init__(self, coordinator: Coordinator) -> None:
        self.coordinator = coordinator

    def create_account(
        self,
        tenant: str,
        account: str,
        balance_cents: int,
        *,
        request_id: str,
    ) -> CommitResult:
        tenant = validate_id(tenant, "tenant")
        account = validate_id(account, "account")
        request_id = validate_id(request_id, "request_id")
        balance_cents = _validate_amount(balance_cents, "balance_cents", allow_zero=True)
        return self.coordinator.execute(
            tenant,
            f"create-{request_id}",
            [Mutation(account, {"balance_cents": balance_cents}, expected_ts=0)],
            request_identity={
                "account": account,
                "balance_cents": balance_cents,
                "operation": "create_account",
            },
        )

    def get_accounts(
        self,
        tenant: str,
        accounts: Iterable[str],
        *,
        read_ts: int | None = None,
    ) -> dict[str, dict[str, int]]:
        tenant = validate_id(tenant, "tenant")
        requested = tuple(validate_id(account, "account") for account in accounts)
        versions = self.coordinator.snapshot(tenant, requested, read_ts=read_ts)
        return {
            account: {
                "balance_cents": _balance(versions[account], account),
                "commit_ts": versions[account].commit_ts,
            }
            for account in requested
        }

    def transfer(
        self,
        tenant: str,
        transfer_id: str,
        source: str,
        destination: str,
        amount_cents: int,
        *,
        fault: str | None = None,
    ) -> dict[str, Any]:
        tenant = validate_id(tenant, "tenant")
        transfer_id = validate_id(transfer_id, "transfer_id")
        source = validate_id(source, "source")
        destination = validate_id(destination, "destination")
        if source == destination:
            raise ValidationError("source and destination accounts must differ")
        amount_cents = _validate_amount(amount_cents, "amount_cents")
        request_identity = {
            "amount_cents": amount_cents,
            "destination": destination,
            "operation": "transfer",
            "source": source,
        }
        replay = self.coordinator.replay_if_exists(
            tenant, f"transfer-{transfer_id}", request_identity
        )
        if replay is not None:
            committed = self.get_accounts(
                tenant, [source, destination], read_ts=replay.commit_ts
            )
            return {
                "amount_cents": amount_cents,
                "balances": committed,
                "commit": replay.as_dict(),
                "destination": destination,
                "source": source,
                "transfer_id": transfer_id,
            }
        snapshot = self.coordinator.snapshot(tenant, [source, destination])
        source_balance = _balance(snapshot[source], source)
        destination_balance = _balance(snapshot[destination], destination)
        if source_balance < amount_cents:
            raise InsufficientFunds("source account has insufficient funds")
        if destination_balance + amount_cents > MAX_BALANCE_CENTS:
            raise ConflictError("destination balance would exceed the supported range")

        result = self.coordinator.execute(
            tenant,
            f"transfer-{transfer_id}",
            [
                Mutation(
                    source,
                    {"balance_cents": source_balance - amount_cents},
                    expected_ts=snapshot[source].commit_ts,
                ),
                Mutation(
                    destination,
                    {"balance_cents": destination_balance + amount_cents},
                    expected_ts=snapshot[destination].commit_ts,
                ),
            ],
            fault=fault,
            request_identity=request_identity,
        )
        committed = self.get_accounts(
            tenant, [source, destination], read_ts=result.commit_ts
        )
        return {
            "amount_cents": amount_cents,
            "balances": committed,
            "commit": result.as_dict(),
            "destination": destination,
            "source": source,
            "transfer_id": transfer_id,
        }

    def total_balance(self, tenant: str, accounts: Iterable[str]) -> int:
        return sum(
            row["balance_cents"] for row in self.get_accounts(tenant, accounts).values()
        )

