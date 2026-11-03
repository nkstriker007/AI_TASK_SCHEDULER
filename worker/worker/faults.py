"""Fault injection for the sim executor (design doc 8.9, 10.4, E5/E6).

Faults are decided per attempt, deterministically: the same seed and attempt_id always get
the same fault, and a retry (a new attempt_id) gets a fresh draw. Two ways to ask for them:

  rates   SIM_FAULT_ERROR_RATE, SIM_FAULT_RATE_LIMIT_RATE, SIM_FAULT_HANG_RATE,
          SIM_FAULT_CRASH_RATE, SIM_FAULT_STRAGGLER_RATE   (probabilities, sum <= 1)
  rules   SIM_FAULTS="t2@1=crash,t3=hang,t5@2=permanent"   (task id, optional attempt, fault)

Rules win over rates. Other settings: SIM_FAULT_SEED (default 0), SIM_FAULT_RETRY_AFTER_MS
(the simulated 429's hint, default 1000), SIM_FAULT_STRAGGLER_FACTOR (default 10).

What each fault does in the sim executor:
  error       retryable failure after half the task's duration
  rate_limit  immediate retryable failure with retry_after_ms (a simulated 429)
  permanent   immediate permanent failure (rules only)
  hang        never finishes; the worker gives up at the attempt's deadline, atsd retries
  crash       the whole worker process exits mid-task (os._exit), like `docker kill`:
              no event, no XACK; atsd's deadline recovers the attempt
  straggler   the task takes SIM_FAULT_STRAGGLER_FACTOR times longer
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from enum import Enum


class Fault(str, Enum):
    none = "none"
    error = "error"
    rate_limit = "rate_limit"
    permanent = "permanent"
    hang = "hang"
    crash = "crash"
    straggler = "straggler"


# Order in which rates are laid out on [0, 1) for one draw.
_RATED = (Fault.error, Fault.rate_limit, Fault.hang, Fault.crash, Fault.straggler)

CRASH_EXIT_CODE = 86  # distinctive in `docker ps -a` / container logs


@dataclass(frozen=True)
class FaultRule:
    task_id: str
    attempt: int | None  # None: every attempt of the task
    fault: Fault


def parse_rules(text: str) -> tuple[FaultRule, ...]:
    """'t2@1=crash,t3=hang' -> rules. Raises ValueError on anything malformed."""
    rules = []
    for item in (p.strip() for p in text.split(",")):
        if not item:
            continue
        target, sep, fault = item.partition("=")
        if not sep:
            raise ValueError(f"fault rule {item!r} needs '=<fault>'")
        task_id, at, attempt = target.strip().partition("@")
        try:
            rules.append(FaultRule(task_id.strip(), int(attempt) if at else None, Fault(fault.strip())))
        except ValueError:
            raise ValueError(f"bad fault rule {item!r} (faults: {', '.join(f.value for f in Fault)})") from None
    return tuple(rules)


@dataclass(frozen=True)
class FaultConfig:
    rates: dict[Fault, float] = field(default_factory=dict)
    rules: tuple[FaultRule, ...] = ()
    seed: int = 0
    retry_after_ms: int = 1000
    straggler_factor: float = 10.0

    def __post_init__(self) -> None:
        if any(f not in _RATED for f in self.rates):
            raise ValueError(f"rates can only be set for {', '.join(f.value for f in _RATED)}")
        if any(r < 0 for r in self.rates.values()) or sum(self.rates.values()) > 1.0 + 1e-9:
            raise ValueError("fault rates must be >= 0 and add up to at most 1")
        if self.retry_after_ms < 0 or self.straggler_factor < 1:
            raise ValueError("retry_after_ms must be >= 0 and straggler_factor >= 1")

    @property
    def enabled(self) -> bool:
        return bool(self.rules) or any(r > 0 for r in self.rates.values())

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> FaultConfig:
        env = os.environ if env is None else env
        rates = {}
        for fault in _RATED:
            value = env.get(f"SIM_FAULT_{fault.value.upper()}_RATE")
            if value:
                rates[fault] = float(value)
        return cls(
            rates=rates,
            rules=parse_rules(env.get("SIM_FAULTS", "")),
            seed=int(env.get("SIM_FAULT_SEED", "0")),
            retry_after_ms=int(env.get("SIM_FAULT_RETRY_AFTER_MS", "1000")),
            straggler_factor=float(env.get("SIM_FAULT_STRAGGLER_FACTOR", "10")),
        )

    def draw(self, attempt_id: str) -> float:
        """A number in [0, 1) fixed by (seed, attempt_id)."""
        digest = hashlib.sha256(f"{self.seed}:{attempt_id}".encode()).digest()
        return int.from_bytes(digest[:8], "big") / 2**64

    def decide(self, task_id: str, attempt: int, attempt_id: str) -> Fault:
        for rule in self.rules:
            if rule.task_id == task_id and rule.attempt in (None, attempt):
                return rule.fault
        u, edge = self.draw(attempt_id), 0.0
        for fault in _RATED:
            edge += self.rates.get(fault, 0.0)
            if u < edge:
                return fault
        return Fault.none
