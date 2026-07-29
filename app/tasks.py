import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


class JobCancelled(Exception):
    pass


@dataclass
class TaskContext:
    is_cancelled: Callable[[], bool]

    def checkpoint(self) -> None:
        if self.is_cancelled():
            raise JobCancelled


TaskHandler = Callable[[dict[str, Any], TaskContext], Any]

## built in task handlers


def echo(payload: dict[str, Any], context: TaskContext) -> Any:
    context.checkpoint()
    return payload.get("value", payload)


def add_numbers(payload: dict[str, Any], context: TaskContext) -> dict[str, float]:
    context.checkpoint()
    numbers = payload.get("numbers")
    if not isinstance(numbers, list) or not numbers:
        raise ValueError("numbers must be a non empty list")
    if not all(
        isinstance(number, int | float) and not isinstance(number, bool) for number in numbers
    ):
        raise ValueError("numbers must only contain numeric values")
    return {"sum": float(sum(numbers))}


def sleep(payload: dict[str, Any], context: TaskContext) -> dict[str, float]:
    seconds = float(payload.get("seconds", 1))
    if not 0 <= seconds <= 30:
        raise ValueError("seconds must be between 0 and 30")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        # keep longer work interruptible
        context.checkpoint()
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    return {"slept_seconds": seconds}


def fail(payload: dict[str, Any], context: TaskContext) -> None:
    context.checkpoint()
    raise RuntimeError(str(payload.get("message", "requested failure")))


TASKS: dict[str, TaskHandler] = {
    "echo": echo,
    "add_numbers": add_numbers,
    "sleep": sleep,
    "fail": fail,
}


def run_task(task_type: str, payload: dict[str, Any], context: TaskContext) -> Any:
    handler = TASKS.get(task_type)
    if not handler:
        raise ValueError(f"unknown task type {task_type}")
    return handler(payload, context)
