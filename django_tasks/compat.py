from typing import Any

from .base import Task

# `django.tasks` only exists on Django 6.0+. Hold the native Task class (when
# there is one) in a plain `type` so the shim needs no version-dependent
# `type: ignore` to type-check.
DjangoTask: "type[Any] | None"

try:
    from django.tasks.base import Task as _DjangoTask
except ImportError:
    DjangoTask = None
else:
    DjangoTask = _DjangoTask

__all__ = ["TASK_CLASSES"]

TASK_CLASSES = (Task, DjangoTask) if DjangoTask is not None else (Task,)
