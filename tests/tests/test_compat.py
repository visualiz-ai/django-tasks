from unittest import skipUnless

from django import VERSION
from django.test import SimpleTestCase, override_settings

from django_tasks import compat, task_backends
from django_tasks.backends.immediate import ImmediateBackend
from django_tasks.base import Task
from tests import tasks as test_tasks

HAS_DJANGO_TASKS = VERSION >= (6, 0)


class DjangoCompatTestCase(SimpleTestCase):
    def test_uses_lib_tasks_by_default(self) -> None:
        self.assertIsInstance(task_backends["default"], ImmediateBackend)
        self.assertEqual(task_backends["default"].task_class, Task)

    @skipUnless(HAS_DJANGO_TASKS, "Requires django.tasks")
    def test_django_using_lib_backend(self) -> None:
        from django.tasks import task_backends

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks.backends.immediate.ImmediateBackend"
                }
            }
        ):
            self.assertIsInstance(task_backends["default"], ImmediateBackend)

    @skipUnless(HAS_DJANGO_TASKS, "Requires django.tasks")
    def test_lib_using_django_backend(self) -> None:
        from django.tasks.backends.immediate import ImmediateBackend

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django.tasks.backends.immediate.ImmediateBackend"
                }
            }
        ):
            self.assertIsInstance(task_backends["default"], ImmediateBackend)

    @skipUnless(HAS_DJANGO_TASKS, "Requires django.tasks")
    @override_settings(
        TASKS={
            "default": {"BACKEND": "django_tasks.backends.immediate.ImmediateBackend"}
        }
    )
    def test_lib_backend_validates_django_task(self) -> None:
        """
        A `django.tasks` Task has none of this library's own Task attributes, so
        validating one must only rely on what both classes have.
        """
        from django.tasks.base import Task as DjangoTask

        # Constructing it validates it against the configured (lib) backend.
        django_task: DjangoTask = DjangoTask(func=test_tasks.noop_task.func)

        self.assertFalse(hasattr(django_task, "job_timeout"))

        task_backends["default"].validate_task(django_task)  # type: ignore[arg-type]

    def test_compat_has_django_task(self) -> None:
        self.assertIn(Task, compat.TASK_CLASSES)

        if HAS_DJANGO_TASKS:
            from django.tasks.base import Task as DjangoTask

            self.assertIn(DjangoTask, compat.TASK_CLASSES)
