#!/bin/env python3

from tasks import Task, infinity

class SchedulerTracer:
  """Class to trace scheduler events which occur."""

  def idle_for(self, scheduler, now, amount):
    pass

  def task_add(self, scheduler, now, task):
    pass

  def task_discard(self, scheduler, now, task):
    pass

  def task_start(self, scheduler, now, task):
    pass

  def task_finish(self, scheduler, now, task):
    pass


class Scheduler:
  sentinel_task = Task("sentinel", 0, deadline=infinity, run_early=False)

  @property
  def tracer(self):
    raise NotImplemented()

  def add_task(self, new_task):
    raise NotImplemented()

  def pending_tasks(self):
    raise NotImplemented()

  def run_next(self, now):
    raise NotImplemented()

  def run_all(self, initial_time=0, maximum_tasks=100):
    raise NotImplemented()


class SchedulerBase(object):
  """Base class for all scheduler implementations."""

  def __init__(self):
    self.tracer = SchedulerTracer()

  def add_task(self, new_task):
    self.tracer.task_add(self, None, new_task)
    self._add_task(new_task)

  def run_next(self, now):
    task = self._next_task(now)
    assert task != self.sentinel_task
    assert task.start_before != infinity

    # Advance the time if the next task can't run right now.
    run_early = task.earlist_run(now)
    if run_early > now:
      self.tracer.idle_for(self, now, run_early - now)
      now = run_early

    self._remove_task(task)

    if task.should_run(now):
      self.tracer.task_start(self, now, task)
      now = task.run(self, now)
      self.tracer.task_finish(self, now, task)
    else:
      self.tracer.task_discard(self, now, task)
      task.discard(self, now)

    return now

  def run_all(self, initial_time = 0, maximum_tasks = 100):
    ran_tasks = 0

    now = initial_time
    while self._pending_tasks() > 0:
      now = self.run_next(now)
      ran_tasks +=1
      if ran_tasks > maximum_tasks:
        raise SystemError()

      assert now != infinity
    return now

  # Methods which should be overridden
  # -------------------------------------------------
  def _pending_tasks(self):
    raise NotImplemented()

  def _remove_task(self, task):
    raise NotImplemented()

  def _next_task(self, now):
    raise NotImplemented()

  def _add_task(self, new_task):
    raise NotImplemented()
  # -------------------------------------------------
