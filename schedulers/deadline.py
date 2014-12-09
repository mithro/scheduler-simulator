#!/bin/env python3

from ..scheduler import *
from .fifo import FIFOScheduler


class DeadlineOrderingScheduler(FIFOScheduler):
  """Scheduler which orders runs by deadlines (then FIFO)."""

  def __init__(self, *args, **kw):
    self.deadline_queue = bdeque()
    FIFOScheduler.__init__(self, *args, **kw)

  def _pending_tasks(self):
    return len(self.deadline_queue) + FIFOScheduler._pending_tasks(self)

  def _next_task(self, now):
    task = self.sentinel_task

    # Do we have a deadline tasks that might need to run?
    if self.deadline_queue:
      task = self.deadline_queue.front()

    # Do we have any non-deadline tasks?
    if FIFOScheduler._pending_tasks(self):
      next_fifo = FIFOScheduler._next_task(self, now)

      # Can we run the non-deadline task before any other task?
      if task.start_before >= next_fifo.earlist_finish(now):
        task = next_fifo

    assert task != self.sentinel_task
    return task

  def _remove_task(self, task):
    if task in self.deadline_queue:
      self.deadline_queue.remove(task)
      return
    FIFOScheduler._remove_task(self, task)

  def _add_task(self, new_task):
    assert new_task

    # If it's a normal task, just FIFO scheduler it
    if not new_task.start_before:
      FIFOScheduler._add_task(self, new_task)
      return

    # Else it's a deadline task, so schedule it specially.
    new_task.adjust(None) # Clear any previous adjustments
    self._add_deadline_task(new_task)

  def _add_deadline_task(self, new_task):
    assert new_task

    self.deadline_queue.back_push(new_task)
    # Restore the queue, so deadline_queue is ordered
    self.deadline_queue = bdeque(sorted(self.deadline_queue, key=lambda t: t.start_before))
