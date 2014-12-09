#!/bin/env python3

from ..scheduler import *

class FIFOScheduler(SchedulerBase):
  """Simple first in, first out task scheduler."""

  def __init__(self, *args, **kw):
    SchedulerBase.__init__(self, *args, **kw)
    self.task_queue = bdeque()
 
  def _pending_tasks(self):
    return len(self.task_queue)
 
  def _next_task(self, now):
    return self.task_queue.front()

  def _remove_task(self, task):
    assert task == self.task_queue.front()
    self.task_queue.front_pop()

  def _add_task(self, task):
    self.task_queue.back_push(task)
