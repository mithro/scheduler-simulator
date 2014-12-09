#!/bin/env python3

from ..scheduler import *
from .deadline import DeadlineOrderingScheduler

class DeadlineOrderingWithAdjustmentScheduler(DeadlineOrderingScheduler):
  """Scheduler which orders by deadlines but adjusts them to meet requirements."""

  def _next_task(self, now):
    task = self.sentinel_task

    # Do we have a deadline tasks that might need to run?
    if self.deadline_queue:
      task = self.deadline_queue.front()

    # Do we have any non-deadline tasks?
    if FIFOScheduler._pending_tasks(self):
      next_fifo = FIFOScheduler._next_task(self, now)

      # Can we run the non-deadline task before any other task?
      if task.start_before >= now + next_fifo.length:
        task = next_fifo

      # Else keep running deadline tasks early until we can fit this task.
      elif self.deadline_queue:
        task.adjust(now - task.start_before)

    assert task != self.sentinel_task
    return task

  def _add_deadline_task(self, new_task):
    assert new_task

    # Unqueue any deadline task which will occur after this task
    previous_task = None
    tasks_after_new_task = []
    while self.deadline_queue:
      previous_task = self.deadline_queue.back()
      if previous_task.start_before < new_task.start_before:
        break
      self._remove_task(previous_task)
      tasks_after_new_task.append(previous_task)

    if self.deadline_queue and previous_task and previous_task.finish_latest >= new_task.start_before:
      # To allow this task to meet the deadline, we need to make the task
      # before this one run earlier.
      # We remove the previous task from the queue, adjust the deadline and try
      # adding it back. This is because the new deadline on this task could
      # cause the task before it to also need adjustment.
      # Effectively their will be a ripple effect moving the deadline of all
      # earlier tasks forward.
      self._remove_task(previous_task)
      previous_task.adjust(new_task.start_before - previous_task.finish_latest)
      self._add_deadline_task(previous_task)
    
    self.deadline_queue.back_push(new_task)

    # We need to add all deadline tasks after the new one which we unqueue. We
    # can't add them directly back to the queue because the new task could cause
    # these tasks to miss their deadline. Instead we add them just like being a
    # new task.
    for task in tasks_after_new_task:
      self._add_deadline_task(task)
