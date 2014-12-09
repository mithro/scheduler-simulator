#!/bin/env python3

infinity = float("inf")
infinity__doc__ = "Useful constant for placing things always in the future."""


from collections import deque
class bdeque(deque):
  """Deque with front/back definition (instead of left/right)."""
  def front(self):
    return self[0]

  def front_pop(self):
    return self.popleft()

  def back(self):
    return self[-1]

  def back_pop(self):
    return self.pop()

  def back_push(self, x):
    return self.append(x)


class Task:
  """Task which can be scheduled."""

  def __repr__(self):
    extra = ""
    if self.deadline:
      extra += ", %s" % self.deadline

    return "Task(%r, %i%s)" % (self.name, self.length, extra)

  def __init__(self, name, length, deadline=None, run_early=False, discardable=False):
    """

    Args:
      name (str): Name of the task.
      length (int): How long the task will take to run.

    Kwargs:
      deadline (int): When the task must be finished by.
      run_early (bool): Allow the task to run early.
    """
    self.name = name
    self.length = length

    self.deadline = deadline
    self.deadline_adjustment = 0

    assert run_early == False or deadline != None, "run_early only makes sense with a deadline."
    self.run_early = run_early

    self.discardable = discardable

  def adjust(self, adjustment):
    """Adjust the deadline by an amount."""
    if adjustment is None:
      self.deadline_adjustment = 0
    else:
      self.deadline_adjustment += adjustment

  def run(self, scheduler, now):
    """
    Args:
      now (int): Current time.

    Returns:
      int. The time on finishing the task.
    """
    assert self.should_run(now)
    self.on_run(scheduler, now)
    return now + self.length

  def discard(self, scheduler, now):
    assert not self.should_run(now)
    self.on_discard(scheduler, now)

  @property
  def start_before(self):
    """Time this task must start at to meet deadline."""
    if not self.deadline:
        return 0
    return self.deadline + self.deadline_adjustment - self.length

  @property
  def finish_latest(self):
    """Time this task would finish if started at self.start time."""
    return self.deadline

  def earlist_run(self, now):
    """Time this task can start from."""
    if self.run_early:
      return now
    else:
      return max(now, self.start_before)
  
  def earlist_finish(self, now):
    """Time this task can finish by if started at earlist_run time."""
    return self.earlist_run(now) + self.length

  def should_run(self, now):
    if not self.discardable:
      return True
    else:
      return now + self.length <= self.finish_latest

  # Methods which should be overridden
  # -------------------------------------------------
  def on_run(self, scheduler, now):
    pass

  def on_discard(self, scheduler, now):
    pass
  # -------------------------------------------------
