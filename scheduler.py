#!/bin/env python3

from collections import deque

infinity = float("inf")

class bdeque(deque):
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
  def __repr__(self):
    extra = ""
    if self.deadline:
      extra += ", %i" % self.deadline

    return "Task(%r, %i%s)" % (self.name, self.length, extra)

  def __init__(self, name, length, deadline=None, run_early=False):
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
    assert run_early == False or deadline != None, "run_early only makes sense with a deadline."
    self.deadline = deadline
    self.run_early = run_early
    self.adjustment = 0

  @property
  def start_before(self):
    """Time this task must start at to meet deadline."""
    if not self.deadline:
        return 0
    return self.deadline + self.adjustment - self.length

  @property
  def finish_latest(self):
    """Time this task would finish if started at self.start time."""
    return self.deadline

  def adjust(self, adjustment):
    """
    """
    if adjustment is None:
      self.adjustment = 0
    else:
      self.adjustment += adjustment

  def run(self, scheduler, now):
    """
    Args:
      now (int): Current time.

    Returns:
      int. The time on finishing the task.
    """
    self.on_run(scheduler, now)
    return now + self.length

  # Methods which should be overridden
  # -------------------------------------------------
  def on_run(self, scheduler, now):
    pass
  # -------------------------------------------------


class DiscardIfLateTask(Task):
  def on_discard(self, now):
    """
    Args:
      now (int): Time now.
    """

  def __init__(self, *args, **kw):
    Task.__init__(self, *args, **kw)
    self.discard_enable = True

  def should_discard(self, now):
    return self.discard_enable and now > self.start_before

  def run(self, scheduler, now):
    # As we are running now, clear any adjustment
    self.adjustment = 0

    # If we are late, call on_discard and then finish
    if self.should_discard(now):
      self.on_discard(scheduler, now)
      return now
    return Task.run(self, scheduler, now)

  # Methods which should be overridden
  # -------------------------------------------------
  def on_run(self, scheduler, now):
    pass

  def on_discard(self, scheduler, now):
    pass
  # -------------------------------------------------


class Scheduler:
  sentinel_task = Task("sentinel", 0, deadline=infinity, run_early=False)

  def __init__(self):
    self.deadline_queue = bdeque()
    self.task_queue = bdeque()

  def pending_tasks(self):
    return len(self.deadline_queue) + len(self.task_queue)

  def next_task(self, now):
    task = self.sentinel_task

    # Do we have a deadline tasks that might need to run?
    if self.deadline_queue:
      task = self.deadline_queue.front()

    # Do we have any non-deadline tasks?
    if self.task_queue:
      # Can we run the non-deadline task before any other task?
      if task.start_before >= now + self.task_queue.front().length:
        task = self.task_queue.front()

    return task

  def remove(self, task):
    if task in self.deadline_queue:
      return self.deadline_queue.remove(task)
    if task in self.task_queue:
      return self.task_queue.remove(task)
    raise ValueError("%s not scheduled!" % task)

  def run_next(self, now):
    task = self.next_task(now)
    assert task.start_before != infinity

    # Advance the time if the next task can't run right now.
    if not task.run_early and task.start_before > now:
      now = task.start_before

    self.remove(task)
    return task.run(self, now)

  def run_all(self, initial_time = 0):
    now = initial_time
    while self.pending_tasks() > 0:
      now = self.run_next(now)
      assert now != infinity
    return now

  def add_task(self, new_task):
    if not new_task.start_before:
      self.task_queue.back_push(new_task)
      return

    # Clear any previous adjustments
    new_task.adjust(None)
    self.add_deadline_task(new_task)

  def add_deadline_task(self, new_task):
    # Unqueue any deadline task which will occur after this task
    previous_task = None
    tasks_after_new_task = []
    while self.deadline_queue:
      previous_task = self.deadline_queue.back()
      if previous_task.start_before < new_task.start_before:
        break
      tasks_after_new_task.append(self.remove(previous_task))

    # Will the task before this one cause our deadline to be missed
    if previous_task and previous_task.finish_latest >= new_task.start_before:
      # To allow this task to meet the deadline, we need to make the task
      # before this one run earlier.
      # We remove the previous task from the queue, adjust the deadline and try
      # adding it back. This is because the new deadline on this task could
      # cause the task before it to also need adjustment.
      # Effectively their will be a ripple effect moving the deadline of all
      # earlier tasks forward.
      self.remove(previous_task)
      previous_task.adjust(new_task.start_before - previous_task.finish_latest)
      self.add_deadline_task(previous_task)

    self.deadline_queue.back_push(new_task)

    # We need to add all deadline tasks after the new one which we unqueue. We
    # can't add them directly back to the queue because the new task could cause
    # these tasks to miss their deadline. Instead we add them just like being a
    # new task.
    for task in tasks_after_new_task:
      self.add_deadline_task(task)


class SchedulerTracer:
  def __init__(self):
    self.records = []

  def on_run(self, task, now):
    self.records.append("%05i: Running %s" % (now, task))

  def on_discard(self, task, now):
    self.records.append("%05i: Discarding %s" % (now, task))

  def trace(self, task):
    task_on_run = task.on_run
    def bind_on_run(scheduler, now):
      self.on_run(task, now)
      task_on_run(scheduler, now)
    task.on_run = bind_on_run

    if hasattr(task, "on_discard"):
      task_on_discard = task.on_discard
      def bind_on_discard(scheduler, now):
        self.on_discard(task, now)
        task_on_discard(scheduler, now)
      task.on_discard = bind_on_discard

    return task


import unittest

class SchedulerTest(unittest.TestCase):

  def setUp(self):
    self.t = SchedulerTracer()
    self.s = Scheduler()

  def add_task(self, task):
    self.s.add_task(self.t.trace(task))

  def assertRun(self, *expected, end_at=None):
    now_at_end = self.s.run_all()
    self.assertEqual("\n".join(self.t.records), "\n".join(expected))
    if end_at:
      self.assertEqual(end_at, now_at_end)

  def test_simple_tasks_without_deadlines(self):
    self.add_task(Task("A", 30))
    self.add_task(Task("B", 20))
    self.add_task(Task("C", 10))
    self.assertRun(
      "00000: Running Task('A', 30)",
      "00030: Running Task('B', 20)",
      "00050: Running Task('C', 10)",
      end_at=60)

  def test_single_deadline_without_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=False))
    self.assertRun(
      "00040: Running Task('A', 10, 50)",
      end_at=50)

  def test_single_deadline_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      end_at=10)
 
  def test_multiple_deadline_without_run_early(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=100))
    self.assertRun(
      "00040: Running Task('A', 10, 50)",
      "00090: Running Task('B', 10, 100)",
      end_at=100)

  def test_multiple_deadline_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=100, run_early=True))
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Running Task('B', 10, 100)",
      end_at=20)

  def test_multiple_deadline_adjustment_1(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=55))
    self.assertRun(
      "00035: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_adjustment_ripple(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertRun(
      "00035: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 60)",
      "00055: Running Task('C', 10, 65)",
      end_at=65)

  def test_multiple_deadline_adjustment_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=55))
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_adjustment_with_run_early_and_ripple(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 60)",
      "00055: Running Task('C', 10, 65)",
      end_at=65)

  def test_mixed_simple(self):
    self.add_task(Task("A", 10))
    self.add_task(Task("B", 10, deadline=50))
    self.add_task(Task("C", 10))
    self.add_task(Task("D", 50))
    self.assertRun(
      "00000: Running Task('A', 10)",
      "00010: Running Task('C', 10)",
      "00040: Running Task('B', 10, 50)",
      "00050: Running Task('D', 50)",
      end_at=100)


if __name__ == '__main__':
    unittest.main()
