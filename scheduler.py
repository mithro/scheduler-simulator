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
      extra += ", %s" % self.deadline

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

  def earlist_run(self, now):
    if self.run_early:
      return now
    else:
      return self.start_before
  
  def earlist_finish(self, now):
    return self.earlist_run(now) + self.length

  # Methods which should be overridden
  # -------------------------------------------------
  def on_run(self, scheduler, now):
    pass
  # -------------------------------------------------


class DiscardIfLateTask(Task):
  def __init__(self, *args, **kw):
    Task.__init__(self, *args, **kw)
    self.discard_enable = True

  def should_discard(self, now):
    return self.discard_enable and now > self.start_before

  def run(self, scheduler, now):
    # As we are running now, clear any adjustment
    self.adjust(None)

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

  def __lt__(self, other):
    return self.name < other.name

  def __init__(self):
    self.deadline_queue = bdeque()
    self.task_queue = bdeque()

  def _pending_tasks(self):
    return len(self.deadline_queue) + len(self.task_queue)

  def _next_task(self, now):
    task = self.sentinel_task

    # Do we have a deadline tasks that might need to run?
    if self.deadline_queue:
      task = self.deadline_queue.front()

    # Do we have any non-deadline tasks?
    if self.task_queue:
      # Can we run the non-deadline task before any other task?
      if task.start_before >= now + self.task_queue.front().length:
        task = self.task_queue.front()

      # Else keep running deadline tasks early until we can fit this task.
      elif self.deadline_queue:
        task = self.deadline_queue.front()
        task.adjust(now - task.start_before)

    return task

  def _remove(self, task):
    if task in self.deadline_queue:
      self.deadline_queue.remove(task)
      return
    if task in self.task_queue:
      self.task_queue.remove(task)
      return
    raise ValueError("%s not scheduled!" % task)

  def run_next(self, now):
    task = self._next_task(now)
    assert task.start_before != infinity

    # Advance the time if the next task can't run right now.
    run_early = task.earlist_run(now)
    if run_early > now:
      now = run_early

    self._remove(task)
    return task.run(self, now)

  def run_all(self, initial_time = 0):
    count = 0

    now = initial_time
    while self._pending_tasks() > 0:
      now = self.run_next(now)
      count +=1
      if count > 100:
        raise SystemError()

      assert now != infinity
    return now

  def add_task(self, new_task):
    assert new_task

    if not new_task.start_before:
      self.task_queue.back_push(new_task)
      return

    # Clear any previous adjustments
    new_task.adjust(None)
    self._add_deadline_task(new_task)

  def _add_deadline_task(self, new_task):
    assert new_task

    # Unqueue any deadline task which will occur after this task
    previous_task = None
    tasks_after_new_task = []
    while self.deadline_queue:
      previous_task = self.deadline_queue.back()
      if previous_task.start_before < new_task.start_before:
        break
      self._remove(previous_task)
      tasks_after_new_task.append(previous_task)

    # Will the task before this one cause our deadline to be missed
    if self.deadline_queue and previous_task and previous_task.finish_latest >= new_task.start_before:
      # To allow this task to meet the deadline, we need to make the task
      # before this one run earlier.
      # We remove the previous task from the queue, adjust the deadline and try
      # adding it back. This is because the new deadline on this task could
      # cause the task before it to also need adjustment.
      # Effectively their will be a ripple effect moving the deadline of all
      # earlier tasks forward.
      self._remove(previous_task)
      previous_task.adjust(new_task.start_before - previous_task.finish_latest)
      self._add_deadline_task(previous_task)

    self.deadline_queue.back_push(new_task)

    # We need to add all deadline tasks after the new one which we unqueue. We
    # can't add them directly back to the queue because the new task could cause
    # these tasks to miss their deadline. Instead we add them just like being a
    # new task.
    for task in tasks_after_new_task:
      self._add_deadline_task(task)


class SchedulerTracer:
  def __init__(self, scheduler):
    self.adds = []
    self.runs = []

    self.schedulers = 0
    self.trace_scheduler(scheduler)

  def trace_scheduler(self, scheduler):
    self.schedulers += 1

    scheduler_add_task = scheduler.add_task
    def bind_on_add_task(new_task):
      self.on_add(scheduler, new_task)
      self.trace_task(new_task)
      scheduler_add_task(new_task)
    scheduler.add_task = bind_on_add_task

    scheduler.traced = True

  def get_str(self, name, scheduler, task):
    prefix = ""
    if self.schedulers > 1:
      prefix = scheduler.name + ' @ '

    if not hasattr(self, '_now'):
      now = "XXXXX"
    else:
      now = "%05i" % self._now

    return "%s%s: %s %s" % (prefix, now, name, task)

  def set_now(self, n):
    assert not hasattr(self, '_now') or self._now <= n, "%s %s" % (getattr(self, '_now', None), n)
    self._now = n

  def on_add(self, scheduler, task):
    assert scheduler.traced == True
    self.adds.append(self.get_str("Adding", scheduler, task))

  def on_run(self, scheduler, task):
    assert scheduler.traced == True
    self.runs.append(self.get_str("Running", scheduler, task))

  def on_discard(self, scheduler, task):
    assert scheduler.traced == True
    self.runs.append(self.get_str("Discarding", scheduler, task))

  def trace_task(self, task):
    if hasattr(task, "traced"):
      return task

    task_on_run = task.on_run
    def bind_on_run(scheduler, now):
      self.set_now(now)
      self.on_run(scheduler, task)
      task_on_run(scheduler, now)
      #self.set_now(now+task.length)
    task.on_run = bind_on_run

    if hasattr(task, "on_discard"):
      task_on_discard = task.on_discard
      def bind_on_discard(scheduler, now):
        self.set_now(now)
        self.on_discard(scheduler, task)
        task_on_discard(scheduler, now)
      task.on_discard = bind_on_discard

    task.traced = True
    return task


import unittest

class SchedulerTestExtra:

  def setUp(self):
    self.s = Scheduler()
    self.t = SchedulerTracer(self.s)

  def add_task(self, task):
    self.s.add_task(task)

  def assertAdds(self, first, *expected):
    if isinstance(first, int):
      assert not expected, repr([first]+list(expected))
      self.assertEqual(first, len(self.t.adds))
    else:
      self.assertEqual("\n".join(self.t.adds)+"\n", "\n".join([first]+list(expected))+"\n")

    self.t.adds = []

  def assertRun(self, *expected, start_at=None, end_at=None):
    now_at_end = self.s.run_all(start_at or 0)
    self.assertEqual("\n".join(self.t.runs)+"\n", "\n".join(expected)+"\n")
    if end_at:
      self.assertEqual(end_at, now_at_end)
    self.t.runs = []


class SchedulerTest(unittest.TestCase, SchedulerTestExtra):

  def setUp(self):
    SchedulerTestExtra.setUp(self)

  def test_simple_tasks_without_deadlines(self):
    self.add_task(Task("A", 30))
    self.add_task(Task("B", 20))
    self.add_task(Task("C", 10))
    self.assertAdds(3)
    self.assertRun(
      "00000: Running Task('A', 30)",
      "00030: Running Task('B', 20)",
      "00050: Running Task('C', 10)",
      end_at=60)

  def test_single_deadline_without_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=False))
    self.assertAdds(1)
    self.assertRun(
      "00040: Running Task('A', 10, 50)",
      end_at=50)

  def test_single_deadline_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.assertAdds(1)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      end_at=10)
 
  def test_multiple_deadline_without_run_early(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=100))
    self.assertAdds(2)
    self.assertRun(
      "00040: Running Task('A', 10, 50)",
      "00090: Running Task('B', 10, 100)",
      end_at=100)

  def test_simple_add_tasks_out_of_order_with_deadlines(self):
    self.add_task(Task("B", 10, deadline=100))
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("C", 10, deadline=20))
    self.assertAdds(3)
    self.assertRun(
      "00010: Running Task('C', 10, 20)",
      "00040: Running Task('A', 10, 50)",
      "00090: Running Task('B', 10, 100)",
      end_at=100)


  def test_multiple_deadline_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=100, run_early=True))
    self.assertAdds(2)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Running Task('B', 10, 100)",
      end_at=20)

  def test_multiple_deadline_adjustment_1(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertRun(
      "00035: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_adjustment_ripple(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertAdds(3)
    self.assertRun(
      "00035: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 60)",
      "00055: Running Task('C', 10, 65)",
      end_at=65)

  def test_multiple_deadline_adjustment_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_adjustment_with_run_early_and_ripple(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertAdds(3)
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
    self.assertAdds(4)
    self.assertRun(
      "00000: Running Task('A', 10)",
      "00010: Running Task('C', 10)",
      "00020: Running Task('B', 10, 50)",
      "00030: Running Task('D', 50)",
      end_at=80)

  def test_mixed_too_large(self):
    self.add_task(Task("A1", 5, deadline=10))
    self.add_task(Task("A2", 5, deadline=20))
    self.add_task(Task("A3", 5, deadline=30))
    self.add_task(Task("A4", 5, deadline=40))
    self.add_task(Task("A5", 5, deadline=50))
    self.add_task(Task("A6", 5, deadline=60))
    self.add_task(Task("A7", 5, deadline=70))
    self.add_task(Task("B", 20))
    self.add_task(Task("C", 10))
    self.assertRun(
      "00000: Running Task('A1', 5, 10)",
      "00005: Running Task('A2', 5, 20)",
      "00010: Running Task('A3', 5, 30)",
      "00015: Running Task('B', 20)",
      "00035: Running Task('A4', 5, 40)",
      "00040: Running Task('A5', 5, 50)",
      "00045: Running Task('C', 10)",
      "00055: Running Task('A6', 5, 60)",
      "00065: Running Task('A7', 5, 70)",
      end_at=70)


if __name__ == '__main__':
    unittest.main()
