#!/bin/env python3

import unittest
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

class bdequeTestCase(unittest.TestCase):
  def basic(self):
    q = bdeque()
    self.assertEqual(len(q), 0)

    a = []
    b = []
    c = []

    q.back_push(a)
    self.assertEqual(len(q), 1)
    self.assertIs(q.back(), a)
    self.assertIs(q.front(), a)

    q.back_push(b)
    self.assertEqual(len(q), 2)
    self.assertIs(q.back(), b)
    self.assertIs(q.front(), a)

    self.assertIs(q.front_pop(), a)
    self.assertEqual(len(q), 1)
    self.assertIs(q.back(), b)
    self.assertIs(q.front(), b)

    q.back_push(c)
    self.assertEqual(len(q), 2)
    self.assertIs(q.back(), c)
    self.assertIs(q.back_pop(), c)
    self.assertIs(q.front(), b)
    self.assertEqual(len(q), 1)
    self.assertIs(q.back(), b)
    self.assertIs(q.front(), b)
    
    self.assertIs(q.back_pop(), b)
    self.assertEqual(len(q), 0 )


class Task:
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


class TaskTestCase(unittest.TestCase):
  def test_start_before(self):
    t = Task("A", 10)
    self.assertEqual(t.start_before, 0)

    t = Task("A", 10, deadline = 20)
    self.assertEqual(t.start_before, 10)

  def test_finish_latest(self):
    t = Task("A", 10)
    self.assertEqual(t.finish_latest, None)

    t = Task("A", 10, deadline = 20)
    self.assertEqual(t.finish_latest, 20)

  def test_earlist_run(self):
    t = Task("A", 10)
    self.assertEqual(t.earlist_run(1), 1)

    t = Task("A", 10, deadline = 20)
    self.assertEqual(t.earlist_run(1), 10)

    t = Task("A", 10, deadline = 20, run_early = True)
    self.assertEqual(t.earlist_run(1), 1)

  def test_earlist_finish(self):
    t = Task("A", 10)
    self.assertEqual(t.earlist_finish(1), 11)

    t = Task("A", 10, deadline = 20)
    self.assertEqual(t.earlist_finish(1), 20)

    t = Task("A", 10, deadline = 20, run_early = True)
    self.assertEqual(t.earlist_finish(1), 11)

  def test_adjust(self):
    t = Task("A", 10, deadline = 20)
   
    t.adjust(-2)
    self.assertEqual(t.start_before, 8)
    self.assertEqual(t.finish_latest, 20)
    self.assertEqual(t.earlist_run(1), 8)
    self.assertEqual(t.earlist_finish(1), 18)

    t.run_early = True
    self.assertEqual(t.start_before, 8)
    self.assertEqual(t.finish_latest, 20)
    self.assertEqual(t.earlist_run(1), 1)
    self.assertEqual(t.earlist_finish(1), 11)

  def test_discard(self):
    t = Task("A", 10, deadline = 20)

    self.assertTrue(t.should_run(1))
    self.assertTrue(t.should_run(10))
    self.assertTrue(t.should_run(11))

    t = Task("A", 10, deadline = 20, discardable = True)
    self.assertTrue(t.should_run(1))
    self.assertTrue(t.should_run(10))
    self.assertFalse(t.should_run(11))

    t = Task("A", 10, deadline = 20, discardable = True)
    t.adjust(-2)
    self.assertTrue(t.should_run(1))
    self.assertTrue(t.should_run(10))
    self.assertFalse(t.should_run(11))


class SchedulerBase:
  sentinel_task = Task("sentinel", 0, deadline=infinity, run_early=False)

  def __init__(self):
    self.tracer = lambda *args, **kw: None

  def _pending_tasks(self):
    raise NotImplemented()

  def _remove_task(self, task):
    raise NotImplemented()

  def _next_task(self, now):
    raise NotImplemented()

  def _add_task(self, new_task):
    raise NotImplemented()

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


class SchedulerTracer:
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


class SchedulerTracerForTesting(SchedulerTracer):
  def __init__(self, scheduler):
    self.events = []
    self.schedulers = 0
    self.trace(scheduler)

  def trace(self, scheduler):
    if scheduler.tracer != self:
      scheduler.tracer = self
      self.schedulers += 1

  def get_str(self, scheduler, name, task):
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

  def idle_for(self, scheduler, now, amount):
    assert scheduler.tracer == self
    self.set_now(now)
    self.events.append(self.get_str(scheduler, "Idle", "for %i" % amount))

  def task_add(self, scheduler, now, task):
    assert scheduler.tracer == self
    if now:
      self.set_now(now)
    self.events.append(self.get_str(scheduler, "Adding", task))

  def task_discard(self, scheduler, now, task):
    assert scheduler.tracer == self
    self.set_now(now)
    self.events.append(self.get_str(scheduler, "Discarding", task))

  def task_start(self, scheduler, now, task):
    assert scheduler.tracer == self
    self.set_now(now)
    self.events.append(self.get_str(scheduler, "Running", task))

  def task_finish(self, scheduler, now, task):
    assert scheduler.tracer == self
    self.set_now(now)
    if task.deadline and now > task.deadline:
       self.events[-1] += " MISSED"

  def clear(self):
    self.events = []

  @property
  def runs(self):
    return [x for x in self.events if "Running" in x or "Discarding" in x or "Idle" in x]

  @property
  def adds(self):
    return [x for x in self.events if "Adding" in x]


class SchedulerTestBase(unittest.TestCase):
  maxDiff = None

  @classmethod
  def setUpClass(cls):
      if cls is SchedulerTestBase:
          raise unittest.SkipTest("Skip BaseTest tests, it's a base class")
      super(SchedulerTestBase, cls).setUpClass()

  def setUp(self):
    self.s = self.SchedulerClass()
    self.t = SchedulerTracerForTesting(self.s)

  def add_task(self, task):
    self.s.add_task(task)

  def assertAdds(self, first, *expected):
    if isinstance(first, int):
      assert not expected, repr([first]+list(expected))
      self.assertEqual(first, len(self.t.adds))
    else:
      self.assertEqual("\n".join(self.t.adds)+"\n", "\n".join([first]+list(expected))+"\n")
    self.t.clear()

  def assertRun(self, *expected, start_at=None, end_at=None):
    now_at_end = self.s.run_all(start_at or 0)
    self.assertEqual("\n".join(self.t.runs)+"\n", "\n".join(expected)+"\n")
    if end_at:
      self.assertEqual(end_at, now_at_end)
    self.t.clear()

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
      "00000: Idle for 40",
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
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Idle for 40",
      "00090: Running Task('B', 10, 100)",
      end_at=100)


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


class FIFOSchedulerTestCase(SchedulerTestBase):
  SchedulerClass = FIFOScheduler

  def test_simple_add_tasks_out_of_order_with_deadlines(self):
    self.add_task(Task("A", 10, deadline=100))
    self.add_task(Task("B", 10, deadline=50))
    self.add_task(Task("C", 10, deadline=20))
    self.assertAdds(3)
    self.assertRun(
      "00000: Idle for 90",
      "00090: Running Task('A', 10, 100)",
      "00100: Running Task('B', 10, 50) MISSED",
      "00110: Running Task('C', 10, 20) MISSED",
      end_at=120)

  def test_multiple_deadline_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=100, run_early=True))
    self.assertAdds(2)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Running Task('B', 10, 100)",
      end_at=20)

  def test_multiple_deadline_overlap(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertRun(
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Running Task('B', 10, 55) MISSED",
      end_at=60)

  def test_multiple_deadline_overlap_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Idle for 35",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_mixed_simple(self):
    self.add_task(Task("A", 10))
    self.add_task(Task("B", 10, deadline=50))
    self.add_task(Task("C", 10))
    self.add_task(Task("D", 50))
    self.assertAdds(4)
    self.assertRun(
      "00000: Running Task('A', 10)",
      "00010: Idle for 30",
      "00040: Running Task('B', 10, 50)",
      "00050: Running Task('C', 10)",
      "00060: Running Task('D', 50)",
      end_at=110)

  def test_mixed_too_large(self):
    self.add_task(Task("A1", 5, deadline=10))
    self.add_task(Task("A2", 5, deadline=20))
    self.add_task(Task("B", 20))
    self.add_task(Task("A3", 5, deadline=30))
    self.add_task(Task("A4", 5, deadline=40))
    self.add_task(Task("A5", 5, deadline=50))
    self.add_task(Task("A6", 5, deadline=60))
    self.add_task(Task("A7", 5, deadline=70))
    self.assertRun(
      "00000: Idle for 5",
      "00005: Running Task('A1', 5, 10)",
      "00010: Idle for 5",
      "00015: Running Task('A2', 5, 20)",
      "00020: Running Task('B', 20)",
      "00040: Running Task('A3', 5, 30) MISSED",
      "00045: Running Task('A4', 5, 40) MISSED",
      "00050: Running Task('A5', 5, 50) MISSED",
      "00055: Running Task('A6', 5, 60)",
      "00060: Idle for 5",
      "00065: Running Task('A7', 5, 70)",
      end_at=70)

  def test_mixed_discardable(self):
    self.add_task(Task("A1", 5, deadline=10, discardable=True))
    self.add_task(Task("A2", 5, deadline=20, discardable=True))
    self.add_task(Task("B", 20))
    self.add_task(Task("A3", 5, deadline=30, discardable=True))
    self.add_task(Task("A4", 5, deadline=40, discardable=True))
    self.add_task(Task("A5", 5, deadline=50, discardable=True))
    self.add_task(Task("A6", 5, deadline=60, discardable=True))
    self.add_task(Task("A7", 5, deadline=70, discardable=True))
    self.assertRun(
      "00000: Idle for 5",
      "00005: Running Task('A1', 5, 10)",
      "00010: Idle for 5",
      "00015: Running Task('A2', 5, 20)",
      "00020: Running Task('B', 20)",
      "00040: Discarding Task('A3', 5, 30)",
      "00040: Discarding Task('A4', 5, 40)",
      "00040: Idle for 5",
      "00045: Running Task('A5', 5, 50)",
      "00050: Idle for 5",
      "00055: Running Task('A6', 5, 60)",
      "00060: Idle for 5",
      "00065: Running Task('A7', 5, 70)",
      end_at=70)


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


class DeadlineOrderingSchedulerTestCase(SchedulerTestBase):
  SchedulerClass = DeadlineOrderingScheduler

  def test_simple_add_tasks_out_of_order_with_deadlines(self):
    self.add_task(Task("A", 10, deadline=100))
    self.add_task(Task("B", 10, deadline=50))
    self.add_task(Task("C", 10, deadline=20))
    self.assertAdds(3)
    self.assertRun(
      "00000: Idle for 10",
      "00010: Running Task('C', 10, 20)",
      "00020: Idle for 20",
      "00040: Running Task('B', 10, 50)",
      "00050: Idle for 40",
      "00090: Running Task('A', 10, 100)",
      end_at=100)

  def test_multiple_deadline_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=100, run_early=True))
    self.assertAdds(2)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Running Task('B', 10, 100)",
      end_at=20)

  def test_multiple_deadline_overlap(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertRun(
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Running Task('B', 10, 55) MISSED",
      end_at=60)

  def test_multiple_deadline_overlap_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Idle for 35",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_overlap_ripple(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertAdds(3)
    self.assertRun(
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Running Task('B', 10, 60)",
      "00060: Running Task('C', 10, 65) MISSED",
      end_at=70)

  def test_multiple_deadline_adjustment_with_run_early(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Idle for 35",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_adjustment_with_run_early_and_ripple(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertAdds(3)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Idle for 40",
      "00050: Running Task('B', 10, 60)",
      "00060: Running Task('C', 10, 65) MISSED",
      end_at=70)

  def test_mixed_simple(self):
    self.add_task(Task("A", 10))
    self.add_task(Task("B", 10, deadline=50))
    self.add_task(Task("C", 10))
    self.add_task(Task("D", 50))
    self.assertAdds(4)
    self.assertRun(
      "00000: Running Task('A', 10)",
      "00010: Running Task('C', 10)",
      "00020: Idle for 20",
      "00040: Running Task('B', 10, 50)",
      "00050: Running Task('D', 50)",
      end_at=100)

  def test_mixed_too_large(self):
    self.add_task(Task("A1", 5, deadline=10))
    self.add_task(Task("A2", 5, deadline=20))
    self.add_task(Task("B", 20))
    self.add_task(Task("A3", 5, deadline=30))
    self.add_task(Task("A4", 5, deadline=40))
    self.add_task(Task("A5", 5, deadline=50))
    self.add_task(Task("A6", 5, deadline=60))
    self.add_task(Task("A7", 5, deadline=70))
    self.assertRun(
      "00000: Idle for 5",
      "00005: Running Task('A1', 5, 10)",
      "00010: Idle for 5",
      "00015: Running Task('A2', 5, 20)",
      "00020: Idle for 5",
      "00025: Running Task('A3', 5, 30)",
      "00030: Idle for 5",
      "00035: Running Task('A4', 5, 40)",
      "00040: Idle for 5",
      "00045: Running Task('A5', 5, 50)",
      "00050: Idle for 5",
      "00055: Running Task('A6', 5, 60)",
      "00060: Idle for 5",
      "00065: Running Task('A7', 5, 70)",
      "00070: Running Task('B', 20)",
      end_at=90)

  def test_mixed_discardable(self):
    self.add_task(Task("A1", 5, deadline=10, discardable=True))
    self.add_task(Task("A2", 5, deadline=20, discardable=True))
    self.add_task(Task("B", 20))
    self.add_task(Task("A3", 5, deadline=30, discardable=True))
    self.add_task(Task("A4", 5, deadline=40, discardable=True))
    self.add_task(Task("A5", 5, deadline=50, discardable=True))
    self.add_task(Task("A6", 5, deadline=60, discardable=True))
    self.add_task(Task("A7", 5, deadline=70, discardable=True))
    self.assertRun(
      "00000: Idle for 5",
      "00005: Running Task('A1', 5, 10)",
      "00010: Idle for 5",
      "00015: Running Task('A2', 5, 20)",
      "00020: Idle for 5",
      "00025: Running Task('A3', 5, 30)",
      "00030: Idle for 5",
      "00035: Running Task('A4', 5, 40)",
      "00040: Idle for 5",
      "00045: Running Task('A5', 5, 50)",
      "00050: Idle for 5",
      "00055: Running Task('A6', 5, 60)",
      "00060: Idle for 5",
      "00065: Running Task('A7', 5, 70)",
      "00070: Running Task('B', 20)",
      end_at=90)


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


class DeadlineOrderingWithAdjustmentSchedulerTestCase(SchedulerTestBase):
  SchedulerClass = DeadlineOrderingWithAdjustmentScheduler

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
      "00000: Idle for 40",
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
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Idle for 40",
      "00090: Running Task('B', 10, 100)",
      end_at=100)

  def test_simple_add_tasks_out_of_order_with_deadlines(self):
    self.add_task(Task("B", 10, deadline=100))
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("C", 10, deadline=20))
    self.assertAdds(3)
    self.assertRun(
      "00000: Idle for 10",
      "00010: Running Task('C', 10, 20)",
      "00020: Idle for 20",
      "00040: Running Task('A', 10, 50)",
      "00050: Idle for 40",
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
      "00000: Idle for 35",
      "00035: Running Task('A', 10, 50)",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_adjustment_ripple(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertAdds(3)
    self.assertRun(
      "00000: Idle for 35",
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
      "00010: Idle for 35",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def test_multiple_deadline_adjustment_with_run_early_and_ripple(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=60))
    self.add_task(Task("C", 10, deadline=65))
    self.assertAdds(3)
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Idle for 35",
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
      "00060: Idle for 5",
      "00065: Running Task('A7', 5, 70)",
      end_at=70)


if __name__ == '__main__':
    unittest.main()
