#!/bin/env python3

import unittest

from scheduler import SchedulerTracer

class SchedulerTracerForTesting(SchedulerTracer):
  """Scheduler tracer which converts events to strings and saves them."""

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
    #assert not hasattr(self, '_now') or self._now <= n, "%s %s" % (getattr(self, '_now', None), n)
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
  """Useful base class for Scheduler testing."""

  maxDiff = None

  @classmethod
  def setUpClass(cls):
      if cls is SchedulerTestBase:
          raise unittest.SkipTest("Skip %s tests, it's a base class" % cls.__name__)
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


if __name__ == '__main__':
    unittest.main()
