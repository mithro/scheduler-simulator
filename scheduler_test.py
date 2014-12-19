#!/bin/env python3

import unittest
import inspect

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
      if not hasattr(cls, 'SchedulerClass') or cls.SchedulerClass is None:
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


class AssertTestNotImplementedHelper:
  """Helper which returns NotImplemented functions for assertTestXXXX methods."""
  def __getattr__(self, key):
    if key.startswith("assertTest"):
      def assertTestNotImplemented(*args, **kw):
        raise NotImplementedError(key)
      return assertTestNotImplemented
    raise AttributeError("%s not found." % key)


class AssertTestMagicCallerHelper:
  """Helper which returns the right assertTestXXXX method for the caller."""

  @property
  def assertTest(self):
    NAME=3  # Function name is the third item in the tuple returned from inspect.stack()

    # Try/finally needed to prevent reference counting leaks.
    frames = inspect.stack()[1:]
    try:
      # Wall up the stack until we find the outermost "testXXXX" function. This
      # allows testAAA to call testBBB and still have assertTestAAA be called.
      while len(frames) > 1 and frames[1][NAME].startswith("test"):
        frames.pop(0)
    
      # Calculate the assert name from the test name
      caller = frames[0][NAME]
      assert caller.startswith("test")
      assert_name = "assert%s%s" % (caller[0].upper(), caller[1:])

      # Return the assert function
      return getattr(self, assert_name)
    finally:
      del frames


class AssertTestMagiCallerHelperTest(unittest.TestCase, AssertTestMagicCallerHelper):

  def testFirstCaller(self):
    self.assertIs(self.assertTest.__name__, "assertTestFirstCaller")

  def assertTestFirstCaller(self):
    pass

  # --

  def testNestedCaller(self, nested=0):
    if nested:
      self.assertIs(self.assertTest.__name__, "assertTestOuterCaller")
    else:
      self.assertIs(self.assertTest.__name__, "assertTestNestedCaller")

  def testOuterCaller(self):
    self.testNestedCaller(nested=True)

  def assertTestNestedCaller(self):
    pass

  def assertTestOuterCaller(self):
    pass


if __name__ == '__main__':
    unittest.main()
