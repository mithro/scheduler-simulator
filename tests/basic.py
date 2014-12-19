
import unittest
import inspect

from tasks import *
from scheduler_test import *


class SingleTaskTests(SchedulerTestBase, AssertTestMagicCallerHelper):
  """Tests with a single task in the queue.

  By default this test provides the assertTestXXXX methods because most
  schedulers are going the run a single task in the exact same way.
  """

  def testDefaultTask(self):
    self.add_task(Task("A", 30))
    self.assertAdds(1)
    self.assertTest()

  def assertTestDefaultTask(self, **kw):
    self.assertRun(
      "0000: Running Task('A', 30)",
      end_at=30, **kw)

  # --

  def testDeadlineTask(self):
    self.add_task(Task("A", 10, deadline=50))
    self.assertAdds(1)
    self.assertTestDeadlineTask()

  def assertTestDeadlineTask(self, **kw):
    self.assertRun(
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      end_at=50, **kw)

  # --

  def testDeadlineTaskWithRunEarly(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.assertAdds(1)
    self.assertTest()

  def assertTestDeadlineTaskWithRunEarly(self, **kw):
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      end_at=10, **kw)

  # --

  def testDiscardableTaskRuns(self):
    self.add_task(Task("A", 10, deadline=20, discardable=True))
    self.assertAdds(1)
    self.assertTest()

  def assertTestDiscardableTaskRuns(self, **kw):
    self.assertRun(
      "00000: Idle for 10",
      "00010: Running Task('A', 10, 20)",
      end_at=10, **kw)

  # --

  def testDiscardableTaskDiscarded(self):
    self.add_task(Task("A", 10, deadline=20, discardable=True))
    self.assertAdds(1)
    self.assertTest(start_at=100)

  def assertTestDiscardableTaskDiscarded(self, **kw):
    self.assertRun("0100: Discarding Task('A', 30, 20)", end_at=kw['start_at'], **kw)


class SimpleMultipleTasksTests(SchedulerTestBase, AssertTestMagicCallerHelper):
  """Tests with a multiple task in the queue which don't conflict in any way.

  By default this test provides the assertTestXXXX methods because most
  schedulers are going the run a single task in the exact same way.
  """

  def testTasksWithoutDeadlines(self):
    self.add_task(Task("A", 30))
    self.add_task(Task("B", 20))
    self.add_task(Task("C", 10))
    self.assertAdds(3)
    self.assertTest()

  def assertTestTasksWithoutDeadlines(self, **kw):
    self.assertRun(
      "00000: Running Task('A', 30)",
      "00030: Running Task('B', 20)",
      "00050: Running Task('C', 10)",
      end_at=60, **kw)

  # --

  def testTasksWithDeadlines(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=100))
    self.assertAdds(2)
    self.assertTest()

  def assertTestTasksWithDeadlines(self, **kw):
    self.assertRun(
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Idle for 40",
      "00090: Running Task('B', 10, 100)",
      end_at=100, **kw)

  # --

  def testTasksWithDeadlinesAndRunEarly(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=100, run_early=True))
    self.assertAdds(2)
    self.assertTest()

  def assertTestTasksWithDeadlinesAndRunEarly(self, **kw):
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Running Task('B', 10, 100)",
      end_at=20, **kw)

  # --

  def testTasksSimpleMix(self):
    self.add_task(Task("A", 10))
    self.add_task(Task("B", 10, deadline=50))
    self.add_task(Task("C", 10))
    self.add_task(Task("D", 50))
    self.assertAdds(4)
    self.assertTest()

  def assertTestTasksSimpleMix(self, **kw):
    self.assertRun(
      "00000: Running Task('A', 10)",
      "00010: Idle for 30",
      "00040: Running Task('B', 10, 50)",
      "00050: Running Task('C', 10)",
      "00060: Running Task('D', 50)",
      end_at=110, **kw)


class MultipleTasksTests(SchedulerTestBase, AssertTestNotImplementedHelper, AssertTestMagicCallerHelper):
  """Tests with a multiple task in the queue which **do** conflict.

  Every scheduler is probably going to do something different here, so each one
  will need to create the assertTestXXXX methods, probably by calling
  self.assertRun with the correct arguments.
  """

  def testTasksAddedOutOfOrder(self):
    self.add_task(Task("A", 10, deadline=100))
    self.add_task(Task("B", 10, deadline=50))
    self.add_task(Task("C", 10, deadline=20))
    self.assertAdds(3)
    self.assertTest()

  def testTasksWithOverlappingDeadlines(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertTest()

  def testTasksWithOverlappingDeadlinesFirstRunningEarly(self):
    self.add_task(Task("A", 10, deadline=50, run_early=True))
    self.add_task(Task("B", 10, deadline=55))
    self.assertAdds(2)
    self.assertTest()

  def testTasksWithOverlappingDeadlinesSecondRunningEarly(self):
    self.add_task(Task("A", 10, deadline=50))
    self.add_task(Task("B", 10, deadline=55, run_early=True))
    self.assertAdds(2)
    self.assertTest()


class TickingTasksTests(SchedulerTestBase, AssertTestNotImplementedHelper, AssertTestMagicCallerHelper):
  """Tests which look at interaction of tasks with a regular deadline task in the queue.

  Every scheduler is probably going to do something different here, so each one
  will need to create the assertTestXXXX methods, probably by calling
  self.assertRun with the correct arguments.
  """

  tick_period = 10
  tick_length = 5

  def setUp(self):
    self.tick_properties = {}
    self.tick_count = 0

  def addTicks(self, count=1):
    for i in range(self.tick_count, self.tick_count+count):
      self.add_task(Task("Tick#%i" % i, self.tick_length, **dict(deadline=i*self.tick_period, **self.tick_properties)))
    self.tick_count += count

  # --

  def testTickingTasksWithSmallOtherTask(self):
    self.addTicks(2) 
    self.add_task(Task("O", 3))
    self.addTicks(2)
    self.assertAdds(5)
    self.assertTest()

  def testTickingTasksWithLargeOtherTask(self):
    self.addTicks(2) 
    self.add_task(Task("O", 30))
    self.addTicks(7) 
    self.assertAdds(10)
    self.assertTest()

  def testDiscardableTickingTasksWithLargeOtherTask(self):
    self.tick_properties["discardable"] = True
    self.testTickingTasksWithLargeOtherTask()

  def testTickingTasksWithExtraSmallDeadlineTask(self):
    self.addTicks(2) 
    self.add_task(Task("O", 3, deadline=16))
    self.addTicks(2)
    self.assertAdds(5)
    self.assertTest()

  def testDiscardableTickingTasksWithExtraSmallDeadlineTask(self):
    self.tick_properties["discardable"] = True
    self.testTickingTasksWithExtraSmallDeadlineTask()

  def testTickingTasksWithOverlappingSmallDeadlineTask(self):
    self.addTicks(2) 
    self.add_task(Task("O", 3, deadline=10))
    self.addTicks(2)
    self.assertAdds(5)
    self.assertTest()

  def testDiscardableTickingTasksWithOverlappingSmallDeadlineTask(self):
    self.tick_properties["discardable"] = True
    self.testTickingTasksWithOverlappingSmallDeadlineTask()

  def testTickingTasksWithLargeDeadlineTask(self):
    self.addTicks(2) 
    self.add_task(Task("O", 30, deadline=10))
    self.addTicks(7) 
    self.assertAdds(10)
    self.assertTest()

  def testDiscardableTickingTasksWithLargeDeadlineTask(self):
    self.tick_properties["discardable"] = True
    self.testTickingTasksWithLargeDeadlineTask()


