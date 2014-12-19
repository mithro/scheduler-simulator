#!/bin/env python3

from fifo import FIFOScheduler

from scheduler_test import SchedulerTestBase
from tests.basic import *


class FIFOSchedulerTestCase(SingleTaskTests, SimpleMultipleTasksTests, MultipleTasksTests, TickingTasksTests, SchedulerTestBase):
  SchedulerClass = FIFOScheduler

  def setUp(self):
    TickingTasksTests.setUp(self)
    SchedulerTestBase.setUp(self)

  # Nothing needed for SingleTaskTests
  # Nothing needed for SimpleMultipleTasksTests

  # MultipleTasksTests
  # -------------------------------------------------
  def assertTestTasksAddedOutOfOrder(self, **kw):
    self.assertRun(
      "00000: Idle for 90",
      "00090: Running Task('A', 10, 100)",
      "00100: Running Task('B', 10, 50) MISSED",
      "00110: Running Task('C', 10, 20) MISSED",
      end_at=120, **kw)

  def assertTestTasksWithOverlappingDeadlines(self, **kw):
    self.assertRun(
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Running Task('B', 10, 55) MISSED",
      end_at=60, **kw)

  def assertTestTasksWithOverlappingDeadlinesFirstRunningEarly(self):
    self.assertRun(
      "00000: Running Task('A', 10, 50)",
      "00010: Idle for 35",
      "00045: Running Task('B', 10, 55)",
      end_at=55)

  def assertTestTasksWithOverlappingDeadlinesSecondRunningEarly(self):
    self.assertRun(
      "00000: Idle for 40",
      "00040: Running Task('A', 10, 50)",
      "00050: Running Task('B', 10, 55) MISSED",
      end_at=55)
 

  # TickingTasksTests
  # -------------------------------------------------

  #def assertTestTickingTasksWithSmallOtherTask(self):
  

  def assertTestTickingTasksWithLargeOtherTask(self):
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

"""
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
"""

if __name__ == '__main__':
    unittest.main()
