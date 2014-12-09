#!/bin/env python3

import unittest
from collections import deque


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
