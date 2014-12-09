#!/bin/env python3

from ..scheduler_test import SchedulerTestBase
from .deadline_adjustment import DeadlineOrderingWithAdjustmentScheduler


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
