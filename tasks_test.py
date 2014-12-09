#!/bin/env python3

import unittest
from tasks import *

class bdequeTestCase(unittest.TestCase):
  """Quick test to make sure our extra methods work."""

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


class TaskTestCase(unittest.TestCase):
  """Tests for the basic task class."""

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


if __name__ == '__main__':
    unittest.main()
