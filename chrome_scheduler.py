#!/bin/env python3

import math
import logging
from scheduler import *


ms = 1000

class ChromeState:
  def __init__(self, frames, main_time, composite_time = 4*ms):
    self.frames = frames
    self._main_tasks = {}
    self._composite_tasks = {}

    self.frame_interval = 16*ms
    self.main_time = main_time
    self.composite_time = composite_time

    self.discarded_main_frames = 0

  def get_main_task(self, frame_time, create=False):
    if create and frame_time not in self._main_tasks:
      self._main_tasks[frame_time] = MainTask(self, frame_time)
    return self._main_tasks.get(frame_time, None)

  def get_composite_task(self, frame_time, create=False):
    if create and frame_time not in self._composite_tasks:
      self._composite_tasks[frame_time] = CompositeTask(self, frame_time)
    return self._composite_tasks.get(frame_time, None)

  def last_composite_task(self):
    return self.get_composite_task(max(self._composite_tasks.keys()))


class MainTask(Task):

  def earlist_run(self, now):
    return self.frame_time

  @property
  def avaliable_time(self):
    return self.state.frame_interval - self.state.composite_time

  def __init__(self, state, frame_time):
    self.state = state
    self.frame_time = frame_time

    Task.__init__(self, 
      name = "MainTask@%i" % frame_time, 
      length = state.main_time,
      deadline = frame_time + self.avaliable_time,
      discardable = True)

    # This check prevents us from continually discarding every main frame we
    # get because of things going horribly wrong.
    if self.state.discarded_main_frames > math.ceil(self.state.main_time / self.avaliable_time):
      self.discardable = False

  def next_task(self):
    next_frame_time = self.state.last_composite_task().frame_time
    if next_frame_time == self.frame_time:
      next_frame_time += self.state.frame_interval
    return self.state.get_main_task(next_frame_time, create=True)

  def should_requeue_without_discard(self, now):
    time_missed_by = now - self.start_before
    logging.debug("Should discard - missed by %s" % (time_missed_by/ms))

    # Optimisation
    # We are going to miss the deadline by a short period but haven't missed
    # any previous frames, lets give it a chance.
    if time_missed_by < 1*ms and self.state.discarded_main_frames == 0:
      logging.debug("ReQ because it's close.")
      return True

    # If we have missed the deadline by more then a frame interval, then this
    # task is horribly out of date and we should just discard it.
    #if time_missed_by > self.state.frame_interval:
    #  logging.debug("Not because its too old.")
    #  return False

    # Otherwise, we need to figure out if attempting this main task will mean
    # more main tasks don't meet their deadlines.

    # How much of the available time the main task takes up determines if
    # re-queuing will effect following main frames.
    main_percentage_of_interval = self.state.main_time / self.avaliable_time

    if main_percentage_of_interval < 0.5:
      # If the main thread can render twice a frame then starting rendering
      # shouldn't cause the next frame to miss it's deadline. Hence we should
      # just try and catch up here.
      logging.debug("ReQ because can't miss. %0.4f%%" % main_percentage_of_interval)
      return True

    if main_percentage_of_interval >= 1.0:
      # Values greater than 1.0 mean the main thread can *never* make the
      # normal deadline.

      # Could we make an extended deadline?
      if main_percentage_of_interval < 2.0:
        # Adjust the deadline and requeue the task
        self.deadline += self.state.frame_interval
      else:
        # The main thread isn't even going an extended deadline, just target
        # the highest throughput.
        self.deadline = None

      logging.debug("ReQ because always miss - new deadline %s. %0.4f%%" % (self.deadline, main_percentage_of_interval))
      return True

    # For percentages between 0.5 and 1.0 then starting rendering too late
    # will cause the next frame to also miss the deadline.
    
    # Work out how late this main task can be before running into the next
    # task.
    next_task = self.next_task()
    if not next_task.discardable:
      logging.debug("Not because next_task can't be discarded. %0.4f%%" % main_percentage_of_interval)
      return False

    logging.debug("ReQ with new deadline - %i. %0.4f%%" % (self.deadline, main_percentage_of_interval))
    self.deadline = next_task.start_before
    return True

  def on_discard(self, scheduler, now):
    # The main task was unable to make the deadline, so this task is being
    # discarded.
    if self.should_requeue_without_discard(now):
      self.discardable = False
      scheduler.add_task(self)
    else:
      self.state.discarded_main_frames += 1
      self.add_next(scheduler)

  def on_run(self, scheduler, now):
    self.state.discarded_main_frames = 0

    self.state.frames -= 1
    if self.state.frames > 0:
      self.add_next(scheduler)

  def add_next(self, scheduler):
    new_task = self.next_task()
    scheduler.add_task(new_task)


class CompositeTask(Task):

  @property
  def main_task(self):
    return self.state.get_main_task(self.frame_time)

  def earlist_run(self, now):
    if self.main_task:
      if self.main_task.earlist_finish(now) > self.start_before:
        return max(now, self.frame_time)
      else:
        return self.main_task.earlist_finish(now)
    return self.start_before

  def __init__(self, state, frame_time):
    self.state = state
    self.frame_time = frame_time
    Task.__init__(self,
      name = "CompositeTask@%i" % frame_time,
      length = state.composite_time,
      deadline = frame_time + self.state.frame_interval,
      run_early = True)

  def next_task(self):
    return self.state.get_composite_task(self.frame_time+self.state.frame_interval, create=True)

  def on_run(self, scheduler, now):
    if self.state.frames > 0:
      scheduler.add_task(self.next_task())


class ChromeSingleSchedulerTest(SchedulerTestBase):
  SchedulerClass = DeadlineOrderingWithAdjustmentScheduler

  def test_single_normal_frame(self):
    c = ChromeState(frames = 0, main_time = 2*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "XXXXX: Adding Task('MainTask@1000', 2000, 13000)",
      "XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "01000: Running Task('MainTask@1000', 2000, 13000)",
      "03000: Running Task('CompositeTask@1000', 4000, 17000)",
      start_at=frame_t0,
      end_at=7*ms)
    self.assertAdds(0)

  def test_single_long_frame(self):
    c = ChromeState(frames = 0, main_time = 30*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "XXXXX: Adding Task('MainTask@1000', 30000, 13000)",
      "XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "01000: Discarding Task('MainTask@1000', 30000, 13000)",
      "01000: Running Task('CompositeTask@1000', 4000, 17000)",
      "05000: Running Task('MainTask@1000', 30000)",
      start_at=frame_t0,
      end_at=35*ms)
    self.assertAdds(
      "01000: Adding Task('MainTask@1000', 30000)",
    )

  def test_multiple_normal_frame(self):
    c = ChromeState(frames = 2, main_time = 2*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "XXXXX: Adding Task('MainTask@1000', 2000, 13000)",
      "XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "01000: Running Task('MainTask@1000', 2000, 13000)",
      "03000: Running Task('CompositeTask@1000', 4000, 17000)",
      "17000: Running Task('MainTask@17000', 2000, 29000)",
      "19000: Running Task('CompositeTask@17000', 4000, 33000)",
      start_at=frame_t0)
    self.assertAdds(
      "01000: Adding Task('MainTask@17000', 2000, 29000)",
      "03000: Adding Task('CompositeTask@17000', 4000, 33000)",
    )

  def test_multiple_long_frame(self):
    c = ChromeState(frames = 2, main_time = 30*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "XXXXX: Adding Task('MainTask@1000', 30000, 13000)",
      "XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "01000: Discarding Task('MainTask@1000', 30000, 13000)",
      "01000: Running Task('CompositeTask@1000', 4000, 17000)",
      "05000: Running Task('CompositeTask@17000', 4000, 33000)",
      "09000: Running Task('MainTask@1000', 30000)",
      "39000: Discarding Task('MainTask@33000', 30000, 45000)",
      "39000: Running Task('CompositeTask@33000', 4000, 49000)",
      "43000: Running Task('CompositeTask@49000', 4000, 65000)",
      "47000: Running Task('MainTask@33000', 30000)",
      "77000: Running Task('CompositeTask@65000', 4000, 81000)",
      start_at=frame_t0)
    self.assertAdds(
      "01000: Adding Task('MainTask@1000', 30000)",
      "01000: Adding Task('CompositeTask@17000', 4000, 33000)",
      "05000: Adding Task('CompositeTask@33000', 4000, 49000)",
      "09000: Adding Task('MainTask@33000', 30000, 45000)",
      "39000: Adding Task('MainTask@33000', 30000)",
      "39000: Adding Task('CompositeTask@49000', 4000, 65000)",
      "43000: Adding Task('CompositeTask@65000', 4000, 81000)",
    )

  maxDiff = None


class NamedFIFOScheduler(FIFOScheduler):
  def __lt__(self, other):
    return self.name < other.name

class MultithreadScheduler:
  def __init__(self):
    self.main_thread = NamedFIFOScheduler()
    self.main_thread.name = "main"
    self.composite_thread = NamedFIFOScheduler()
    self.composite_thread.name = "comp"

  def add_task(self, task):
    if isinstance(task, MainTask):
      self.main_thread.add_task(task)
    else:
      self.composite_thread.add_task(task)

  def _next_task_on(self, nows):
    next_tasks = {}
    for thread in nows:
      if thread._pending_tasks():
        next_tasks[thread] = thread._next_task(nows[thread])

    earlist = min((task.earlist_run(nows[thread]), thread) for thread, task in next_tasks.items())
    return earlist[-1]

  def run_next(self, nows):
    thread = self._next_task_on(nows)
    nows[thread] = thread.run_next(nows[thread])
    return nows
      
  def run_all(self, initial_time = 0):
    nows = {
      self.main_thread: initial_time,
      self.composite_thread: initial_time,
    }

    while sum(thread._pending_tasks() for thread in nows) > 0:
      nows = self.run_next(dict(nows))

    return nows


class ChromeMultiSchedulerTest(SchedulerTestBase):

  def setUp(self):
    self.s = MultithreadScheduler()
    self.t = SchedulerTracerForTesting(self.s.main_thread)
    self.t.trace(self.s.composite_thread)

  def test_single_normal_frame(self):
    c = ChromeState(frames = 1, main_time = 2*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "main @ XXXXX: Adding Task('MainTask@1000', 2000, 13000)",
      "comp @ XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "main @ 01000: Running Task('MainTask@1000', 2000, 13000)",
      "comp @ 03000: Running Task('CompositeTask@1000', 4000, 17000)",
      start_at=frame_t0)
    self.assertAdds(0)

  def test_single_long_frame(self):
    c = ChromeState(frames = 1, main_time = 30*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "main @ XXXXX: Adding Task('MainTask@1000', 30000, 13000)",
      "comp @ XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "comp @ 01000: Running Task('CompositeTask@1000', 4000, 17000)",
      "main @ 01000: Discarding Task('MainTask@1000', 30000, 13000)",
      "main @ 01000: Running Task('MainTask@1000', 30000)",
      "comp @ 29000: Running Task('CompositeTask@17000', 4000, 33000)",
      start_at=frame_t0)
    self.assertAdds(
      "comp @ 01000: Adding Task('CompositeTask@17000', 4000, 33000)",
      "main @ 01000: Adding Task('MainTask@1000', 30000)",
    )

  def test_multiple_normal_frame(self):
    c = ChromeState(frames = 2, main_time = 2*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "main @ XXXXX: Adding Task('MainTask@1000', 2000, 13000)",
      "comp @ XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "main @ 01000: Running Task('MainTask@1000', 2000, 13000)",
      "comp @ 03000: Running Task('CompositeTask@1000', 4000, 17000)",
      "main @ 17000: Running Task('MainTask@17000', 2000, 29000)",
      "comp @ 19000: Running Task('CompositeTask@17000', 4000, 33000)",
      start_at=frame_t0)
    self.assertAdds(
      "main @ 01000: Adding Task('MainTask@17000', 2000, 29000)",
      "comp @ 03000: Adding Task('CompositeTask@17000', 4000, 33000)",
    )

  def test_multiple_long_frame(self):
    c = ChromeState(frames = 2, main_time = 30*ms)

    frame_t0 = 1*ms
    self.add_task(c.get_main_task(frame_t0, create=True))
    self.add_task(c.get_composite_task(frame_t0, create=True))
    self.assertAdds(
      "main @ XXXXX: Adding Task('MainTask@1000', 30000, 13000)",
      "comp @ XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "comp @ 01000: Running Task('CompositeTask@1000', 4000, 17000)",
      "main @ 01000: Discarding Task('MainTask@1000', 30000, 13000)",
      "main @ 01000: Running Task('MainTask@1000', 30000)",
      "comp @ 17000: Running Task('CompositeTask@17000', 4000, 33000)",
      "main @ 31000: Discarding Task('MainTask@17000', 30000, 29000)",
      "main @ 31000: Running Task('MainTask@17000', 30000)",
      "comp @ 45000: Running Task('CompositeTask@33000', 4000, 49000)",
      start_at=frame_t0)
    self.assertAdds(
      "comp @ 01000: Adding Task('CompositeTask@17000', 4000, 33000)",
      "main @ 01000: Adding Task('MainTask@1000', 30000)",
      "main @ 01000: Adding Task('MainTask@17000', 30000, 29000)",
      "comp @ 17000: Adding Task('CompositeTask@33000', 4000, 49000)",
      "main @ 31000: Adding Task('MainTask@17000', 30000)",
    )

  maxDiff = None


if __name__ == '__main__':
    unittest.main()
