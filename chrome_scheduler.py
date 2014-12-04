#!/bin/env python3

import math
from scheduler import *


ms = 1000

class ChromeState:
  def __init__(self, frames, main_time, composite_time = 4*ms):
    self.frames = frames

    self.frame_interval = 16*ms
    self.main_time = main_time
    self.composite_time = composite_time

    self.discarded_main_frames = 0


class MainTask(DiscardIfLateTask):

  def earlist_run(self, now):
    return self.frame_time

  @property
  def avaliable_time(self):
    return self.state.frame_interval - self.state.composite_time

  def __init__(self, state, frame_time):
    self.state = state
    self.frame_time = frame_time

    DiscardIfLateTask.__init__(self, 
      name = "MainTask@%i" % frame_time, 
      length = state.main_time,
      deadline = frame_time + self.avaliable_time)

  def next_task(self):
    task = MainTask(self.state, self.frame_time + self.state.frame_interval)

    # This check prevents us from continually discarding every main frame we
    # get because of things going horribly wrong.
    if self.state.discarded_main_frames > math.ceil(self.state.main_time / self.avaliable_time):
      task.discard_enable = False

    return task

  def should_requeue_without_discard(self, now):
    time_missed_by = self.start_before - now

    # Optimisation
    # We are going to miss the deadline by a short period but haven't missed
    # any previous frames, lets give it a chance.
    if time_missed_by < 1*ms and self.state.discarded_main_frames == 0:
      return True

    # If we have missed the deadline by more then a frame interval, then this
    # task is horribly out of date and we should just discard it.
    if time_missed_by > self.state.frame_interval:
      return False

    # Otherwise, we need to figure out if attempting this main task will mean
    # more main tasks don't meet their deadlines.

    # How much of the available time the main task takes up determines if
    # re-queuing will effect following main frames.
    main_percentage_of_interval = self.state.main_time / self.avaliable_time

    if main_percentage_of_interval < 0.5:
      # If the main thread can render twice a frame then starting rendering
      # shouldn't cause the next frame to miss it's deadline. Hence we should
      # just try and catch up here.
      return True

    if main_percentage_of_interval >= 1.0:
      # Values greater than 1.0 mean the main thread can *never* make the
      # normal deadline.

      # Could we make an extended deadline?
      if main_percentage_of_interval < 2.0:
        # Adjust the deadline and requeue the task
        self.deadline += self.state.frame_interval

      # The main thread isn't even going an extended deadline, just target
      # the highest throughput.
      return True

    # For percentages between 0.5 and 1.0 then starting rendering too late
    # will cause the next frame to also miss the deadline.
    
    # Work out how late this main task can be before running into the next
    # task.
    next_task = self.next_task()
    if not next_task.discard_enable:
      return False

    self.deadline = next_task.start_before
    return True

  def on_discard(self, scheduler, now):
    # The main task was unable to make the deadline, so this task is being
    # discarded.
    if self.should_requeue_without_discard(now):
      self.discard_enable = False
      scheduler.add_task(self)
    else:
      self.state.discarded_main_frames += 1
      scheduler.add_task(self.next_task())

  def on_run(self, scheduler, now):
    if self.state.frames > 0:
      scheduler.add_task(self.next_task())


class CompositeTask(Task):

  def earlist_run(self, now):
    return self.frame_time

  def __init__(self, state, frame_time):
    self.state = state
    self.frame_time = frame_time
    Task.__init__(self,
      name = "CompositeTask@%i" % frame_time,
      length = state.composite_time,
      deadline = frame_time + self.state.frame_interval)

  def next_task(self):
    return CompositeTask(self.state, self.frame_time+self.state.frame_interval)

  def on_run(self, scheduler, now):
    if self.state.frames > 0:
      self.state.frames -= 1
      scheduler.add_task(self.next_task())


class ChromeSchedulerTest(SchedulerTest):

  def setUp(self):
    SchedulerTest.setUp(self)

  def test_single_normal_frame(self):
    self.c = ChromeState(frames = 0, main_time = 2*ms)

    frame_t0 = 1*ms
    self.add_task(MainTask(self.c, frame_t0))
    self.add_task(CompositeTask(self.c, frame_t0))
    self.assertAdds(2,
      "XXXXX: Adding Task('MainTask@1000', 2000, 13000)",
      "XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "01000: Running Task('MainTask@1000', 2000, 13000)",
      "03000: Running Task('CompositeTask@1000', 4000, 17000)",
      start_at=frame_t0,
      end_at=7*ms)
    self.assertAdds(0)

  def test_multiple_normal_frame(self):
    self.c = ChromeState(frames = 1, main_time = 2*ms)

    frame_t0 = 1*ms
    self.add_task(MainTask(self.c, frame_t0))
    self.add_task(CompositeTask(self.c, frame_t0))
    self.assertAdds(2,
      "XXXXX: Adding Task('MainTask@1000', 2000, 13000)",
      "XXXXX: Adding Task('CompositeTask@1000', 4000, 17000)",
    )
    self.assertRun(
      "01000: Running Task('MainTask@1000', 2000, 13000)",
      "03000: Running Task('CompositeTask@1000', 4000, 17000)",
      "17000: Running Task('MainTask@17000', 2000, 29000)",
      "19000: Running Task('CompositeTask@17000', 4000, 33000)",
      start_at=frame_t0)
    self.assertAdds(2,
      "01000: Adding Task('MainTask@17000', 2000, 29000)",
      "03000: Adding Task('CompositeTask@17000', 4000, 33000)",
    )


if __name__ == '__main__':
    unittest.main()
