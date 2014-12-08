
# Python Scheduler Simulator

This repository contains a simulator for a scheduler for cooperatively
scheduling tasks with mixture of deadline and other tasks.

The primary aim of this repository is to draw nice diagrams of how scheduled
tasks interact to allow explanation of task interaction quick experimentation
with different scheduling models.

This code is written in Python **3** and has the following dependencies;
 * None currently.
 * Probably need matplotlib or other graphics library shortly.

## TODO List

 - [ ] Add the ability to actually generate diagrams from traces.
 - [ ] Rewrite the scheduler / tasks to allow easier tracing rather then the
       current horrible hacks.
 - [ ] Add tasks which have uncertainty in their run duration.
 - [ ] Add tasks with unknown run duration.
 - [ ] Make `scheduler.py` just contain tasks and scheduler interface. Move
       current scheduler implementations to different file.
 - [ ] Add support for preemption + preemptable tasks.

# Chrome Compositor Scheduler Simulator

The `chrome_scheduler.py` file provides a set of tasks and scheduler set ups to
partially emulate the
[Compositor scheduler in Chrome](https://code.google.com/p/chromium/codesearch#chromium/src/cc/scheduler/).

## TODO List

 - [ ] Add whole Commit->Activate->Draw tasks path.
 - [ ] Look at emulating the Blink scheduler tasks.

