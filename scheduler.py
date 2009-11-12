from __future__ import with_statement

from maried.mirte import Event, Module
from heapq import heappush, heappop

import time
import select
import random
import socket
import logging
import threading

class Scheduler(Module):
	class Entry:
		def __init__(self, time, action):
			self.time = time
			self.action = action
		def __cmp__(self, other):
			return cmp(self.time, other.time)

	def __init__(self, settings, logger):
		super(Scheduler, self).__init__(settings, logger)
		self.running = True
		self.sp = socket.socketpair()
		self.queue = []
		# estimated gap between threadPool.execute and execution
		self.gap = 0.0
		# estimated oversleep on time.sleep
		self.delay = 0.0
	
	def _calibrate(self, staged_at):
		diff = time.time() - staged_at
		if abs(diff) < self.calibration_goal:
			return
		self.l.info('Calibration diff: %s' % diff)
		next = time.time() + self.calibration_delay
		self.plan(next, self._calibrate, next)

	def run(self):
		self._calibrate(time.time())
		while self.running:
			if len(self.queue) == 0:
				timeout = None
			else:
				timeout = (self.queue[0].time - time.time()
						- self.gap)
				if timeout <= 0:
					event = heappop(self.queue)
					self._stage(event)
					continue
			rl, wl, xl = select.select([self.sp[1]], [], [],
					timeout)
			if self.sp[1] in rl:
				self.sp[1].recv(4096)
	
	def _stage(self, event):
		self.threadPool.execute(self._sleep_for, event, time.time())
	
	def _sleep_for(self, event, staged_at):
		self.gap = .2 * 10 * (time.time() - staged_at) + .8 * self.gap
		tosleep = event.time - time.time() - self.delay
		if tosleep > 0:
			time.sleep(tosleep)
		self.delay = 0.8 * self.delay + \
			     0.2 * (time.time() - event.time + self.delay)
		event.action()

	def plan(self, time, func, *args, **kwargs):
		def action():
			func(*args, **kwargs)
		heappush(self.queue, Scheduler.Entry(time, action))
		self._wake()
	
	def _wake(self):
		self.sp[0].send('!')
	
	def stop(self):
		self.running = False
		self._wake()
