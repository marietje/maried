from __future__ import with_statement

from maried.mirte import Event, Module

import logging
import threading

class ThreadPool(Module):
	class Worker(threading.Thread):
		def __init__(self, pool, l):
			threading.Thread.__init__(self)
			self.l = l
			self.pool = pool
		def run(self):
			self.l.debug("Hello")
			self.pool.cond.acquire()
			self.pool.actualFT += 1
			while True:
				if not self.pool.running:
					break
				if not self.pool.jobs:
					self.pool.cond.wait()
					continue
				job = self.pool.jobs.pop()
				self.pool.actualFT -= 1
				self.pool.cond.release()
				try:
					ret = job()
				except Exception, e:
					self.l.exception("Uncatched exception")
					ret = True
				self.pool.cond.acquire()
				self.pool.actualFT += 1
				self.pool.expectedFT += 1
				if not ret:
					break
			self.pool.actualFT -= 1
			self.pool.expectedFT -= 1
			self.pool.workers.remove(self)
			self.pool.cond.release()
			self.l.debug("Bye")

	def __init__(self, settings, logger):
		super(ThreadPool, self).__init__(settings, logger)
		self.running = True
		self.jobs = list()
		self.cond = threading.Condition()
		self.mcond = threading.Condition()
		self.actualFT = 0
		self.expectedFT = 0
		self.ncreated = 0
		self.workers = set()
	
	def _remove_worker(self):
		self._queue(lambda: False)
	
	def _create_worker(self):
		self.ncreated += 1
		self.expectedFT += 1
		n = self.ncreated
		l = logging.getLogger("%s.%s" % (self.l.name, n)) 
		t = ThreadPool.Worker(self, l)
		self.workers.add(t)
		t.start()
	
	def run(self):
		self.mcond.acquire()
		while self.running:
			self.cond.acquire()
			gotoSleep = False
			tc = self.minFree - self.expectedFT + len(self.jobs)
			td = self.expectedFT - len(self.jobs) - self.maxFree
			if tc > 0:
				for i in xrange(tc):
					self._create_worker()
			elif td > 0:
				for i in xrange(td):
					self._remove_worker()
			else:
				gotoSleep = True
			self.cond.release()
			if gotoSleep:	
				self.mcond.wait()
		self.l.info("Waking and joining all workers")
		with self.cond:
			self.cond.notifyAll()
			workers = list(self.workers)
		self.mcond.release()
		for worker in workers:
			worker.join()
		self.l.info("   joined")
	def stop(self):
		self.running = False
		with self.mcond:
			self.mcond.notify()
	
	def _queue(self, raw):
		if self.actualFT == 0:
			self.l.warn("No actual free threads, yet "+
				    "(increase threadPool.minFree)")
		self.jobs.append(raw)
		self.expectedFT -= 1
		self.cond.notify()
		self.mcond.notify()
	
	def execute(self, function, *args, **kwargs):
		def _entry():
			function(*args, **kwargs)
			return True
		with self.mcond:
			with self.cond:
				self._queue(_entry)
