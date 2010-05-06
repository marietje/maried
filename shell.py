from __future__ import with_statement

import sys
import code
import socket
import select
import os.path
import logging
import threading

from maried.core import Module, UnixSocketServer
from maried.io import IntSocketFile

class HushFile(object):
	""" Wraps around a file, but hushes all errors """
	def __init__(self, f):
		self.f = f
	def write(self, v):
		try: self.f.write(v)
		except: pass
	def read(self, v):
		try: return self.f.read(v)
		except: return ''
	def flush(self):
		try: self.f.flush()
		except: pass
	def readline(self):
		try: return self.f.readline()
		except: return ''

class FileMux(object):
	""" (Barely) wraps a file for writing and notifies handlers about
	    these writes. """
	def __init__(self, old):
		self.old = old
		self.handlers = set()
	def write(self, v):
		self.old.write(v)
		for handler in self.handlers:
			handler(v)
	def register(self, handler):
		self.handlers.add(handler)
	def deregister(self, handler):
		self.handlers.remove(handler)

class SockConsole(code.InteractiveConsole):
	""" The InteractiveConsole addapted for usage with ShellServer """
	def __init__(self, sock, locals):
		code.InteractiveConsole.__init__(self, locals)
		self.f = IntSocketFile(sock)
		if not isinstance(sys.stdout, FileMux):
			sys.stdout = FileMux(sys.stdout)
		if not isinstance(sys.stderr, FileMux):
			sys.stderr = FileMux(sys.stderr)
		sys.stdout.register(self.on_std_write)
		sys.stderr.register(self.on_std_write)
		self.log_handler = logging.StreamHandler(HushFile(self.f))
		formatter = logging.Formatter(
				"%(levelname)s:%(name)s:%(message)s")
		self.log_handler.setFormatter(formatter)
		logging.getLogger('').addHandler(self.log_handler)
	def raw_input(self, prompt):
		self.f.write(prompt)
		ret = self.f.readline()
		if ret == '': raise IOError
		return ret[:-1]
	def on_std_write(self, v):
		try:
			self.f.write(v)
		except Exception:
			pass
	def cleanup(self):
		sys.stdout.deregister(self.on_std_write)
		sys.stderr.deregister(self.on_std_write)
		logging.getLogger('').removeHandler(self.log_handler)
	def interrupt(self):
		self.f.interrupt()
	def handle(self):
		self.interact()

class ShellServer(UnixSocketServer):
	def create_handler(self, con, addr, logger):
		locals = {'manager': self.manager}
		for k, ii in self.manager.insts.iteritems():
			locals[k] = ii.object
		console = SockConsole(con, locals)
		return console
