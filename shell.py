from __future__ import with_statement

import sys
import code
import socket
import select
import os.path
import logging
import threading

from core import Module

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

class IntSocketFile(object):
	""" IntSocketFile(s) ~ s.makefile(), but has a nice
	    interrupt function """
	# Yeah, the annoying branches are a bit dense in this code.
	def __init__(self, sock):
		self.socket = sock
		sock.setblocking(0)
		self._sleep_socket_pair = socket.socketpair()
		self.running = True
		self.read_buffer = ''
	def write(self, v):
		to_write = v
		while self.running and len(to_write) > 0:
			rlist, wlist, xlist = select.select(
					[self._sleep_socket_pair[1]],
				      	[self.socket],
					[self.socket])
			if (self._sleep_socket_pair[1] in rlist and
			    not self.running):
				break
			if self.socket in xlist:
				raise IOError
			if not self.socket in wlist:
				continue
			written = self.socket.send(v)
			if written == 0:
				raise IOError
			to_write = to_write[written:]
	def read(self, n):
		to_read = n
		ret = ''
		if len(self.read_buffer) > 0:
			if len(self.read_buffer) >= n:
				ret = self.read_buffer[:n]
				self.read_buffer = self.read_buffer[n:]
				return ret
			ret = self.read_buffer
			self.read_buffer = ''
			to_read -= len(ret)
		while self.running and to_read > 0:
			rlist, wlist, xlist = select.select(
					[self._sleep_socket_pair[1],
					 self.socket], [],
					[self.socket])
			if (self._sleep_socket_pair[1] in rlist and
			    not self.running):
				break
			if self.socket in xlist:
				raise IOError
			if not self.socket in rlist:
				continue
			tmp = self.socket.recv(min(2048, to_read))
			if len(tmp) == 0:
				raise IOError
			ret += tmp
			to_read -= len(tmp)
		return ret
	def readline(self):
		ret = ''
		bit = self.read_buffer
		self.read_buffer = ''
		while self.running:
			if not "\n" in bit:
				ret += bit
			else:
				bit, rem = bit.split("\n", 1)
				ret += bit + "\n"
				self.read_buffer += rem
				return ret
			rlist, wlist, xlist = select.select(
					[self._sleep_socket_pair[1],
					 self.socket], [],
					[self.socket])
			if (self._sleep_socket_pair[1] in rlist and
			    not self.running):
				break
			if self.socket in xlist:
				raise IOError
			if not self.socket in rlist:
				continue
			bit = self.socket.recv(1024)
			if len(bit) == 0:
				raise IOError
		return ''
	def interrupt(self):
		self.running = False
		self._sleep_socket_pair[0].send('Good morning!')
	def flush(self):
		pass

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
		self.f.write(v)
	def cleanup(self):
		sys.stdout.deregister(self.on_std_write)
		sys.stderr.deregister(self.on_std_write)
		logging.getLogger('').removeHandler(self.log_handler)
	def interrupt(self):
		self.f.interrupt()

class ShellServer(Module):
	def __init__(self, settings, logger):
		super(ShellServer, self).__init__(settings, logger)
		self.running = False
		self._sleep_socket_pair = socket.socketpair()
		self.n_conn = 0
		self.consoles = set()
		self.lock = threading.Lock()
	def run(self):
		assert not self.running
		self.running = True
		if os.path.exists(self.socketPath):
			os.unlink(self.socketPath)
		s = self.socket = socket.socket(socket.AF_UNIX,
						socket.SOCK_STREAM)
		s.bind(self.socketPath)
		s.listen(3)
		while self.running:
			rlist, wlist, xlist = select.select([s,
				self._sleep_socket_pair[1]], [], [s])
			if (self._sleep_socket_pair[1] in rlist and
			    not self.running):
				break
			if s in xlist:
				self.l.error("select(2) says socket in error")
				break
			if not s in rlist:
				continue
			con, addr = s.accept()
			self.n_conn += 1
			t = threading.Thread(target=self._handle_request,
					     args=(con, addr, self.n_conn))
			t.start()
	def _handle_request(self, con, addr, n_conn):
		l = logging.getLogger("%s.%s" % (self.l.name, n_conn))
		l.info('Accepted connection from %s' % addr)
		console = SockConsole(con, {})
		with self.lock:
			self.consoles.add(console)
		try:
			console.interact()
		except IOError:
			l.warn("Console caught IOError")
		finally:
			with self.lock:
				self.consoles.remove(console)
			console.cleanup()

	def stop(self):
		self.running = False
		self._sleep_socket_pair[0].send('good morning!')
		with self.lock:
			consoles = set(self.consoles)
		for console in consoles:
			console.interrupt()
