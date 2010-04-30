from __future__ import with_statement

import os
import socket
import select
import logging
import threading
import logging.handlers

from maried.core import Module
from maried.io import IntSocketFile
from maried.runtime import ExceptionCatchingWrapper

class LogServer(Module):
	class SocketHandler(logging.handlers.SocketHandler):
		def __init__(self, socket):
			self.__socket = socket
			logging.handlers.SocketHandler.__init__(self, None,
					None)
		def makeSocket(self):
			return self.__socket

	class Handler(object):
		def __init__(self, server, _socket, l):
			self.l = l
			self.server = server
			self.f = ExceptionCatchingWrapper(
					IntSocketFile(_socket),
					self._on_exception)
			self.handler = LogServer.SocketHandler(self.f)
			self.socket = _socket
			self._sleep_socket_pair = socket.socketpair()
			self.running = True

		def _on_exception(self, attr, exc):
			if attr == 'send':
				self.interrupt()
				return 0
			raise exc

		def interrupt(self):
			self.f.interrupt()
			self.running = False
			self._sleep_socket_pair[0].send('good morning!')

		def handle(self):
			logging.getLogger('').addHandler(self.handler)
			while self.server.running and self.running:
				rlist, wlist, xlist = select.select((
					self._sleep_socket_pair[1],), (),
					(self.socket,))
				if self.socket in xlist:
					self.l.error("select(2) says socket "+
							"in error")
					break
				if self._sleep_socket_pair[1] in rlist:
					self._sleep_socket_pair[1].recv(4096)
			logging.getLogger('').removeHandler(self.handler)

	def __init__(self, settings, logger):
		super(LogServer, self).__init__(settings, logger)
		self.running = False
		self._sleep_socket_pair = socket.socketpair()
		self.n_conn = 0
		self.handlers = set()
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
			self.threadPool.execute(self._handle_request,
						con, addr, self.n_conn)
	def _handle_request(self, con, addr, n_conn):
		l = logging.getLogger("%s.%s" % (self.l.name, n_conn))
		l.info('Accepted connection from %s' % addr)
		handler = LogServer.Handler(self, con, l)
		with self.lock:
			self.handlers.add(handler)
		try:
			handler.handle()
		except IOError:
			l.warn("Handler caught IOError")
		finally:
			with self.lock:
				self.handlers.remove(handler)

	def stop(self):
		self.running = False
		self._sleep_socket_pair[0].send('good morning!')
		with self.lock:
			handlers = set(self.handlers)
		for handler in handlers:
			handler.interrupt()
