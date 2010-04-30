import select
import socket

class CappedReadFile(object):
	def __init__(self, f, cap):
		self.f = f
		self.left = cap
	def read(self, n):
		if n <= self.left:
			self.left -= n
			return self.f.read(n)
		if self.left == 0:
			return ''
		ret = self.f.read(self.left)
		self.left = 0
		return ret

class BufferedFile(object):
	""" Wraps around a normal fileobject, buffering IO writes,
		@f	the file to wrap
		@n	the buffer size """
	def __init__(self, f, n=4096):
		self.f = f
		self.n = n
		# Maybe a cStringIO would be faster.  However <n> is still
		# pretty small in comparison.
		self.buf = ''
	
	def read(self, n=None):
		if n is None:
			return self.f.read()
		return self.f.read(n)
	
	def write(self, s):
		if len(s) + len(self.buf) >= self.n:
			self.f.write(self.buf + s)
			self.buf = ''
		else:
			self.buf += s
	
	def flush(self):
		self.f.write(self.buf)
		self.buf = ''
	
	def close(self):
		self.f.close()

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
			written = self.socket.send(to_write)
			if written <= 0:
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
	def readsome(self, amount=2048):
		if len(self.read_buffer) != 0:
			ret = self.read_buffer
			self.read_buffer = ''
			return self.read_bufffer
		rlist, wlist, xlist = select.select(
				[self._sleep_socket_pair[1],
				 self.socket], [],
				[self.socket])
		if (self._sleep_socket_pair[1] in rlist and
		    not self.running):
			return
		if self.socket in xlist:
			raise IOError
		if not self.socket in rlist:
			return
		return self.socket.recv(amount)

	def recv(self, amount=2048):
		return self.readsome(amount)
	def send(self, data):
		self.write(data)
		return len(data)
	def close(self):
		self.socket.close()

	def interrupt(self):
		self.running = False
		self._sleep_socket_pair[0].send('Good morning!')
	def flush(self):
		pass
