from maried.core import Module, Event
import threading
import os.path
import logging
import maried
import socket
import select
import time

from xml.dom.minidom import Document
from BaseHTTPServer import BaseHTTPRequestHandler

class AjaxServerHandler(BaseHTTPRequestHandler):
	def __init__(self, request, addr, server, l):
		self.l = l
		self.path_map = {'htdocs': self.do_htdocs,
				 '': self.do_htdocs,
				 'requests': self.do_requests,
				 'media': self.do_media,
				 'playing': self.do_playing}
		BaseHTTPRequestHandler.__init__(self, request, addr, server)
	def log_message(self, format, *args, **kwargs):
		self.l.info(format, *args, **kwargs)
	def log_error(self, format, *args, **kwargs):
		self.l.error(format, *args, **kwargs)
	def log_request(self, code=None, size=None):
		self.l.info("Request: %s %s" % (code, size))
	def do_GET(self):
		bits = self.path.split('/')
		if len(bits) < 1:
			bits = ('', '')
		if not bits[1] in self.path_map:
			self.send_error(404, "No such action")
			return
		self.path_map[bits[1]]()
	def do_htdocs(self):
		bits = self.path.split('/')
		file = '' if len(bits) < 3 else os.path.basename(bits[2])
		if file == '': file = 'index.html'
		path = os.path.join(self.server.htdocs_path, file)
		if not os.path.isfile(path):
			self.send_error(404, "No such file")
			return
		self.send_response(200)
		self.end_headers()
		with open(path, 'r') as f:
			while True:
				tmp = f.read(4096)
				if len(tmp) == 0: break
				self.wfile.write(tmp)
	def do_requests(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/xml')
		self.end_headers()
		doc = Document()
		n_reqs = doc.createElement('requests')
		doc.appendChild(n_reqs)
		for request in self.server.desk.list_requests():
			n_req = doc.createElement('request')
			if not request.by is None:
				n_req.setAttribute('by', str(request.by.key))
			n_req.setAttribute('media', str(request.media.key))
			n_reqs.appendChild(n_req)
		self.wfile.write(doc.toprettyxml(indent="  "))
	def do_media(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/xml')
		self.end_headers()
		with self.server.MR_cond:
			if self.server.MR is None:
				self.l.info('No cached /media response yet.'+
						' Waiting...')
				self.server.MR_cond.wait()
			txt = self.server.MR
		self.wfile.write(txt)
	def do_playing(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/xml')
		self.end_headers()
		media, req, endTime = self.server.desk.get_playing()
		doc = Document()
		n_play = doc.createElement('playing')
		doc.appendChild(n_play)
		n_play.setAttribute('media', str(media.key))
		if not req is None:
			n_play.setAttribute('requestedBy', req.by)
		n_play.setAttribute('endTime',
				str(time.mktime(endTime.timetuple())))
		self.wfile.write(doc.toprettyxml(indent="  "))	

class AjaxServer(Module):
	def __init__(self, settings, logger):
		super(AjaxServer, self).__init__(settings, logger)
		self.running = True
		self._ssp = socket.socketpair()
		self.n_conn = 0
		self.MR = None
		self.MR_cond = threading.Condition()
		self.desk.on_media_changed.register(
				self.on_media_changed)
		self.htdocs_path = os.path.join(
				os.path.dirname(maried.ajax.server.__file__),
				'htdocs')
		if not os.path.exists(self.htdocs_path):
			self.l.error("%s doens't exist!" % self.htdocs_path)
	def on_media_changed(self):
		self.threadPool.execute(self.do_refresh_MR)
	def do_refresh_MR(self):
		self.l.info("Refreshing cached /media response")
		self.l.info
		doc = Document()
		n_media = doc.createElement('media')
		doc.appendChild(n_media)
		for media in self.desk.list_media():
			n_m = doc.createElement('media')
			try:
				n_m.setAttribute('uploadedBy',
						str(media.uploadedBy.key))
			except KeyError:
				pass
			n_m.setAttribute('key', str(media.key))
			n_m.setAttribute('artist', media.artist)
			n_m.setAttribute('title', media.title)
			n_m.setAttribute('length', str(media.length))
			n_m.setAttribute('uploadedTimestamp',
					str(media.uploadedTimestamp))
			n_media.appendChild(n_m)
		txt = doc.toprettyxml(indent="  ")
		with self.MR_cond:
			self.MR = txt
			self.MR_cond.notifyAll()

	def run(self):
		s = self.socket = socket.socket(socket.AF_INET,
						socket.SOCK_STREAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		try:
			s.bind((self.host, self.port))
			s.listen(3)
			self._inner_run(s)
		finally:
			s.close()
	def _inner_run(self, s):
		while self.running:
			rlist, wlist, xlist = select.select(
					[self._ssp[1], s], [],
					[self._ssp[1], s])
			if (self._ssp[1] in rlist and
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
	def _handle_request(self, conn, addr, n):
		l = logging.getLogger("%s.%s" % (self.l.name, n))
		l.debug("Accepted connection from %s" % repr(addr))
		rh = AjaxServerHandler(conn, addr, self, l)
		conn.close()

	def stop(self):
		self.running = False
		self._ssp[0].send("Rise and shine!")
