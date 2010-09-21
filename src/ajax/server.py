from mirte.core import Module
from maried.core import Denied
from sarah.event import Event
from sarah.io import IntSocketFile
from sarah.socketServer import TCPSocketServer

import threading
import os.path
import logging
import maried
import time

from cStringIO import StringIO
from xml.dom.minidom import Document
from BaseHTTPServer import BaseHTTPRequestHandler

class AjaxServerHandlerWrapper(object):
	def __init__(self, request, addr, server, l):
		self.request = IntSocketFile(request)
		self.addr = addr
		self.server = server
		self.l = l
	def handle(self):
		self.h = AjaxServerHandler(self.request, self.addr,
					self.server, self.l)
	def interrupt(self):
		self.request.interrupt()
	def cleanup(self):
		self.request.close()

class AjaxServerHandler(BaseHTTPRequestHandler):
	def __init__(self, request, addr, server, l):
		self.l = l
		self.path_map = {'htdocs': self.do_htdocs,
				 '': self.do_htdocs,
				 'requests': self.do_requests,
				 'media': self.do_media,
				 'request': self.do_request,
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
	def do_request(self):
		bits = self.path.split('/')
		if len(bits) < 5:
			self.send_error(400, "Wrong request")
			return
		user, password, media = bits[2:5]
		try: user = self.server.desk.user_by_key(user)
		except KeyError: return self._respond_to_request('wrong-login')
		try: media = self.server.desk.media_by_key(media)
		except KeyError: return self._respond_to_request('wrong-media')
		if not user.check_password(password):
			return self._respond_to_request('wrong-login')
		try:
			self.server.desk.request_media(media, user)
		except Denied, e:
			return self._respond_to_request('denied', repr(e))
		self._respond_to_request('ok')

	def _respond_to_request(self, code, message=None):
		self.send_response(200)
		self.send_header('Content-type', 'text/xml')
		self.end_headers()
		doc = Document()
		n_stat = doc.createElement('status')
		doc.appendChild(n_stat)
		n_stat.setAttribute('code', code)
		if not message is None:
			n_stat.setAttribute('message', message)
		self.wfile.write(doc.toprettyxml(indent="  "))
		
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
			n_req.setAttribute('media',  str(request.media.key))
			n_req.setAttribute('artist', str(request.media.artist))
			n_req.setAttribute('title',  str(request.media.title))
			n_req.setAttribute('length', str(request.media.length))
			n_reqs.appendChild(n_req)
		self.wfile.write(doc.toprettyxml(indent="  "))
	def do_media(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/javascript')
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
			n_play.setAttribute('requestedBy', str(req.by.key))
		n_play.setAttribute('artist', str(media.artist))
		n_play.setAttribute('title',  str(media.title))
		n_play.setAttribute('length', str(media.length))
		n_play.setAttribute('endTime',
				str(time.mktime(endTime.timetuple())))
		n_play.setAttribute('serverTime',
				str(time.time()))
		self.wfile.write(doc.toprettyxml(indent="  "))	

class AjaxServer(TCPSocketServer):
	def __init__(self, *args, **kwargs):
		super(AjaxServer, self).__init__(*args, **kwargs)
		self.MR = None
		self.MR_cond = threading.Condition()
		self.desk.on_media_changed.register(
				self.on_media_changed)
		self.htdocs_path = os.path.join(
				os.path.dirname(maried.ajax.server.__file__),
				'htdocs')
		if not os.path.exists(self.htdocs_path):
			self.l.error("%s doens't exist!" % self.htdocs_path)
		self.on_media_changed()
	def on_media_changed(self):
		self.threadPool.execute_named(self.do_refresh_MR,
				'%s do_refresh_MR' % self.name)
	def do_refresh_MR(self):
		self.l.info("Refreshing cached /media response")
		self.l.info
		doc = {}
		b = StringIO()
		b.write('{')
		first = True
		for media in sorted(self.desk.list_media(),
				    cmp=lambda x,y: 2*cmp(x.artist, y.artist) +
				    		    cmp(x.title, y.title)):
			if first: first = False
			else: b.write(',')
			b.write("_%s:[%s,%s,%s]" % (media.key,
						        repr(media.artist),
						        repr(media.title),
						        media.length))
		b.write('}')
		txt = b.getvalue()
		with self.MR_cond:
			self.MR = txt
			self.MR_cond.notifyAll()
	def create_handler(self, con, addr, logger):
		return AjaxServerHandlerWrapper(con, addr, self, logger)

	def _handle_request(self, conn, addr, n):
		l = logging.getLogger("%s.%s" % (self.l.name, n))
		l.debug("Accepted connection from %s" % repr(addr))
		rh = AjaxServerHandler(conn, addr, self, l)
		conn.close()
