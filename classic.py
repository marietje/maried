from __future__ import with_statement

from mirte.core import Module
from maried.core import *
from sarah.event import Event
from sarah.socketServer import TCPSocketServer
from sarah.io import *

import os
import time
import socket
import random
import os.path
import MySQLdb
import hashlib
import logging
import weakref
import datetime
import tempfile
import cStringIO
import threading
import contextlib
import subprocess

class AlreadyInQueueError(Denied):
	pass
class MaxQueueLengthExceededError(Denied):
	pass
class MaxQueueCountExceededError(Denied):
	pass

class ClassicMedia(Media):
	def __init__(self, coll, key, artist, title, length, mediaFileKey,
			   uploadedByKey, uploadedTimestamp, trackGain,
			   trackPeak):
		self.coll = coll
		self._key = key
		self.artist = artist
		self.title = title
		self.length = length
		self.mediaFileKey = mediaFileKey
		self.uploadedByKey = uploadedByKey
		self.uploadedTimestamp = uploadedTimestamp
		self.trackGain = trackGain
		self.trackPeak = trackPeak
	
	@property
	def uploadedBy(self):
		return self.coll._user_by_key(self.uploadedByKey)
	@property
	def mediaFile(self):
		return self.coll.mediaStore.by_key(self.mediaFileKey)
	
	@property
	def key(self):
		return self._key

	def save(self):
		self.coll._save_media(self)
	
	def unlink(self):
		self.coll._unlink_media(self)

	def __repr__(self):
		return "<ClassicMedia %s - %s>" % (self.artist, self.title)

	def __eq__(self, other):
		return self._key == other.key
	def __ne__(self, other):
		return self._key != other.key

class ClassicMediaFile(MediaFile):
	def __init__(self, store, path, key):
		super(ClassicMediaFile, self).__init__(store, key)
		self.path = path
	def open(self):
		return open(self.path)
	def get_named_file(self):
		return self.path
	def __repr__(self):
		return "<ClassicMediaFile %s>" % self._key

class ClassicBaseRequest(object):
	def __init__(self, mediaKey, byKey):
		self.mediaKey = mediaKey
		self.byKey = byKey
	@property
	def media(self):
		return self.collection.by_key(self.mediaKey)
	@property
	def by(self):
		if self.byKey is None:
			return None
		return self.collection._user_by_key(self.byKey)

class ClassicPastRequest(PastRequest, ClassicBaseRequest):
	def __init__(self, history, key, mediaKey, byKey, at):
		ClassicBaseRequest.__init__(self, mediaKey, byKey)
		self.at = at
		self.key = key
		self.history = history
		self.collection = history.collection
	def __repr__(self):
		return "<ClassicPastRequest %s - %s @ %s>" % (
				self.byKey,
				repr(self.media),
				self.at)
	def remove(self):
		self.history._remove_request(self)

class ClassicRequest(Request, ClassicBaseRequest):
	def __init__(self, queue, key, mediaKey, byKey):
		ClassicBaseRequest.__init__(self, mediaKey, byKey)
		self.key = key
		self.collection = queue.collection
	def __repr__(self):
		return "<ClassicRequest %s - %s>" % (self.byKey,
						     repr(self.media))

class ClassicUser(User):
	def __init__(self, coll, key, realName, level, passwordHash):
		super(ClassicUser, self).__init__(key, realName)
		self.level = level
		self.coll = coll
		self.passwordHash = passwordHash
	@property
	def key(self):
		return self._key
	def __repr__(self):
		return "<ClassicUser %s %s>" % (self._key,
						self.realName)
	@property
	def has_access(self):
		return self.level >= 2
	@property
	def may_cancel(self):
		return self.level >= 3
	@property
	def may_move(self):
		return self.level >= 3
	def check_password(self, password):
		return self.passwordHash == hashlib.md5(password).hexdigest()

class ClassicUsers(Users):
	def assert_request(self, user, media):
		if not user.has_access:
			raise Denied
		requests = self.queue.requests
		if any(map(lambda x: x.media == media, requests)):
			raise AlreadyInQueueError
		ureqs = filter(lambda y: y.by == user, requests)
		if len(ureqs) > self.maxQueueCount:
			raise MaxQueueCountExceededError
		if (sum(map(lambda x: x.media.length, ureqs)) > 
				self.maxQueueLength):
			raise MaxQueueLengthExceededError
	def assert_addition(self, user, mediaFile):
		pass
	def assert_cancel(self, user, request):
		if request.by == user:
			return
		if not user.may_cancel:
			raise Denied
	def assert_move(self, user, request, amount):
		if request.by == user and amount < 0:
			return
		if not user.may_move:
			raise Denied
	def by_key(self, key):
		return self.collection._user_by_key(key)

class ClassicQueue(Queue):
	def request(self, media, user):
		self.db.queue_request(media.key, user.key)
	@property
	def requests(self):
		ret = list()
		for tmp in self.db.queue_get_requests():
			ret.append(ClassicRequest(self, *tmp))
		return ret
	def shift(self):
		if not self.db.ready:
			raise EmptyQueueException
		tmp = self.db.queue_shift()
		if tmp is None:
			raise EmptyQueueException
		return ClassicRequest(self, *tmp)
	def cancel(self, request):
		raise NotImplementedError
	def move(self, request, amount):
		raise NotImplementedError

class ClassicHistory(History):
	def __init__(self, settings, logger):
		super(ClassicHistory, self).__init__(settings, logger)
		self.db.on_changed.register(self._on_db_changed)
	
	def _on_db_changed(self):
		self.on_pretty_changed()

	def record(self, media, request, at):
		self.l.info(repr(media if request is None else request))
		timeStamp = time.mktime(at.timetuple())
		byKey = "Marietje" if request is None else request.by.key
		trackId = media.key
		self.db.history_record(byKey, trackId, timeStamp)
		self.on_record(ClassicPastRequest(
			self,
			None,
			media.key,
			None if request is None else request.by.key,
			at))
	
	def list_past_requests(self):
		for logId, timeStamp, userName, trackId \
				in self.db.history_list():
			at = datetime.datetime.fromtimestamp(timeStamp)
			yield ClassicPastRequest(self,
						 logId,
					         trackId,
						 userName,
						 at)
	
	def _remove_request(self, req):
		self.db.history_remove_request(req.key)
		# As _remove_request is very infrequent and if used, is
		# used in volume, we won't call self.on_pretty_changed.

class ClassicDesk(Desk):
	pass

class ClassicRequestServer(TCPSocketServer):
	class Handler(object):
		def __init__(self, server, conn, addr, logger):
			self.server = server
			self.f = IntSocketFile(conn)
			self.addr = addr
			self.l = logger
			self.cmd_map = {
				'LIST::QUEUE': self.handle_list_queue,
				'LIST::NOWPLAYING': self.handle_nowplaying,
				'LIST::ALL': self.handle_list_all,
				'LIST::PLAYLISTS::USER::': self.handle_list_pls,
				'REQUEST::SONG::': self.handle_request_song,
				'REQUEST::UPLOAD::': self.handle_request_upload,
				'LOGIN::USER::': self.handle_login_user}

		def cleanup(self):
			self.f.close()

		def handle_list_queue(self, cmd):
			queue = self.server.desk.list_requests()
			endTime = self.server.desk.get_playing()[2]
			timeLeft = (0 if endTime is None else
				int(time.mktime(endTime.timetuple()) -
				time.mktime(datetime.datetime.now(
					).timetuple())))
			self.f.write("TOTAL::%s::TIMELEFT::%s\n" % (len(queue),
							       int(timeLeft)))
			wf = BufferedFile(self.f)
			for req in queue:
				media = req.media
				byKey = ('marietje' if req.by is None
						    else req.by.key)
				wf.write("SONG::%s::%s::%s::%s\n" % (
					media.artist, media.title,
					media.length, byKey))
			wf.flush()

		def handle_nowplaying(self, cmd):
			media, request, endTime = self.server.desk.get_playing()
			if (media is None or endTime is None):
				self.l.warn("Not properly playing, yet")
				return
			endTimeTS = int(time.mktime(endTime.timetuple()) -
					media.length)
			timeTS = int(time.mktime(
				datetime.datetime.now().timetuple()))
			self.f.write(("ID::%s::Timestamp::%s::Length::"+
					"%s::Time::%s\n")
					% (media.key, endTimeTS, media.length,
						timeTS))

		def handle_list_all(self, cmd):
			with self.server.LAR_cond:
				if self.server.LAR is None:
					self.l.warn("Waiting for LIST::ALL "+
							"response cache")
					self.server.LAR_cond.wait()
				LAR_count = self.server.LAR_count
				LAR = self.server.LAR
			self.f.write("TOTAL::%s\n" % LAR_count)
			self.f.write(LAR)

		def handle_login_user(self, cmd):
			key = cmd.strip().split('::', 2)[-1]
			try:
				user = self.server.desk.user_by_key(key)
			except KeyError:
				self.f.write("User doesn't exist\n")
				self.l.warn("User doesn't exist %s" % key)
				return
			if not user.has_access:
				self.f.write("Access denied\n")
				self.l.warn("User hasn't got access %s" % user)
				return
			self.f.write("LOGIN::SUCCESS\n")

		def handle_request_song(self, cmd):
			bits = cmd.strip().split('::')
			if len(bits) != 5:
				self.f.write("Wrong number of arguments\n")
				self.l.warn("Wrong number of arguments %s"
						% repr(cmd))
				return
			songKey = int(bits[2])
			userKey = bits[4]
			try:
				user = self.server.desk.user_by_key(userKey)
			except KeyError:
				self.f.write("User doesn't exist\n")
				self.l.warn("User doesn't exist %s" % userKey)
				return
			try:
				media = self.server.desk.media_by_key(songKey)
			except KeyError:
				self.f.write("Song doens't exist\n")
				self.l.warn("Song doesn't exist %s" % songKey)
				return
			try:
				self.server.desk.request_media(media, user)
			except AlreadyInQueueError:
				self.f.write('ERROR::Track already in queue')
				return
			except Denied, e:
				self.f.write("ERROR::%s" % e)
				return
			self.f.write("REQUEST::SUCCESS")

		def handle_list_pls(self, cmd):
			self.f.write("TOTAL::0\n")

		def handle_request_upload(self, cmd):
			bits = cmd.strip().split('::')
			if len(bits) != 10:
				self.f.write("Wrong number of arguments\n")
				self.l.warn("Wrong number of arguments %s"
						% repr(cmd))
				return
			if (bits[2] != 'ARTIST' or
			    bits[4] != 'TITLE' or
			    bits[6] != 'USER' or
			    bits[8] != 'SIZE'):
				self.f.write("Malformed command\n")
				self.l.warn("Malformed command %s" % repr(cmd))
				return
			artist, title, user, size = bits[3], bits[5], \
						    bits[7], int(bits[9])
			self.f.write("SEND::FILE")
			try:
				mf = self.server.desk.add_media(CappedReadFile(
					self.f, size), user, {'artist': artist,
						              'title': title})
			except Exception, e:
				self.l.exception("Error while desk.add_media")
				self.f.write("ERROR::%s" % e)
				return
			self.f.write("UPLOAD::SUCCESS")	

		def handle(self):
			cmd = self.f.readsome()
			self.l.info("%s %s" % (repr(self.addr), repr(cmd)))
			handler = None
			for key in self.cmd_map:
				if cmd[:len(key)] == key:
					handler = self.cmd_map[cmd[:len(key)]]
					break
			if handler is None:
				l.warn("Unknown command %s" % repr(cmd))
			else:
				handler(cmd)
		
		def interrupt(self):
			self.f.interrupt()
	
	def _on_media_changed(self):
		self.threadPool.execute(self._do_refresh_LAR)

	def _do_refresh_LAR(self):
		self.l.debug("Refreshing LIST::ALL response")
		so = cStringIO.StringIO()
		media_l = sorted(self.desk.list_media(),
				cmp=lambda x,y: (cmp(x.title, y.title) 
						 if not cmp(x.artist, y.artist)
						 else cmp(x.artist, y.artist)))
		for media in media_l:
			so.write("SONG::%s::%s::%s::%s\n" % (
					media.key,
					media.artist,
					media.title,
					0))
		with self.LAR_cond:
			self.LAR = so.getvalue()
			self.LAR_count = len(media_l)
			self.LAR_cond.notifyAll()

	def __init__(self, settings, logger):
		super(ClassicRequestServer, self).__init__(settings, logger)
		self.desk.on_media_changed.register(
				self._on_media_changed)
		self.LAR_cond = threading.Condition()
		self.LAR = None
		self.LAR_count = 0
		self._on_media_changed()
	
	def create_handler(self, con, addr, logger):
		return ClassicRequestServer.Handler(self, con, addr, logger)

class ClassicScreen(Module):
	def __init__(self, settings, logger):
		super(ClassicScreen, self).__init__(settings, logger)
		self.desk.on_playing_changed.register(self._on_playing_changed)
		self.cond = threading.Condition()
		self.running = True
	def _on_playing_changed(self, previous_playing):
		with self.cond:
			self.cond.notify()
	def run(self):
		while True:
			m, r, tmp = self.desk.get_playing()
			by = "Marietje" if r is None else r.by.realName
			artist = "?" if m is None else m.artist
			title = "?" if m is None else m.title
			with open(self.bannerFile, 'w') as f:
				f.write("\scroll %s: %s - %s\n" % (
					by, artist, title))
			with self.cond:
				if not self.running: break
				self.cond.wait()
				if not self.running: break
	def stop(self):
		self.running = False
		with self.cond:
			self.cond.notify()
	

class DummyPlayer(Player):
	def __init__(self, settings, logger):
		super(DummyPlayer, self).__init__(settings, logger)
		self._sleep_socket = socket.socketpair()
	def stop(self):
		self._sleep_socket[0].send('good morning!')
	def play(self, media):
		self.endTime = datetime.datetime.fromtimestamp(
				time.time() + media.length)
		select.select([self._sleep_socket[1]], [], [], media.length)

class ClassicPlayer(Player):
	def play(self, media):
		self.endTime = datetime.datetime.fromtimestamp(
				time.time() + media.length)
		try:
			mf = media.mediaFile
		except KeyError:
			self.l.error("%s's mediafile doesn't exist" % media)
			return
		self.l.info("Playing %s" % media)
		with mf.open() as f:
			pipe = subprocess.Popen(['mpg123', '-q', '-'],
						stdin=f.fileno())
			pipe.wait()
	def stop(self):
		self.l.warn("You'll might have to wait -- we haven't " +
			    "implemented a kill yet")

class ClassicMediaInfo(MediaInfo):
	def get_info_by_path(self, path):
		self.l.info("Info for %s" % path)
		p = subprocess.Popen(['mp3info', '-p', '%a\\n%t\\n%S', path],
				     stdout=subprocess.PIPE)
		artist, title, length = p.stdout.read().split("\n")
		length = int(length)
		return {'artist':	artist,
			'title':	title,
			'length': 	length}

class ClassicMediaStore(MediaStore):
	def __init__(self, settings, logger):
		super(ClassicMediaStore, self).__init__(settings, logger)
		self.register_on_setting_changed('path', self.osc_path)
		self.osc_path()
		self._keys = None
		self.keysCond = threading.Condition()
	
	def osc_path(self):
		self._refresh_keys()
	
	def _refresh_keys(self):
		if not hasattr(self, 'path'):
			return
		self.threadPool.execute(self._do_refresh_keys)
	
	def _do_refresh_keys(self):
		with self.keysCond:
			self._keys = os.listdir(self.path)
			self.l.info("Got %s files" % len(self._keys))
			self.keysCond.notifyAll()

	def create(self, stream):
		(fd, fn) = tempfile.mkstemp()
		f = open(fn, 'w')
		m = hashlib.sha512()
		while True:
			b = stream.read(2048)
			m.update(b)
			if len(b) == 0:
				break
			f.write(b)
		f.close()
		hd = m.hexdigest()
		path = os.path.join(self.path, hd)
		if os.path.exists(path):
			self.l.warn("Duplicate file %s" % hd)
		else:
			os.rename(fn, path)
			os.chmod(path, 0644)
			with self.keysCond:
				if self._keys is None:
					self.l.debug(
						"create: waiting on keysCond")
					self.keysCond.wait()
				self._keys.append(hd)
		return self.by_key(hd)

	def by_key(self, key):
		with self.keysCond:
			if self._keys is None:
				self.l.debug("by_key: waiting on keysCond")
				self.keysCond.wait()
			if not key in self._keys:
				raise KeyError, key
		p = os.path.join(self.path, key)
		assert os.path.exists(p)
		return ClassicMediaFile(self, p, key)

	def remove(self, mediaFile):
		self.l.info("Removing %s" % mediaFile)
		os.unlink(mediaFile.path)
	
	@property
	def keys(self):
		with self.keysCond:
			if self._keys is None:
				self.l.debug("keys: waiting on keysCond")
				self.keysCond.wait()
			return tuple(self._keys)

class ClassicCollection(Collection):
	def __init__(self, settings, logger):
		super(ClassicCollection, self).__init__(settings, logger)
		self._media = None
		# notice on locking;
		#  we assume self._media won't turn into None and that wrong
		#  reads aren't that bad.  We only lock concurrent writes.
		self.lock = threading.Lock()
		self.register_on_setting_changed('db', self.osc_db)
		self.osc_db()
	
	def osc_db(self):
		self.db.on_changed.register(self.on_db_changed)
		self.on_db_changed()
	
	def on_db_changed(self):
		if not self.db.ready:
			return
		with self.lock:
			self._media = {}
			for tmp in self.db.list_media():
				self._media[tmp[0]] = ClassicMedia(self, *tmp)
			self.users = {}
			for tmp in self.db.list_users():
				self.users[tmp[0]] = ClassicUser(self, *tmp)
		self.l.info("Cached %s tracks, %s users" % (len(self._media),
							    len(self.users)))
		self.on_keys_changed()
		self.on_changed()
		with self.lock:
			if len(self._media) > 0:
				self.got_media_event.set()
			else:
				self.got_media_event.clear()
	
	@property
	def media(self):
		if self._media is None:
			return list()
		return self._media.itervalues()

	@property
	def media_keys(self):
		if self._media is None:
			return list()
		return self._media.keys()

	def by_key(self, key):
		return self._media[int(key)]

	def _user_by_key(self, key):
		return self.users[key]

	def stop(self):
		self.got_media_event.set()

	def add(self, mediaFile, user, extraInfo=None):
		info = mediaFile.get_info()
		if not info is None:
			info.update(extraInfo)
		tmp = (info['artist'],
		       info['title'],
		       info['length'],
		       mediaFile.key,
		       user,
		       int(time.time()),
		       info.get('track_gain', None),
		       info.get('track_peak', None))
		key = self.db.add_media(*tmp)
		with self.lock:
			self._media[key] = ClassicMedia(self, key, *tmp)
			if len(self._media) == 1:
				self.got_media_event.set()
		self.on_keys_changed()
		self.on_changed()
	
	def _unlink_media(self, media):
		with self.lock:
			del(self._media[media.key])
		self.db.remove_media(media.key)
		self.on_keys_changed()
		self.on_changed()

	def _save_media(self, media):
		self.db.update_media(media.key,
				     media.artist,
				     media.title,
				     media.length,
				     media.mediaFile.key,
				     media.uploadedByKey,
				     media.uploadedTimestamp,
				     media.trackGain,
				     media.trackPeak)
		self.on_changed()
	
class ClassicRandom(Random):
	def __init__(self, settings, logger):
		super(ClassicRandom, self).__init__(settings, logger)
		self.collection.on_keys_changed.register(
				self.on_collection_keys_changed)
		self.on_collection_keys_changed()
	
	def on_collection_keys_changed(self):
		self.keys = self.collection.media_keys
		if len(self.keys) > 0:
			self.on_ready()

	def pick(self):
		if len(self.keys) == 0:
			return None
		key = self.keys[random.randint(0, len(self.keys) - 1)]
		return self.collection.by_key(key)

	def _handle_history_record(self, pr):
		pass
	def _handle_history_pretty_changed(self):
		pass
	@property
	def ready(self):
		return len(self.keys) > 0

class ClassicOrchestrator(Orchestrator):
	pass

class ClassicDb(Module):
	class CursorWrapper(object):
		def __init__(self, cdb):
			self.__dict__['__cursor'] = cdb.conn.cursor()
			self.__dict__['__cdb'] = cdb
		def __getattr__(self, k):
			return getattr(self.__dict__['__cursor'], k)
		def __setattr__(self, k, v):
			setattr(self.__dict__['__cursor'], k, v)
		def __delattr__(self, k):
			delattr(self.__dict__['__cursor'], k)
		def execute(self, *args, **kwargs):
			try:
				return self.__dict__['__cursor'].execute(*args, **kwargs)
			except MySQLdb.OperationalError, e:
				if e.args[0] != 2006:
					raise e
			self.__dict__['__cdb']._reconnect()
			self.__dict__['__cursor'] = self.__dict__['__cdb'].conn.cursor() 
			return self.__dict__['__cursor'].execute(*args, **kwargs)
	
	def __init__(self, settings, logger):
		super(ClassicDb, self).__init__(settings, logger)
		self.local = threading.local()
		self.on_changed = Event()
		self.creds_ok = False
		self.connections = list()
		for key in ('username', 'host', 'password', 'database'):
			if not key in settings:
				setattr(self, key, None)
			self.register_on_setting_changed(key, self.osc_creds)
		self.osc_creds()
	
	def test_credentials(self):
		try:
			with MySQLdb.connect(**self.credentials) as testConn:
				pass
		except MySQLdb.MySQLError:
			self.l.exception('New credentials failed')
			return False
		return True

	def osc_creds(self):
		self.credentials = {'host': self.host,
				    'user': self.username,
				    'passwd': self.password,
				    'db': self.database}
		if (any(map(lambda x: x is None,
				self.credentials.values())) or
				not self.test_credentials()):
			self.creds_ok = False
			return
		self.l.info("Credentials are OK!")
		self.creds_ok = True
		self.on_changed()

	def create_conn(self):
		if not self.creds_ok:
			raise ValueError, "Credentials aren't ok"
		conn = MySQLdb.connect(**self.credentials)
		conn.autocommit(True)
		self.connections.append(weakref.ref(conn,
			lambda x: self.connections.remove(x)))
		return conn

	@property
	def conn(self):
		if not hasattr(self.local, 'conn'):
			self.local.conn = self.create_conn()
			self.l.debug("Created new DB connection")
		return self.local.conn

	def _reconnect(self):
		self.local.conn = self.create_conn()
		self.l.debug("Reconnected DB connection")

	@property
	def ready(self):
		return self.creds_ok

	def cursor(self):
		return ClassicDb.CursorWrapper(self)
	
	def media_keys(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT trackId
			FROM tracks;""")
		ret = map(lambda x: x[0], c.fetchall())
		if not cursor is None: cursor.close()
		return ret

	def queue_shift(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT requestId,
			       trackId,
			       requestedBy
			FROM queue
			WHERE played=0
			ORDER BY requestId
			LIMIT 0, 1;""")
		tmp = c.fetchone()
		if tmp is None:
			return None
		requestId, trackId, byKey = tmp
		c.execute("""
			UPDATE queue
			SET played=1
			WHERE requestid=%s; """, requestId)
		ret = (requestId, trackId, byKey)
		if not cursor is None: cursor.close()
		return ret

	def queue_get_requests(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT requestId,
			       trackId,
			       requestedBy
			FROM queue
			WHERE played=0
			ORDER BY requestId; """)
		for requestId, trackId, requestedBy in c.fetchall():
			yield requestId, trackId, requestedBy
		if not cursor is None: cursor.close()
	
	def queue_request(self, media, user, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			INSERT INTO `queue` (
				`TrackID`,
				`RequestedBy`,
				`Played`)
			VALUES (
				%s,
				%s,
				%s); """,
			(media, user, 0))
		if not cursor is None: cursor.close()

	def list_users(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT username, fullName, level, password
			FROM users; """)
		for username, fullName, level, passwordHash in c.fetchall():
			yield username, fullName, level, passwordHash
		if not cursor is None: cursor.close()

	def list_media(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT trackId, artist, title, length, fileName,
			       uploadedBy, uploadedTimestamp, trackGain,
			       trackPeak
			FROM tracks
			WHERE deleted=0; """)
		for (trackId, artist, title, length, fileName, uploadedBy,
				uploadedTimestamp, trackGain, trackPeak) \
					in c.fetchall():
			yield (trackId, artist, title, length, fileName,
			       uploadedBy, uploadedTimestamp, trackGain,
			       trackPeak)
		if not cursor is None: cursor.close()
	
	def remove_media(self, trackId, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			DELETE FROM tracks
			WHERE trackId=%s """,
			(trackId,))
		if not cursor is None: cursor.close()

	def add_media(self, artist, title, length, fileName, uploadedBy,
			uploadedTimestamp, trackGain, trackPeak, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			INSERT INTO tracks (
				artist,
				title,
				length,
				fileName,
				uploadedBy,
				uploadedTimestamp,
				trackGain,
				trackPeak
			) VALUES (
				%s, %s, %s, %s, %s, %s, %s, %s
			); """,
			(artist,
			 title,
			 length,
			 fileName,
			 uploadedBy,
			 uploadedTimestamp,
			 trackGain,
			 trackPeak))
		if not cursor is None: cursor.close()
		c = self.cursor() if cursor is None else cursor
		c.execute(""" SELECT trackId
			      FROM tracks
			      WHERE fileName=%s """, fileName)
		ret = c.fetchone()[0]
		if not cursor is None: cursor.close()
		return ret

	def update_media(self, trackId, artist, title, length, fileName,
			 uploadedBy, uploadedTimestamp, trackGain, trackPeak,
			 cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			UPDATE tracks
			SET artist=%s,
			    title=%s,
			    length=%s,
			    fileName=%s,
			    uploadedBy=%s,
			    uploadedTimestamp=%s,
			    trackGain=%s,
			    trackPeak=%s
			WHERE trackId=%s; """,
			(artist,
			 title,
			 length,
			 fileName,
			 uploadedBy,
			 uploadedTimestamp,
			 trackGain,
			 trackPeak,
			 trackId))
		if not cursor is None: cursor.close()

	def history_remove_request(self, logId, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			DELETE FROM `log`
			WHERE LogID=%s """,
			(logId,))
		if not cursor is None: cursor.close()

	def history_record(self, byKey, trackId, timeStamp, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			INSERT INTO `log` (
				`Type`,
				`Username`,
				`TrackID`,
				`Timestamp`)
			VALUES (
				'request',
				%s,
				%s,
				%s); """,
			(byKey, trackId, timeStamp))
		if not cursor is None: cursor.close()
	
	def history_list(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT LogID, username, trackid, timestamp
			FROM log;
			""")
		while True:
			rrs = c.fetchmany(128)
			if not rrs:
				break
			for rr in rrs:
				logId, username, trackId, timeStamp = rr
				if username == 'Marietje':
					username = None
				yield logId, timeStamp, username, trackId
		if not cursor is None: cursor.close()
