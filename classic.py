from __future__ import with_statement

from maried.core import *
from maried.io import IntSocketFile

import time
import socket
import select
import random
import select
import os.path
import MySQLdb
import logging
import datetime
import threading
import subprocess

class AlreadyInQueueError(Denied):
	pass
class MaxQueueLengthExceededError(Denied):
	pass
class MaxQueueCountExceededError(Denied):
	pass

class ClassicMedia(Media):
	def __init__(self, coll, key, artist, title, length, mediaFileKey,
			   uploadedByKey, uploadedTimestamp):
		self.coll = coll
		self.key = key
		self.artist = artist
		self.title = title
		self.length = length
		self.mediaFileKey = mediaFileKey
		self.uploadedByKey = uploadedByKey
		self.uploadedTimestamp = uploadedTimestamp
	
	@property
	def uploadedBy(self):
		return self.coll._user_by_key(self.uploadedByKey)
	@property
	def mediaFile(self):
		return self.coll.mediaStore.by_key(self.mediaFileKey)
	
	def get_key(self):
		return self.key

	def __repr__(self):
		return "<ClassicMedia %s - %s>" % (self.artist, self.title)

	def __eq__(self, other):
		return self.key == other.key
	def __ne__(self, other):
		return self.key != other.key

class ClassicMediaFile(MediaFile):
	def __init__(self, store, path, key):
		self.store = store
		self.path = path
		self.key = key
	
	def open(self):
		return open(self.path)
	
	def get_key(self):
		return self.key

	def __repr__(self):
		return "<ClassicMediaFile %s>" % self.key
	def __eq__(self, other):
		return self.key == other.key
	def __ne__(self, other):
		return self.key != other.key

class ClassicRequest(Request):
	def __init__(self, queue, key, mediaKey, byKey):
		self.queue = queue
		self.key = key
		self.mediaKey = mediaKey
		self.byKey = byKey
	
	@property
	def media(self):
		return self.queue.collection.by_key(self.mediaKey)

	@property
	def by(self):
		return self.queue.collection._user_by_key(self.byKey)

	def __repr__(self):
		return "<ClassicRequest %s - %s>" % (self.byKey,
						     repr(self.media))

class ClassicUser(User):
	def __init__(self, coll, key, realName, level):
		super(ClassicUser, self).__init__(key, realName)
		self.level = level
		self.coll = coll
	def get_key(self):
		return self.key
	def __repr__(self):
		return "<ClassicUser %s %s>" % (self.key,
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

class ClassicUsers(Users):
	def assert_request(self, user, media):
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
		self.db.queue_request(media.get_key(), user.get_key())
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

class ClassicHistory(Module):
	def record(self, media, request):
		self.l.info("%s: %s" % (media, request))

class ClassicDesk(Desk):
	pass

class ClassicRequestServer(Module):
	def _handle_list_queue(self, conn, addr, l, f, cmd):
		queue = self.desk.list_requests()
		endTime = self.desk.get_playing()[2]
		timeLeft = int(time.mktime(endTime.timetuple()) -
			time.mktime(datetime.datetime.now().timetuple()))
		f.write("TOTAL::%s::TIMELEFT::%s\n" % (len(queue),
						       int(timeLeft)))
		for req in queue:
			media = req.media
			f.write("SONG::%s::%s::%s::%s\n" % (media.artist,
							    media.title,
							    media.length,
							    req.by.get_key()))

	def _handle_nowplaying(self, conn, addr, l, f, cmd):
		media, request, endTime = self.desk.get_playing()
		endTimeTS = int(time.mktime(endTime.timetuple()) - media.length)
		timeTS = int(time.mktime(datetime.datetime.now().timetuple()))
		f.write( "ID::%s::Timestamp::%s::Length::%s::Time::%s\n" % (
				media.get_key(),
				endTimeTS,
				media.length,
				timeTS))

	def _handle_list_all(self, conn, addr, l, f, cmd):
		media_l = list(self.desk.list_media())
		f.write("TOTAL::%s\n" % len(media_l))
		for media in media_l:
			f.write("SONG::%s::%s::%s::%s\n" % (
					media.get_key(),
					media.artist,
					media.title,
					0))

	def _handle_login_user(self, conn, addr, l, f, cmd):
		key = cmd.strip().split('::', 2)[-1]
		try:
			user = self.desk.user_by_key(key)
		except KeyError:
			f.write("User doesn't exist\n")
			self.l.warn("User doesn't exist %s" % key)
			return
		if not user.has_access:
			f.write("Access denied\n")
			self.l.warn("User hasn't got access %s" % user)
			return
		f.write("LOGIN::SUCCESS\n")

	def _handle_request_song(self, conn, addr, l, f, cmd):
		bits = cmd.strip().split('::')
		if len(bits) != 5:
			f.write("Wrong number of arguments\n")
			self.l.warn("Wrong number of arguments %s" % repr(cmd))
			return
		songKey = int(bits[2])
		userKey = bits[4]
		try:
			user = self.desk.user_by_key(userKey)
		except KeyError:
			f.write("User doesn't exist\n")
			self.l.warn("User doesn't exist %s" % userKey)
			return
		try:
			media = self.desk.media_by_key(songKey)
		except KeyError:
			f.write("Song doens't exist\n")
			self.l.warn("Song doesn't exist %s" % songKey)
			return
		try:
			self.desk.request_media(media, user)
		except AlreadyInQueueError:
			f.write('ERROR::Track already in queue')
			return
		except Denied, e:
			f.write("ERROR::%s" % e)
			return
		f.write("REQUEST::SUCCESS")

	def _dispatch_request(self, conn, addr, n):
		try:
			l = logging.getLogger("%s.%s" % (self.l.name, n))
			f = IntSocketFile(conn)
			with self.lock:
				self.connections.add(f)
			cmd = f.readsome()
			l.info("%s %s" % (repr(addr), repr(cmd)))
			handler = None
			for key in self.cmd_map:
				if cmd[:len(key)] == key:
					handler = self.cmd_map[cmd[:len(key)]]
					break
			if handler is None:
				l.warn("Unknown command %s" % repr(cmd))
			else:
				handler(conn, addr, l, f, cmd)
		finally:
			with self.lock:
				self.connections.remove(f)
			conn.close()

	def __init__(self, settings, logger):
		super(ClassicRequestServer, self).__init__(settings, logger)
		self.running = False
		self.connections = set()
		self.lock = threading.Lock()
		self._sleep_socket_pair = socket.socketpair()
		self.n_conn = 0
		self.cmd_map = {'LIST::QUEUE\n': self._handle_list_queue,
				'LIST::NOWPLAYING\n': self._handle_nowplaying,
				'LIST::ALL': self._handle_list_all,
				'REQUEST::SONG::': self._handle_request_song,
				'LOGIN::USER::': self._handle_login_user}
		
	def _inner_run(self):
		rlist, wlist, xlist = select.select(
				[self._sleep_socket_pair[1],
				 self.socket], [],
				[self.socket])
		if self._sleep_socket_pair[1] in rlist:
			return True
		if self.socket in xlist:
			raise IOError, "Accept socket in select clist"
		if not self.socket in rlist:
			return False
		conn, addr = self.socket.accept()
		self.n_conn += 1
		t = threading.Thread(target=self._dispatch_request,
				     args=(conn, addr, self.n_conn))
		t.start()
		return False

	def run(self):
		self.running = True
		try:
			s = self.socket = socket.socket(socket.AF_INET,
							socket.SOCK_STREAM)
			s.bind((self.host, self.port))
			s.listen(3)
			self.l.info("Listening on %s:%s" % (self.host, self.port))
			while self.running:
				if self._inner_run():
					break
		finally:
			self.socket.close()
			
	def stop(self):
		self.running = False
		self._sleep_socket_pair[0].send('Good morning!')
		with self.lock:
			conns = set(self.connections)
		for conn in conns:
			conn.interrupt()

class ClassicScreen(Module):
	def run(self):
		pass
	def stop(self):
		pass

class DummyPlayer(Module):
	def __init__(self, settings, logger):
		super(DummyPlayer, self).__init__(settings, logger)
		self._sleep_socket = socket.socketpair()
	def stop(self):
		self._sleep_socket[0].send('good morning!')
	def play(self, media):
		self.endTime = datetime.datetime.fromtimestamp(
				time.time() + media.length)
		select.select([self._sleep_socket[1]], [], [], media.length)

class ClassicPlayer(Module):
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
			pipe = subprocess.Popen(['mpg123', '-'],
						stdin=f.fileno())
			pipe.wait()
	def stop(self):
		self.l.warn("You'll might have to wait -- we haven't " +
			    "implemented a kill yet")

class ClassicMediaInfo(MediaInfo):
	pass

class ClassicMediaStore(MediaStore):
	def by_key(self, key):
		p = os.path.join(self.path, key)
		if not os.path.exists(p):
			raise KeyError, key
		return ClassicMediaFile(self, p, key)

class ClassicCollection(Collection):
	def __init__(self, settings, logger):
		super(ClassicCollection, self).__init__(settings, logger)
		self.media = None
		# notice on locking;
		#  we assume self.media won't turn into None and that wrong
		#  reads aren't that bad.  We only lock concurrent writes.
		self.lock = threading.Lock()
		self.register_on_setting_changed('db', self.osc_db)
		self.osc_db()
	
	def osc_db(self):
		self.db.on_changed.register(self.on_db_changed)
	
	def on_db_changed(self):
		if not self.db.ready:
			return
		with self.lock:
			self.media = {}
			for tmp in self.db.list_media():
				self.media[tmp[0]] = ClassicMedia(self, *tmp)
			self.users = {}
			for tmp in self.db.list_users():
				self.users[tmp[0]] = ClassicUser(self, *tmp)
		self.l.info("Cached %s tracks, %s users" % (len(self.media),
							    len(self.users)))
		self.on_keys_changed()
		with self.lock:
			if len(self.media) > 0:
				self.got_media_event.set()
			else:
				self.got_media_event.clear()
	
	def list_media(self):
		if self.media is None:
			return list()
		return self.media.itervalues()

	def media_keys(self):
		if self.media is None:
			return list()
		return self.media.keys()

	def by_key(self, key):
		return self.media[key]

	def _user_by_key(self, key):
		return self.users[key]

	def stop(self):
		self.got_media_event.set()

class ClassicRandom(Random):
	def __init__(self, settings, logger):
		super(ClassicRandom, self).__init__(settings, logger)
		self.collection.on_keys_changed.register(
				self.on_collection_keys_changed)
		self.keys = list()
		self.on_collection_keys_changed()
	
	def on_collection_keys_changed(self):
		self.keys = self.collection.media_keys()

	def pick(self):
		if len(self.keys) == 0:
			return None
		key = self.keys[random.randint(0, len(self.keys) - 1)]
		return self.collection.by_key(key)

class ClassicOrchestrator(Orchestrator):
	pass

class ClassicDb(Module):
	def __init__(self, settings, logger):
		super(ClassicDb, self).__init__(settings, logger)
		self.local = threading.local()
		self.on_changed = Event()
		self.connections = list()
		self.creds_ok = False
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
		self.connections.append(conn)
		return conn

	@property
	def conn(self):
		try:
			self.local.conn
		except AttributeError:
			self.local.conn = self.create_conn()
		return self.local.conn

	@property
	def ready(self):
		return self.creds_ok

	def cursor(self):
		return self.conn.cursor()
	
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
			WHERE requestid=%s; commit; """, requestId)
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
				%s); commit; """,
			(media, user, 0))
		if not cursor is None: cursor.close()

	def list_users(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT username, fullName, level
			FROM users; """)
		for username, fullName, level in c.fetchall():
			yield username, fullName, level
		if not cursor is None: cursor.close()

	def list_media(self, cursor=None):
		c = self.cursor() if cursor is None else cursor
		c.execute("""
			SELECT trackId, artist, title, length, fileName,
			       uploadedBy, uploadedTimestamp
			FROM tracks
			WHERE deleted=0; """)
		for (trackId, artist, title, length, fileName, uploadedBy,
				uploadedTimestamp) in c.fetchall():
			yield (trackId, artist, title, length, fileName,
			       uploadedBy, uploadedTimestamp)
		if not cursor is None: cursor.close()
