from __future__ import with_statement

from maried.core import *

import time
import socket
import random
import select
import os.path
import MySQLdb
import threading
import subprocess

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

class ClassicMediaFile(MediaFile):
	def __init__(self, store, path, key):
		self.store = store
		self.path = path
		self.key = key
	
	def open(self):
		return open(self.path)
	
	def get_key(self):
		return self.key

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

class ClassicUser(User):
	def __init__(self, coll, key, realName, level):
		super(ClassicUser, self).__init__(key, realName)
		self.level = level
		self.coll = coll

class ClassicUsers(Users):
	def assert_request(self, user, media):
		reqs = self.queue.get_requests()
		# blaat die blaat
		return True
	def assert_addition(self, user, mediaFile):
		# blaat die blaat
		return True
	def assert_cancel(self, user, request):
		return True
	def assert_move(self, user, request, amount):
		return True

class ClassicQueue(Queue):
	def request(self, media, user):
		self.db.queue_request(media, user)
	def get_requests(self, media, user):
		return self.db.queue_get_requests()
	def shift(self):
		if not self.db.ready:
			raise EmptyQueueException
		tmp = self.db.queue_shift()
		if tmp is None:
			raise EmptyQueueException
		return ClassicQueue(queue, *tmp)
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
	def run(self):
		pass
	def stop(self):
		pass

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
		select.select([self._sleep_socket[1]], [], [], media.length)

class ClassicPlayer(Module):
	def play(self, media):
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
		self.on_keys_changed()
		with self.lock:
			if len(self.media) > 0:
				self.got_media_event.set()
			else:
				self.got_media_event.clear()
	
	def list_media(self):
		if self.media is None:
			return list()
		return media.itervalues()

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
			WHERE requestid=%s;""", requestId)
		ret = (requestId, trackId, byKey)
		if not cursor is None: cursor.close()
		return ret

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
