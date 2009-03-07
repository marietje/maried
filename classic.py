from __future__ import with_statement

from maried.core import *

import random
import MySQLdb
import threading

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
		self.classicDb.queue_request(media, user)
	def get_requests(self, media, user):
		return self.classicDb.queue_get_requests()
	def shift(self):
		head = self.classicDb.queue_shift()
	def cancel(self, request):
		raise NotImplementedError
	def move(self, request, amount):
		raise NotImplementedError
	
class ClassicRequest(Request):
	def __init__(self, key, *args, **kwargs):
		super(ClassicRequest, self).__init__(args, **kwargs)
		self.key = key

class MediaRequest(Request):
	def __init__(self, key, *args, **kwargs):
		super(MediaRequest, self).__init__(*args, **kwargs)
		self.key = key

class ClassicHistory(Module):
	pass

class ClassicDesk(Desk):
	pass

class ClassicRequestServer(Module):
	def run(self):
		pass

class ClassicScreen(Module):
	def run(self):
		pass

class ClassicPlayer(Module):
	pass

class ClassicMediaInfo(MediaInfo):
	pass

class ClassicMediaStore(MediaStore):
	pass

class ClassicCollection(Collection):
	def __init__(self, settings, logger):
		super(ClassicCollection, self).__init__(settings, logger)
		self.register_on_setting_changed('db', self.osc_db)
	
	def osc_db(self):
		self.db.on_changed.register(self.on_db_changed)
	
	def oc_db_changed(self):
		self.on_keys_changed()
	
	def list_media(self):
		return []

class ClassicRandom(Random):
	def __init__(self, settings, logger):
		super(ClassicRandom, self).__init__(settings, logger)
		self.collection.on_keys_changed.register(
				self.on_collection_keys_changed)
		self.keys = list()
		self.on_collection_keys_changed()
	
	def on_collection_keys_changed(self):
		self.keys = map(lambda x: x.get_key(),
				self.collection.list_media())

	def pick(self):
		key = self.keys[random.randint(0, len(self.keys) - 1)]
		return self.collection.by_key(key)

class ClassicOrchestrator(Orchestrator):
	def run(self):
		pass

class ClassicDb(Module):
	def __init__(self, settings, logger):
		super(ClassicDb, self).__init__(settings, logger)
		#with  MySQLdb.connect(self.con_params) as testconn:
		#	pass
		self.local = threading.local
		self.on_change = Event()
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
		self.on_change()

	def create_conn(self):
		conn = MySQLdb.connect(**self.credentials)
		self.connections.append(conn)
		return conn

	def get_conn(self):
		try:
			self.local.conn
		except AttributeError:
			self.local.conn = self.create_conn(self)
		return self.local.conn

	conn = property(get_conn)
	

	def cursor(self):
		return self.conn.cursor()
	
	def with_cursor(self, meth, *args, **kwargs):
		with self.cursor() as cursor:
			meth(cursor=cursor, *args, **kwargs)
	
	def track_keys(self, cursor):
		cursor.execute("""
			SELECT trackId
			FROM tracks;""")
		return map(lambda x: x[0], cursor.fetchall())

	def queue_shift(self, cursor):
		cursor.execute("""
			SELECT requestId
			FROM queue
			WHERE played=0
			ORDER BY requestId
			LIMIT 0, 1;""")
		rid, = cursor.fetchone()

		cursor.execute("""
			UPDATE queue
			SET played=1
			WHERE requestid=%s;""", rid)
		return self.request(rid, cursor)
	
	def request(self, cursor, key):
		cursor.execute("""
			SELECT trackid, requestedby
			FROM queue
			WHERE played=1 AND requestid=%s;""", key)
		tid, rbid = cursor.fetchone()
		return ClassicRequest(key, self.media(cursor, tid), 
				self.user(cursor, rbid))

	def media(self, cursor, key):
		raise NotImplementedError

	def user(self, cursor, key):
		raise NotImplementedError
	
	def queue_request(self, cursor, media, user):
		raise NotImplementedError

	
