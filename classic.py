from __future__ import with_statement

from maried.core import *

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
		raise NotImplemented
	def move(self, request, amount):
		raise NotImplemented
	
class ClassicRequest(Request):
	def __init__(self, key, *args, **kwargs):
		super(self.__class__, self).__init__(args, **kwargs)
		self.key = key

class MediaRequest(Request):
	def __init__(self, key, *args, **kwargs):
		super(self.__class__, self).__init__(*args, **kwargs)
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
	pass

class ClassicRandom(Random):
	pass

class ClassicOrchestrator(Orchestrator):
	def run(self):
		pass

class ClassicDb(Module):
	def __init__(self, settings, logger):
		super(self.__class__, self).__init__(settings, logger)
		#with  MySQLdb.connect(self.con_params) as testconn:
		#	pass
		self.local = threading.local

	# TODO: consider abstracting this.
	def create_conn(self):
		return MySQLdb.connect(self.con_params)

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
			meth(cursor *args, **kwargs)

	def queue_shift(self, cursor):
		cursor.execute("""
			SELECT MIN(notplayed.requestid) 
			FROM (
				SELECT requestid 
				FROM queue 
				WHERE played=0);""")
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
		raise NotImplemented

	def user(self, cursor, key):
		raise NotImplemented
	
	def queue_request(self, cursor, media, user):
		raise NotImplemented

	
