from __future__ import with_statement

from maried.core import MediaStore, MediaFile, Media, Collection, User, Users, \
			History, Request, PastRequest, Denied, \
                        AlreadyInQueueError, MaxQueueLengthExceededError
from mirte.core import Module
from sarah.event import Event
from sarah.dictlike import AliasingMixin

from pymongo.objectid import ObjectId

import threading
import tempfile
import pymongo
import hashlib
import gridfs
import time
import os

class MongoMediaFile(MediaFile):
	def open(self):
		return self.store._open(self.key)
	def get_named_file(self):
		return self.store._get_named_file(self.key)
	def __repr__(self):
		return "<MongoMediaFile %s>" % self._key

class MongoUser(AliasingMixin, User):
	aliases = {'key': '_id',
		   'realName': 'n',
		   'level': 'l',
		   'passwordHash': 'p'}
	def __init__(self, coll, data):
		super(MongoUser, self).__init__(self.normalize_dict(data))
		self.collection = coll
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

class MongoMedia(AliasingMixin, Media):
	aliases = {'key': '_id',
		   'artist': 'a',
		   'title': 't',
		   'trackGain': 'tg',
		   'trackPeak': 'tp',
		   'length': 'l',
		   'mediaFileKey': 'k',
		   'uploadedByKey': 'ub',
		   'uploadedTimestamp': 'ut'}
	def __init__(self, coll, data):
		super(MongoMedia, self).__init__(coll,
                                self.normalize_dict(data))

class MongoPastRequest(AliasingMixin, PastRequest):
	aliases = {'key': '_id',
		   'byKey': 'b',
		   'at': 'a',
		   'mediaKey': 'm'}
	def __init__(self, history, data):
		super(MongoPastRequest, self).__init__(history,
                                self.normalize_dict(data))

class MongoRequest(AliasingMixin, PastRequest):
	aliases = {'key': '_id',
		   'byKey': 'b',
		   'mediaKey': 'm'}
	def __init__(self, queue, data):
		super(MongoRequest, self).__init__(queue,
                                self.normalize_dict(data))


class MongoDb(Module):
	def __init__(self, *args, **kwargs):
		super(MongoDb, self).__init__(*args, **kwargs)
		self.register_on_setting_changed('host', self.osc_creds)
		self.register_on_setting_changed('port', self.osc_creds)
		self.register_on_setting_changed('db', self.osc_creds)
		self.on_changed = Event()
		self.ready = None
		self.osc_creds()

	def osc_creds(self):
		if None in (self.host, self.port, self.db):
			return
		self.con = pymongo.Connection(self.host, self.port)
		self.db = self.con[self.db]
		self.ready = True
		self.on_changed()

class MongoCollection(Collection):
	def __init__(self, *args, **kwargs):
		super(MongoCollection, self).__init__(*args, **kwargs)
		self._media = None
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
			self.cMedia = self.db.db['media']
			self.cUsers = self.db.db['users']
			self._media = {}
			self.users = {}
			for tmp in self.cMedia.find():
				self._media[tmp['_id']] = MongoMedia(self, tmp)
			for tmp in self.cUsers.find():
				self.users[tmp['_id']] = MongoUser(self, tmp)
		self.l.info("Cached %s media %s users" % (len(self._media),
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

        @property
        def media_count(self):
                return len(self._media)

	def by_key(self, key):
                if isinstance(key, basestring):
                        key = ObjectId(key)
		return self._media[key]

	def _user_by_key(self, key):
		return self.users[key]

	def stop(self):
		self.got_media_event.set()

	def add(self, mediaFile, user, extraInfo=None):
		info = mediaFile.get_info()
		if not extraInfo is None:
			info.update(extraInfo)
		info.update({
			'mediaFileKey': mediaFile.key,
			'uploadedTimestamp': time.time(),
			'uploadedByKey': user.key})
		key = self.cMedia.insert(MongoMedia.normalize_dict(info))
		info['_id'] = key
		with self.lock:
			self._media[key] = MongoMedia(self, info)
			if len(self._media) == 1:
				self.got_media_event.set()
		self.on_keys_changed()
		self.on_changed()
	
	def _unlink_media(self, media):
		with self.lock:
			del(self._media[media.key])
		self.cMedia.remove({'_id': media.key})
		self.db.on_keys_changed()
		self.on_changed()
	
	def _save_media(self, media):
		self.db.save(media.to_dict())
		self.on_changed()

class MongoMediaStore(MediaStore):
	def __init__(self, *args, **kwargs):
		super(MongoMediaStore, self).__init__(*args, **kwargs)
		self.ready = False
		self.keysCond = threading.Condition()
		self._keys = None
		self.register_on_setting_changed('db', self.osc_db)
                self.osc_db()

	def osc_db(self):
		self.db.on_changed.register(self.on_db_changed)
		self.on_db_changed()

	def on_db_changed(self):
		if not self.db.ready:
			return
		self.fs = gridfs.GridFS(self.db.db, self.collection)
		self.threadPool.execute_named(self._do_refresh_keys,
				'%s _do_refresh_keys' % self.l.name)
	
	def _do_refresh_keys(self):
		with self.keysCond:
			self._keys = self.fs.list()
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
                stream.close()
		f.close()
		hd = m.hexdigest()
		if not self._keys is None and hd in self._keys:
			self.l.warn("Duplicate file %s" % hd)
		else:
			with open(fn) as f:
				self.fs.put(f, filename=hd)
			os.unlink(fn)
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
		return MongoMediaFile(self, key)

	def remove(self, mediaFile):
		self.l.info("Removing %s" % mediaFile)
		self.fs.delete(mediaFile.key)
	
	def _open(self, key):
		return self.fs.get_last_version(key)

	def _get_named_file(self, key):
		raise NotImplementedError
	
	@property
	def keys(self):
		with self.keysCond:
			if self._keys is None:
				self.l.debug("keys: waiting on keysCond")
				self.keysCond.wait()
			return tuple(self._keys)

class MongoHistory(History):
	def __init__(self, *args, **kwargs):
		super(MongoHistory, self).__init__(*args, **kwargs)
		self.register_on_setting_changed('db', self.osc_db)
		self.osc_db()

        def osc_db(self):
		self.db.on_changed.register(self._on_db_changed)
                self._on_db_changed()
	
	def _on_db_changed(self):
		if not self.db.ready:
			return
		self.cHistory = self.db.db['history']
		self.on_pretty_changed()
	
	def record(self, media, request, at):
		self.l.info(repr(media if request is None else request))
		info = {'mediaKey': media.key,
			'byKey': None if request is None else request.by.key,
			'at':  time.mktime(at.timetuple())}
		info['key'] = self.cHistory.insert(
                                MongoPastRequest.normalize_dict(info))
		self.on_record(MongoPastRequest(self, info))
	
	def list_past_requests(self):
		for tmp in self.cHistory.find():
			yield MongoPastRequest(self, tmp)

class MongoUsers(Users):
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
