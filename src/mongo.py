from __future__ import with_statement

from maried.core import MediaStore, MediaFile
from mirte.core import Module
from sarah.event import Event

import threading
import tempfile
import pymongo
import hashlib
import gridfs
import os

class MongoMediaFile(MediaFile):
	def open(self):
		return self.store._open(self.key)
	def get_named_file(self):
		return self.store._get_named_file(self.key)
	def __repr__(self):
		return "<MongoMediaFile %s>" % self._key

class MongoDb(Module):
	def __init__(self, settings, logger):
		super(MongoDb, self).__init__(settings, logger)
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
		self._db = self.con[self.db]
		self.ready = True
		self.on_changed()

class MongoMediaStore(MediaStore):
	def __init__(self, settings, logger):
		super(MongoMediaStore, self).__init__(settings, logger)
		self.db.on_changed.register(self.on_db_changed)
		self.ready = False
		self.keysCond = threading.Condition()
		self._keys = None
		self.on_db_changed()

	def on_db_changed(self):
		if not self.db.ready:
			return
		self.fs = gridfs.GridFS(self.db._db, self.collection)
		self.threadPool.execute(self._do_refresh_keys)
	
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

