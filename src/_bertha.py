from __future__ import with_statement

from bertha import BerthaClient

from maried.core import MediaStore, MediaFile
from mirte.core import Module
from sarah.event import Event

import threading

class BerthaMediaFile(MediaFile):
	def open(self):
		return self.store._open(self.key)
	def get_named_file(self):
		return self.store._get_named_file(self.key)
	def __repr__(self):
		return "<BerthaMediaFile %s>" % self._key

class BerthaMediaStore(MediaStore):
	def __init__(self, settings, logger):
		super(BerthaMediaStore, self).__init__(settings, logger)
		self.register_on_setting_changed('host', self.osc_creds)
		self.register_on_setting_changed('port', self.osc_creds)
		self.ready = False
		self.keysCond = threading.Condition()
		self._keys = None
		self.osc_creds()

	def osc_creds(self):
		self.c = BerthaClient(self.host, self.port)
		self.threadPool.execute(self._do_refresh_keys)

	def _do_refresh_keys(self):
		with self.keysCond:
			self._keys = set(self.c.list())
			self.l.info("Got %s keys" % len(self._keys))
			self.keysCond.notifyAll()

	def create(self, stream):
		key = self.c.put_file(stream)
		with self.keysCond:
			self._keys.add(key)
		return self.by_key(key)

	def by_key(self, key):
		with self.keysCond:
			if self._keys is None:
				self.l.debug("by_key: waiting on keysCond")
				self.keysCond.wait()
			if not key in self._keys:
				raise KeyError, key
		return BerthaMediaFile(self, key)

	def remove(self, mediaFile):
		self.l.warning("Not implemented yet")
		return
	
	def _open(self, key):
		return self.c.get(key)

	def _get_named_file(self, key):
		raise NotImplementedError
	
	@property
	def keys(self):
		with self.keysCond:
			if self._keys is None:
				self.l.debug("keys: waiting on keysCond")
				self.keysCond.wait()
			return tuple(self._keys)
