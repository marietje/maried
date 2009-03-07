from __future__ import with_statement
import threading

class Module(object):
	def __init__(self, settings, logger):
		for k, v in settings.items():
			setattr(self, k, v)
		self.l = logger
		self.on_settings_changed = dict()
	
	def change_setting(self, key, value):
		setattr(self, key, value)
		if not key in self.on_settings_changed:
			return
		self.on_settings_changed[key]()

	def register_on_setting_changed(self, key, handler):
		if not key in self.on_settings_changed:
			self.on_settings_changed[key] = Event()
		self.on_settings_changed[key].register(handler)

class Event(object):
	def __init__(self):
		self.handlers = []
	def register(self, handler):
		self.handlers.append(handler)
	def __call__(self, *args, **kwargs):
		for handler in self.handlers:
			handler(*args, **kwargs)

class Denied(Exception):
	pass
class EmptyQueueException(Exception):
	pass

class Media(object):
	pass
class MediaFile(object):
	def remove(self):
		self.store.remove(self)
	def get_key(self):
		raise NotImplementedError
class MediaFileInfo(object):
	pass
class Request(object):
	def __init__(self, queue, media, by):
		self.queue = queue
		self.media = media
		self.by = by
	def move(self, amount):
		self.queue.move(self, amount)
	def cancel(self):
		self.queue.cancel(self)
class User(object):
	def __init__(self, key, realName):
		self.realName = realName
		self.key = key
	

class Desk(Module):
	def list_media(self):
		return self.collection.list_media()
	def request_media(self, media, user):
		self.users.assert_request(user, media)
		self.queue.request(media, user)
	def add_media(self, stream, user):
		mediaFile = self.mediaStore.create(stream)
		try:
			self.users.assert_addition(user, mediaFile)
		except Denied:
			mediaFile.remove()
			raise
		self.collection.add(mediaFile, user)
	def list_requests(self):
		return self.queue.get_requests()
	def cancel_request(self, request, user):
		self.users.assert_cancel(user, request)
		request.cancel()
	def move_request(self, request, amount):
		self.users.assert_move(user, request, amount)
		request.move(amount)
	def get_playing(self):
		return self.orchestrator.get_playing()

class Users(Module):
	def assert_request(self, user, media):
		raise NotImplementedError
	def assert_addition(self, user, mediaFile):
		raise NotImplementedError
	def assert_cancel(self, user, request):
		raise NotImplementedError
	def assert_move(self, user, request, amount):
		raise NotImplementedError

class Queue(Module):
	def __init__(self, settings, logger):
		super(Queue, self).__init__(settings, logger)
		self.list = list()
		self.lock = threading.Lock()
	def request(self, media, user):
		with self.lock:
			self.list.insert(0, Request(self, media, user))
	def get_requests(self, media, user):
		with self.lock:
			return reversed(self.list)
	def shift(self):
		with self.lock:
			return self.list.pop()
	def cancel(self, request):
		with self.lock:
			self.list.remove(request)
	def move(self, request, amount):
		aa = abs(amount)
		with self.lock:
			o = self.list if amount == aa else reversed(self.list) 
			idx = o.index(request)
			n = (self.list[:idx] + 
			     self.list[idx+1:idx+aa+1] +
			     [self.list[idx]] +
			     self.list[idx+aa+1:])
			self.list = n if amount == aa else reversed(n)

class Orchestrator(Module):
	def __init__(self, settings, logger):
		super(Orchestrator, self).__init__(settings, logger)
		self.lock = threading.Lock()
	def get_playing(self):
		with self.lock:
			return (self.playing_media,
				self.satisfied_request,
				self.player.endTime)
	def stop(self):
		with self.lock:
			self.running = False

	def run(self):
		self.running = True
		while self.running:
			with self.lock:
				if not self.running: break
				req = None
				try:
					req = self.queue.shift()
					media = req.media
				except EmptyQueueException:
					media = self.random.pick()
				self.playing_media = media
				self.satisfied_request = req
			self.history.record(self.playing_media,
					    self.satisfied_request)
			self.player.play(media)

class Random(Module):
	def pick(self):
		raise NotImplementedError

class MediaInfo(Module):
	def get_info(self, stream):
		raise NotImplementedError

class MediaStore(Module):
	def create(self, stream):
		raise NotImplementedError
	def by_key(self, key):
		raise NotImplementedError

class Collection(Module):
	def __init__(self, settings, logger):
		super(Collection, self).__init__(settings, logger)
		self.on_keys_changed = Event()
	def add(self, mediaFile, user):
		raise NotImplementedError
	def list_media(self):
		raise NotImplementedError
	def media_keys(self):
		raise NotImplementedError
	def by_key(self, key):
		raise NotImplementedError

