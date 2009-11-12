from core import Module
import threading
import scrobbler
import time

class Scrobbler(Module):
	def __init__(self, settings, logger):
		super(Scrobbler, self).__init__(settings, logger)
		self.desk.on_playing_changed.register(self._on_playing_changed)
		self.register_on_setting_changed('username', self.osc_creds)
		self.register_on_setting_changed('password', self.osc_creds)
		self.cond = threading.Condition()
		self.running = True
		self.authenticated = False
	def osc_creds(self):
		if (not hasattr(self, 'username') or
				not hasattr(self, 'password')):
			return
		try:
			scrobbler.login(self.username,
					self.password, hashpw=True)
		except scrobbler.AuthError:
			self.l.error('Couldn\'t authenticate with last.fm')
			self.authenticated = False
			return
		self.authenticated = True

	def _on_playing_changed(self):
		with self.cond:
			self.cond.notify()
	def scrobble(self, media):
		if not self.authenticated:
			return
		if media.length <= 30:
			self.l.info("%s is too short to be scrobbled")
			return
		scrobbler.submit(media.artist, media.title, int(time.time()),
				length=int(media.length))
		scrobbler.flush()
	def run(self):
		while True:
			m, r, tmp = self.desk.get_playing()
			if not r is None:
				self.scrobble(m)
			with self.cond:
				if not self.running: break
				self.cond.wait()
				if not self.running: break
	def stop(self):
		self.running = False
		with self.cond:
			self.cond.notify()
