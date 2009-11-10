from core import Module
import threading
import scrobbler
import time

class Scrobbler(Module):
	def __init__(self, settings, logger):
		super(Scrobbler, self).__init__(settings, logger)
		self.desk.on_playing_changed.register(self._on_playing_changed)
		self.cond = threading.Condition()
		self.running = True
		scrobbler.login(self.username, self.password, hashpw=True)
	def _on_playing_changed(self):
		with self.cond:
			self.cond.notify()
	def scrobble(self, media):
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
