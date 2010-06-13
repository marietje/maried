from mirte.core import Module
from sarah.event import Event
from sarah.comet import CometServer, BaseCometSession

import time
import maried

class MariedCometSession(BaseCometSession):
	pass

class MariedCometServer(CometServer):
	def __init__(self, settings, logger):
		super(MariedCometServer, self).__init__(settings, logger,
				MariedCometSession)
		self.desk.on_media_changed.register(
				self._on_media_changed)
		self.desk.on_playing_changed.register(
				self._on_playing_changed)
	def _on_media_changed(self):
		pass
	def _on_playing_changed(self, previous_playing):
		playing = self.desk.get_playing()
		by = None if playing[1] is None else {
					'key': playing[1].by.key,
					'realName': playing[1].realName
					}
		endTime = (time.mktime(playing[2].timetuple()) if
				not playing[2] is None else None)
		self.send_message({
			'type': 'playing_changed',
			'playing': {
				'key': playing[0].key,
				'artist': playing[0].artist,
				'title': playing[0].title
			},
			'endTime': endTime})

