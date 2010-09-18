from mirte.core import Module
from sarah.event import Event
from sarah.comet.server import CometServer, BaseCometSession

import time
import maried

class MariedCometSession(BaseCometSession):
	def handle_message(self, data):
		if data['type'] == 'get_playing':
			t = self.server._get_playing()
			t['type'] = 'playing'
			self.send_message(t)
		elif data['type'] == 'list_media':
			self.send_message({
				'type': 'media',
				'media': [m.to_dict() for m
					in self.server.desk.list_media()]})
		elif data['type'] == 'list_requests':
			self.send_message({
				'type': 'requests',
				'requests': [r.to_dict() for r
					in self.server.desk.list_requests()]})


class MariedCometServer(CometServer):
	def __init__(self, *args, **kwargs):
		super(MariedCometServer, self).__init__(*args, **kwargs)
		self.desk.on_media_changed.register(
				self._on_media_changed)
		self.desk.on_playing_changed.register(
				self._on_playing_changed)
	def create_session(self, token):
		return MariedCometSession(self, token)
	def _on_media_changed(self):
		self.send_message({'type': 'collection_changed'})
	def _get_playing(self):
		playing = self.desk.get_playing()
		by = None if playing[1] is None else playing[1].to_dict()
		media = None if playing[0] is None else playing[0].to_dict()
		endTime = (time.mktime(playing[2].timetuple()) if
				not playing[2] is None else None)
		return {'media': media,
			'requestedBy': by,
			'endTime': endTime}

	def _on_playing_changed(self, previous_playing):
		t = self._get_playing()
		t['type'] = 'playing_changed'
		self.send_message(t)

