import os
import time
import base64
import hashlib

import maried

from mirte.core import Module
from sarah.event import Event
from sarah._itertools import iter_by_n
from joyce.base import JoyceChannel


class MariedChannelClass(JoyceChannel):
	def __init__(self, server, *args, **kwargs):
		super(MariedChannelClass, self).__init__(*args, **kwargs)
                self.server = server
                self.user = None
                self.login_token = None
                self.send_message({
                        'type': 'welcome',
                        'protocols': [0],
                        'note': 'The API is not stable'})

        def handle_stream(self, stream):
                if self.user is None:
                        stream.close()
                        self.send_message({
                                'type': 'error_upload',
                                'message': 'Please log in before uploading'})
                        return
                self.l.info('Download started')
                mf = self.server.desk.add_media(stream, self.user)
                self.l.info('Download finished: ' + repr(mf))

	def handle_message(self, data):
		if data['type'] == 'get_playing':
			t = self.server._get_playing()
			t['type'] = 'playing'
			self.send_message(t)
		elif data['type'] == 'list_media':
                        self.send_message({
                                'type': 'media',
                                'count': self.server.desk.get_media_count()})
                        for ms in iter_by_n(self.server.desk.list_media(), 2):
                                self.send_message({
                                        'type': 'media_part',
                                        'part': [{
                                                'key': str(m.key),
                                                'artist': m.artist,
                                                'title': m.title,
                                                'uploadedByKey':
                                                        str(m.uploadedByKey),
                                                'uploadedTimestamp':
                                                        m.uploadedTimestamp,
                                                'length': m.length}
                                                        for m in ms]})
		elif data['type'] == 'list_requests':
			self.send_message({
				'type': 'requests',
				'requests': [{
                                                'byKey': None if r.byKey is None
                                                        else str(r.byKey),
                                                'mediaKey': str(r.mediaKey)
                                             } for r
                                        in self.server.desk.list_requests()]})
                elif data['type'] == 'request_login_token':
                        self.login_token = base64.b64encode(os.urandom(6))
                        self.send_message({
                                'type': 'login_token',
                                'login_token': self.login_token})
                elif data['type'] == 'login':
                        if ('username' not in data or
                            'hash' not in data):
                                self.send_message({
                                        'type': 'error_login',
                                        'message': 'Expected user and hash'})
                                return
                        try:
                                user = self.server.desk.user_by_key(
                                                data['username'])
                        except KeyError:
                                self.send_message({
                                        'type': 'error_login',
                                        'message': 'User does not exist'})
                                return
                        expected_hash = hashlib.md5(user.passwordHash +
                                                self.login_token).hexdigest()
                        if expected_hash != data['hash']:
                                self.send_message({
                                        'type': 'error_login',
                                        'message': 'Wrong password'})
                                return
                        self.user = user
                        self.send_message({
                                'type': 'logged_in'})

class JoyceRS(Module):
	def __init__(self, *args, **kwargs):
		super(JoyceRS, self).__init__(*args, **kwargs)
		self.joyceServer.channel_class = self._channel_constructor
		self.desk.on_media_changed.register(
				self._on_media_changed)
		self.desk.on_playing_changed.register(
				self._on_playing_changed)
	def _channel_constructor(self, *args, **kwargs):
		return MariedChannelClass(self, *args, **kwargs)
	def _on_media_changed(self):
		self.joyceServer.broadcast_message({
                        'type': 'collection_changed'})
	def _get_playing(self):
		playing = self.desk.get_playing()
                return {'mediaKey': None if playing[0] is None
                                else str(playing[0].key),
                        'byKey': None if playing[1] is None
                                else str(playing[1].key),
                        'endTime': (time.mktime(playing[2].timetuple()) if
                                not playing[2] is None else None)}

	def _on_playing_changed(self, previous_playing):
		t = self._get_playing()
		t['type'] = 'playing_changed'
		self.joyceServer.broadcast_message(t)

