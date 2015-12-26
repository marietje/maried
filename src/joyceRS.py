import os
import time
import base64
import hashlib
import threading

import maried
from maried.core import AlreadyInQueueError, Denied, MissingTagsError

from mirte.core import Module
from sarah.event import Event
from sarah._itertools import iter_by_n
from joyce.base import JoyceChannel

def _media_dict(media):
    return {'key': str(media.key),
        'artist': media.artist,
        'title': media.title,
        'uploadedByKey': str(media.uploadedByKey),
        'length': media.length}


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
        try:
            mf = self.server.desk.add_media(stream, self.user)
            self.l.info('Download finished: ' + repr(mf))
        except MissingTagsError:
            self.send_message({
                'type': 'error_upload',
                'message': 'Your upload missed some tags'})
        except Denied:
            self.send_message({
                'type': 'error_upload',
                'message': 'Your upload was denied'})
        finally:
            stream.close()

    def handle_message(self, data):
        if data['type'] == 'follow':
            for followed in data['which']:
                self.server._register_follower(self, followed)
        elif data['type'] == 'unfollow':
            for followed in data['which']:
                self.server._unregister_follower(self, followed)
        elif data['type'] == 'request_login_token':
            self.login_token = base64.b64encode(os.urandom(6))
            self.send_message({
                'type': 'login_token',
                'login_token': self.login_token})
        elif data['type'] == 'regenerate_accessKey':
            if self.user is None:
                self.send_message({
                    'type': 'error_regenerate_accessKey',
                    'message': 'not logged in'})
                return
            self.user.regenerate_accessKey()
            self.user.save()
            self.send_message({
                'type': 'accessKey',
                'accessKey': self.user.accessKey})
        elif data['type'] == 'login' or \
             data['type'] == 'login_accessKey':
            if ('username' not in data or
                'hash' not in data):
                self.send_message({
                    'type': 'error_'+data['type'],
                    'message': 'Expected user and hash'})
                return
            try:
                user = self.server.desk.user_by_key(
                        data['username'])
            except KeyError:
                self.send_message({
                    'type': 'error_'+data['type'],
                    'message': 'User does not exist'})
                return
            secret = user.passwordHash if data['type'] == 'login' \
                    else user.accessKey
            if secret is None:
                self.send_message({
                    'type': 'error_'+data['type'],
                    'message': 'Secret not set'})
                return
            if self.login_token is None:
                self.send_message({
                    'type': 'error_'+data['type'],
                    'message': 'No login_token requested'})
                return
            expected_hash = hashlib.md5(secret +
                        self.login_token).hexdigest()
            if expected_hash != data['hash']:
                self.send_message({
                    'type': 'error_'+data['type'],
                    'message': 'Wrong password'})
                return
            self.user = user
            if user.accessKey is None:
                user.regenerate_accessKey()
                user.save()
            self.send_message({
                'type': 'logged_in',
                'accessKey': user.accessKey})
        elif data['type'] == 'request':
            if self.user is None:
                self.send_message({
                    'type': 'error_request',
                    'message': 'Please log in before '+
                            'requesting'})
                return
            try:
                m = self.server.desk.media_by_key(
                        data['mediaKey'])
            except KeyError:
                self.send_message({
                    'type': 'error_request',
                    'message': 'No such media'})
                return
            try:
                m = self.server.desk.request_media(m, self.user)
            except AlreadyInQueueError:
                self.send_message({
                    'type': 'error_request',
                    'message': 'Already queued'})
            except Denied:
                self.send_message({
                    'type': 'error_request',
                    'message': 'Request denied'})
        elif data['type'] == 'cancel_request':
            if not 'key' in data:
                self.send_message({
                    'type': 'error_cancel_request',
                    'message': 'Missing key'})
                return
            try:
                req = self.server.requests_ns.by_key(
                        data['key'])
            except KeyError:
                self.send_message({
                    'type': 'error_cancel_request',
                    'message': 'Request not found'})
                return
            self.server.desk.cancel_request(req, self.user)
        elif data['type'] == 'move_request':
            if not 'key' in data or not 'amount' in data:
                self.send_message({
                    'type': 'error_move_request',
                    'message': 'Missing key or amount'})
                return
            try:
                req = self.server.requests_ns.by_key(
                    data['key'])
            except KeyError:
                self.send_message({
                    'type': 'error_move_request',
                    'message': 'Request not found'})
                return
            self.server.desk.move_request(req, data['amount'],
                            self.user)
        elif data['type'] == 'list_media':
            self.server._send_all_media((self,))
        elif data['type'] == 'skip_playing':
            self.server.desk.skip_playing(self.user)
        elif data['type'] == 'query_media':
            self.send_message({
                'type': 'query_media_results',
                'token': data.get('token'),
                'results': [_media_dict(m)
                    for m in self.server.desk.query_media(
                        data.get('query', ''),
                        data.get('skip', 0),
                        data.get('count', None))]})
        else:
            self.send_message({
                'type': 'error',
                'message': 'unknown msg type %s' % \
                        data['type']})

    def after_close(self):
        self.l.debug("Closed")
        self.server._remove_follower(self)

class JoyceRS(Module):
    def __init__(self, *args, **kwargs):
        super(JoyceRS, self).__init__(*args, **kwargs)
        self.joyceServer.channel_class = self._channel_constructor
        self.desk.on_playing_changed.register(
                self._on_playing_changed)
        self.desk.on_requests_changed.register(
                self._on_requests_changed)
        self.following_lut = {
                'requests': (set(), self._send_all_requests),
                'playing': (set(), self._send_playing),
                'media': (set(), self._send_all_media)}
        self.lock = threading.Lock()
        self.requests_ns = self.refStore.create_namespace()
    def _channel_constructor(self, *args, **kwargs):
        return MariedChannelClass(self, *args, **kwargs)
    def _on_requests_changed(self):
        self._send_all_requests(self._followers_of('requests'))
    def _on_playing_changed(self, previously_playing):
        self._send_playing(self._followers_of('playing'))

    def _send_playing(self, followers):
        playing = self.desk.get_playing()
        msg = { 'type': 'playing',
            'playing': {
            'media': None if playing[0] is None
                else _media_dict(playing[0]),
            'byKey': None if playing[1] is None
                else str(playing[1].byKey),
            'serverTime': time.time(),
            'endTime': (time.mktime(playing[2].timetuple()) if
                not playing[2] is None else None)}}
        for follower in followers:
            follower.send_message(msg)

    def _send_all_requests(self, followers):
        msg = {
            'type': 'requests',
            'requests': [{
                    'key': self.requests_ns.key_of(r),
                    'byKey': None if r.byKey is None
                        else str(r.byKey),
                    'media': _media_dict(r.media)
                     } for r
                in self.desk.list_requests()]}
        for follower in followers:
            follower.send_message(msg)

    def _send_all_media(self, followers):
        for follower in followers:
            follower.send_message({
                'type': 'media',
                'count': self.desk.get_media_count()})
        for ms in iter_by_n(self.desk.list_media(), 250):
            msg = {
                'type': 'media_part',
                'part': [_media_dict(m) for m in ms]}
            for follower in followers:
                follower.send_message(msg)

    def _followers_of(self, followed):
        with self.lock:
            return tuple(self.following_lut[followed][0])

    def _register_follower(self, follower, followed):
        with self.lock:
            if not followed in self.following_lut:
                raise KeyError
            self.following_lut[followed][0].add(follower)
            full_cb = self.following_lut[followed][1]
        full_cb([follower])

    def _unregister_follower(self, follower, followed):
        with self.lock:
            if not followed in self.following_lut or \
                    follower not in self.following_lut[
                            followed][0]:
                raise KeyError
            self.following_lut[followed][0].remove(follower)

    def _remove_follower(self, follower):
        with self.lock:
            for v in self.following_lut.itervalues():
                if follower in v[0]:
                    v[0].remove(follower)

