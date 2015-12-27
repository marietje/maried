from __future__ import with_statement
import os
import time
import random
import logging
import datetime
import tempfile
import threading
import collections

from mirte.core import Module
from sarah.event import Event
from sarah.dictlike import DictLike

ChangeList = collections.namedtuple('ChangeList', (
        'added', 'updated', 'removed'))

class Denied(Exception):
    pass
class Stopped(Exception):
    pass
class EmptyQueueException(Exception):
    pass
class MissingTagsError(Denied):
    pass
class AlreadyInQueueError(Denied):
    pass
class MaxQueueLengthExceededError(Denied):
    pass
class MaxQueueCountExceededError(Denied):
    pass
class UnderPreShiftLock(Denied):
    pass


class Media(DictLike):
    def __init__(self, coll, data):
        super(Media, self).__init__(data)
        self.self.coll = coll
    def unlink(self):
        self.coll.unlink_media(self)
    def save(self):
        self.coll.save_media(self)
    @property
    def mediaFile(self):
        return self.coll.mediaStore.by_key(self.mediaFileKey)
    def __eq__(self, other):
        if not isinstance(other, Media):
            return False
        return self.key == other.key
    def __ne__(self, other):
        if not isinstance(other, Media):
            return True
        return self.key != other.key


class MediaFile(object):
    def __init__(self, store, key):
        self._key = key
        self.store = store
    def get_named_file(self):
        raise NotImplementedError
    def open(self):
        raise NotImplementedError
    def remove(self):
        self.store.remove(self)
    def __eq__(self, other):
        if not isinstance(other, MediaFile):
            return False
        return self._key == other.key
    def __ne__(self, other):
        if not isinstance(other, MediaFile):
            return True
        return self._key != other.key
    def get_info(self):
        stream = self.open()
        ret = self.store.mediaInfo.get_info(stream)
        if hasattr(stream, 'close'):
            stream.close()
        return ret
    @property
    def key(self):
        return self._key
class BaseRequest(DictLike):
    @property
    def by(self):
        try:
            return DictLike.__getattr__(self, 'by')
        except AttributeError:
            return None
    def __repr__(self):
        return "<%s %r by %r>" % (self.__class__.__name__,
                                        self.media, self.by)
class PastRequest(BaseRequest):
    def __init__(self, history, data):
        super(PastRequest, self).__init__(data)
        self.self.history = history
    def remove(self):
        raise NotImplementedError
class Request(BaseRequest):
    def __init__(self, queue, data):
        super(Request, self).__init__(data)
        self.self.queue = queue
    def move(self, amount):
        self.queue.move(self, amount)
    def cancel(self):
        self.queue.cancel(self)
    @property
    def mediaKey(self):
        return self.media.key
    @property
    def byKey(self):
        try:
            return self.by.key
        except AttributeError:
            return None
class User(DictLike):
    def has_access(self):
        raise NotImplementedError
    def __eq__(self, other):
        if not isinstance(other, User):
            return False
        return self.key == other.key
    def __ne__(self, other):
        if not isinstance(other, User):
            return True
        return self.key != other.key

class Desk(Module):
    def __init__(self, *args, **kwargs):
        super(Desk, self).__init__(*args, **kwargs)
        self.on_playing_changed = Event()
        self.on_media_changed = Event()
        self.on_requests_changed = Event()
        self.orchestrator.on_playing_changed.register(
                self._on_playing_changed)
        self.collection.on_changed.register(
                self._on_collection_changed)
        self.queue.on_changed.register(
                self._on_requests_changed)
    def _on_playing_changed(self, previous_playing):
        self.on_playing_changed(previous_playing)
    def _on_collection_changed(self, changeList):
        self.on_media_changed(changeList)
    def _on_requests_changed(self):
        self.on_requests_changed()
    def list_media(self):
        return self.collection.media
    def list_media_keys(self):
        return self.collection.media_keys
    def get_media_count(self):
        return self.collection.media_count
    def request_media(self, media, user):
        self.users.assert_request(user, media)
        self.queue.request(media, user)
    def add_media(self, stream, user, customInfo=None):
        mediaFile = self.mediaStore.create(stream)
        try:
            self.users.assert_addition(user, mediaFile)
        except Denied:
            mediaFile.remove()
            raise
        try:
            return self.collection.add(mediaFile, user, customInfo)
        except Exception, e:
            mediaFile.remove()
            raise e
    def query_media(self, query, skip=0, count=None):
        return self.collection.query(query, skip, count)
    def list_requests(self):
        return self.queue.requests
    def skip_playing(self, user):
        playing = self.orchestrator.get_playing()
        # We check the end time to avoid to skip a race conditon
        # and skip the next song.
        if playing[2] and (playing[2]  - datetime.datetime.now()
                    < datetime.timedelta(0, 1)):
            raise Denied
        self.users.assert_skip(user, playing[1])
        self.orchestrator.skip()
    def cancel_request(self, request, user):
        self.users.assert_cancel(user, request)
        request.cancel()
    def move_request(self, request, amount, user):
        self.users.assert_move(user, request, amount)
        request.move(amount)
    def get_playing(self):
        return self.orchestrator.get_playing()
    def user_by_key(self, key):
        return self.users.by_key(key)
    def media_by_key(self, key):
        return self.collection.by_key(key)

class History(Module):
    def __init__(self, *args, **kwargs):
        super(History, self).__init__(*args, **kwargs)
        # when a record is added runtime
        self.on_record = Event()
        # when the whole history is changed (db change, et al)
        self.on_pretty_changed = Event()
    def record(self, media, request, at):
        raise NotImplementedError
    def list_past_requests(self):
        raise NotImplementedError

class Users(Module):
    def assert_request(self, user, media):
        raise NotImplementedError
    def assert_addition(self, user, mediaFile):
        raise NotImplementedError
    def assert_cancel(self, user, request):
        raise NotImplementedError
    def assert_move(self, user, request, amount):
        raise NotImplementedError
    def assert_access(self, user):
        return NotImplementedError
    def by_key(self, key):
        return NotImplementedError

class RandomQueue(Module):
    def __init__(self, *args, **kwargs):
        super(RandomQueue, self).__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self.list = list()
        self.on_changed = Event()
        self.register_on_setting_changed('length', self.osc_length)
        self.osc_length()
        self.random.on_ready.register(self._random_on_ready)
        self.pre_shift_lock = False
    def _random_on_ready(self):
        with self.lock:
            self._fill()
        self.on_changed()
    @property
    def requests(self):
        with self.lock:
            return reversed(self.list)
    def peek(self, set_pre_shift_lock=False):
        with self.lock:
            if not self.list:
                raise EmptyQueueException
            ret = self.list[-1]
            if set_pre_shift_lock:
                self.pre_shift_lock = True
            return ret
    def shift(self):
        with self.lock:
            if not self.list:
                raise EmptyQueueException
            ret = self.list.pop()
            self._grow()
            self.pre_shift_lock = False
        self.on_changed()
        return ret
    def _grow(self):
        if self.random.ready:
            self.list.insert(0, Request(self, {
                'media': self.random.pick(),
                'by': None}))
    def request(self, media, user):
        assert False # shouldn't do that
    def cancel(self, request):
        with self.lock:
            if self.pre_shift_lock and request == self.list[-1]:
                raise UnderPreShiftLock
            self.list.remove(request)
            self._grow()
        self.on_changed()
    def move(self, request, amount):
        assert False # shouldn't do that
    def osc_length(self):
        with self.lock:
            self._fill()
        self.on_changed()
    def _fill(self):
        if not self.random.ready:
            return
        if len(self.list) < self.length:
            for i in xrange(self.length - len(self.list)):
                self._grow()
        else:
            self.list = self.list[:self.length]

class AmalgamatedQueue(Module):
    def __init__(self, *args, **kwargs):
        super(AmalgamatedQueue, self).__init__(*args, **kwargs)
        self.on_changed = Event()
        self.register_on_setting_changed('first', self.osc_first)
        self.register_on_setting_changed('second', self.osc_second)
        self.osc_first()
        self.osc_second()
    def peek(self, set_pre_shift_lock):
        try:
            return self.first.peek(set_pre_shift_lock)
        except EmptyQueueException:
            return self.second.peek(set_pre_shift_lock)
    def osc_first(self):
        self.first.on_changed.register(self._subqueue_changed)
    def osc_second(self):
        self.second.on_changed.register(self._subqueue_changed)
    def _subqueue_changed(self):
        self.on_changed()
    def request(self, media, user):
        self.first.request(media, user)
    @property
    def requests(self):
        return (tuple(self.first.requests) +
            tuple(self.second.requests))
    def shift(self):
        assert False # shouldn't do that
    def cancel(self, request):
        if request in self.first.requests:
            self.first.cancel(request)
        else:
            self.second.cancel(request)
    def move(self, request, amount):
        if request in self.first.requests:
            self.first.move(request, amount)
        else:
            self.second.move(request, amount)

class Queue(Module):
    def __init__(self, *args, **kwargs):
        super(Queue, self).__init__(*args, **kwargs)
        self.list = list()
        self.lock = threading.Lock()
        self.on_changed = Event()
        self.pre_shift_lock = False
    def request(self, media, user):
        with self.lock:
            self.list.insert(0, Request(self, {
                    'media': media,
                    'by': user}))
        self.on_changed()
    @property
    def requests(self):
        with self.lock:
            return reversed(self.list)
    def peek(self, set_pre_shift_lock=False):
        """ Returns the first element in the Queue

        If set_pre_shift_lock is set, then the queue will disallow
        any modification to the first element in the Queue, until
        it has been shifted. """
        with self.lock:
            if not self.list:
                raise EmptyQueueException
            ret = self.list[-1]
            if set_pre_shift_lock:
                self.pre_shift_lock = True
            return ret
    def shift(self):
        with self.lock:
            if not self.list:
                raise EmptyQueueException
            ret = self.list.pop()
            self.pre_shift_lock = False
        self.on_changed()
        return ret
    def cancel(self, request):
        with self.lock:
            if self.pre_shift_lock and self.list[-1] == request:
                raise UnderPreShiftLock
            self.list.remove(request)
        self.on_changed()
    def move(self, request, amount):
        aa = abs(amount)
        with self.lock:
            o = self.list if amount != aa else list(
                        reversed(self.list))
            if self.pre_shift_lock and o[-1] == request:
                raise UnderPreShiftLock
            idx = o.index(request)
            n = (o[:idx] +
                 o[idx+1:idx+aa+1] +
                 [o[idx]] +
                 o[idx+aa+1:])
            self.list = n if amount != aa else list(reversed(n))
        self.on_changed()

class Orchestrator(Module):
    def __init__(self, *args, **kwargs):
        super(Orchestrator, self).__init__(*args, **kwargs)
        self.on_playing_changed = Event()
        self.lock = threading.Lock()
        self.playing_media = None
        self.satisfied_request = None
        self.player.on_about_to_finish.register(
                self._player_on_about_to_finish)
        self.player.on_playing_started.register(
                self._player_on_playing_started)
        self.player.on_playing_finished.register(
                self._player_on_playing_finished)
        self.peeked_from_randomQueue = True
        self.next_satisfied_request = None
        self.next_playing_media = None
        self.previously_playing = None

    def get_playing(self):
        with self.lock:
            return (self.playing_media,
                self.satisfied_request,
                self.player.endTime)
    def run(self):
        with self.lock:
            self.running = True
        self._queue_next()

    def stop(self):
        with self.lock:
            self.running = False

    def _queue_next(self):
        self.lock.acquire()
        try:
            if not self.running:
                return
            req = None
            try:
                req = self.queue.peek(set_pre_shift_lock=True)
                media = req.media
                assert not media is None
                self.peeked_from_randomQueue = False
            except EmptyQueueException:
                media = self._peek_from_randomQueue()
            self.next_playing_media = media
            self.next_satisfied_request = req
            self.player.queue(media)
        except Stopped:
            if self.running:
                self.l.exception("Unexpected stopped raised")
        finally:
            self.lock.release()

    def skip(self):
        self.player.skip()

    def _player_on_playing_finished(self, media, endTime):
        with self.lock:
            assert media == self.playing_media
            self.previously_playing = (media,
                         self.satisfied_request,
                         endTime)
            satisfied_request = self.satisfied_request
        if not media is None:
            self.history.record(media,
                satisfied_request,
                endTime  - datetime.timedelta(0, media.length))

    def _player_on_playing_started(self, media, endTime):
        with self.lock:
            assert media == self.next_playing_media
            self.satisfied_request = self.next_satisfied_request
            self.next_satisfied_request = None
            self.next_playing_media = None
            self.playing_media = media
            if self.peeked_from_randomQueue:
                self.randomQueue.shift()
            else:
                self.queue.shift()
            previously_playing = self.previously_playing
            self.previously_playing = None
        self.on_playing_changed(previously_playing)
    def _peek_from_randomQueue(self):
        while True:
            try:
                media = self.randomQueue.peek(
                    set_pre_shift_lock=True).media
                self.peeked_from_randomQueue = True
                return media
            except EmptyQueueException:
                self.lock.release()
                self.wait_for_media()
                self.lock.acquire()
                if not self.running:
                    self.l.info("    but we stopped")
                    return

    def _player_on_about_to_finish(self):
        self._queue_next()
    
    def wait_for_media(self):
        self.l.info("Randomqueue couldn't return media -- collection "+
                "is assumed to be empty -- waiting for media.")
        self.randomQueue.random.collection.got_media_event.wait()
        self.l.info("Woke!")

class Random(Module):
    def __init__(self, *args, **kwargs):
        super(Random, self).__init__(*args, **kwargs)
        self.on_ready = Event()
    def pick(self):
        raise NotImplementedError
    @property
    def ready(self):
        return True

class SimpleRandom(Random):
    def __init__(self, *args, **kwargs):
        super(SimpleRandom, self).__init__(*args, **kwargs)
        self.keys = None
        self.lock = threading.Lock()
        self.register_on_setting_changed('collection',
                self.osc_collection)
        self.osc_collection()
    def osc_collection(self):
        self.collection.on_changed.register(self._on_collection_changed)
        self._on_collection_changed()
    def _on_collection_changed(self):
        self.l.debug("Caching keys")
        keys = list(self.collection.media_keys)
        self.l.debug("Cached %s keys", len(keys))
        with self.lock:
            signal_ready = self.keys is None
            self.keys = keys
        if signal_ready:
            self.on_ready()
    def pick(self):
        return self.collection.by_key(
                self.keys[random.randint(0,len(self.keys)-1)])
    @property
    def ready(self):
        with self.lock:
            return self.keys is not None

class MediaInfo(Module):
    def get_info(self, stream):
        pass
    def get_info_by_path(self, path):
        with open(path) as f:
            return self.get_info(f)

class MediaStore(Module):
    def create(self, stream):
        raise NotImplementedError
    def by_key(self, key):
        raise NotImplementedError
    @property
    def keys(self):
        raise NotImplementedError

class Player(Module):
    def __init__(self, *args, **kwargs):
        super(Player, self).__init__(*args, **kwargs)
        self.endTime = None
        self.on_about_to_finish = Event()
        self.on_playing_started = Event()
        self.on_playing_finished = Event()
    def stop(self):
        raise NotImplementedError
    def queue(self, media):
        raise NotImplementedError

class Collection(Module):
    def __init__(self, *args, **kwargs):
        super(Collection, self).__init__(*args, **kwargs)
        self.on_keys_changed = Event()
        self.on_changed = Event()
        # got_media_event is set when the Collection isn't
        # empty.
        self.got_media_event = threading.Event()
    def add(self, mediaFile, user, extraInfo=None):
        raise NotImplementedError
    @property
    def media(self):
        raise NotImplementedError
    @property
    def media_keys(self):
        raise NotImplementedError
    def by_key(self, key):
        raise NotImplementedError
    def save_media(self, media):
        raise NotImplementedError
    def unlink_media(self, media):
        raise NotImplementedError

