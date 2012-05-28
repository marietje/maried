from __future__ import with_statement

from maried.core import MediaStore, MediaFile, Media, Collection, User, Users, \
                        History, Request, PastRequest, Denied, \
                        AlreadyInQueueError, MaxQueueLengthExceededError, \
                        ChangeList, MissingTagsError, Random
from mirte.core import Module
from sarah.event import Event
from sarah.dictlike import AliasingMixin

try:
        from pymongo.objectid import ObjectId
except ImportError:
        from bson import ObjectId

import threading
import tempfile
import pymongo
import hashlib
import random
import gridfs
import base64
import time
import os

class MongoMediaFile(MediaFile):
        def open(self):
                return self.store._open(self.key)
        def get_named_file(self):
                return self.store._get_named_file(self.key)
        def __repr__(self):
                return "<MongoMediaFile %s>" % self._key

class MongoUser(AliasingMixin, User):
        aliases = {'key': '_id',
                   'realName': 'n',
                   'level': 'l',
                   'accessKey': 'a',
                   'passwordHash': 'p'}
        def __init__(self, coll, data):
                super(MongoUser, self).__init__(self.normalize_dict(data))
                self.self.collection = coll
        @property
        def has_access(self):
                return self.level >= 2
        @property
        def may_cancel(self):
                return self.level >= 3
        @property
        def may_skip(self):
                return self.level >= 5
        @property
        def is_admin(self):
                return self.level >= 5
        @property
        def may_move(self):
                return self.level >= 3
        def check_password(self, password):
                return self.passwordHash == hashlib.md5(password).hexdigest()
        def set_password(self, password):
                self.passwordHash = hashlib.md5(password).hexdigest()
        def regenerate_accessKey(self):
                self.accessKey = base64.b64encode(os.urandom(6))
        def save(self):
                self.collection._save_user(self)

class MongoMedia(AliasingMixin, Media):
        aliases = {'key': '_id',
                   'artist': 'a',
                   'title': 't',
                   'trackGain': 'tg',
                   'trackPeak': 'tp',
                   'length': 'l',
                   'mediaFileKey': 'k',
                   'uploadedByKey': 'ub',
                   'randomOffset': 'r',
                   'uploadedTimestamp': 'ut'}
        def __init__(self, coll, data):
                super(MongoMedia, self).__init__(coll,
                                self.normalize_dict(data))
        def __repr__(self):
                return '<MongoMedia %s - %s (%s)>' % (self.artist, self.title,
                                                        str(self.key))

class MongoPastRequest(AliasingMixin, PastRequest):
        aliases = {'key': '_id',
                   'byKey': 'b',
                   'at': 'a',
                   'mediaKey': 'm'}
        def __init__(self, history, data):
                super(MongoPastRequest, self).__init__(history,
                                self.normalize_dict(data))

class MongoRequest(AliasingMixin, PastRequest):
        aliases = {'key': '_id',
                   'byKey': 'b',
                   'mediaKey': 'm'}
        def __init__(self, queue, data):
                super(MongoRequest, self).__init__(queue,
                                self.normalize_dict(data))


class MongoDb(Module):
        def __init__(self, *args, **kwargs):
                super(MongoDb, self).__init__(*args, **kwargs)
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
                self.db = self.con[self.db]
                self.ready = True
                self.on_changed()

class MongoCollection(Collection):
        def __init__(self, *args, **kwargs):
                super(MongoCollection, self).__init__(*args, **kwargs)
                self._media = None
                self.lock = threading.Lock()
                self.register_on_setting_changed('db', self.osc_db)
                self.register_on_setting_changed('mediaCollection',
                                        self.on_db_changed)
                self.register_on_setting_changed('usersCollection',
                                        self.on_db_changed)
                self.osc_db()
        
        def osc_db(self):
                self.db.on_changed.register(self.on_db_changed)
                self.on_db_changed()

        def on_db_changed(self):
                if not self.db.ready:
                        return
                with self.lock:
                        self.cUsers = self.db.db[self.usersCollection]
                        self.cMedia = self.db.db[self.mediaCollection]
                        if self.cMedia.count():
                                self.got_media_event.set()
                        else:
                                self.got_media_event.clear()
        
        @property
        def media(self):
                for d in self.cMedia.find():
                        yield MongoMedia(self, d)

        @property
        def media_keys(self):
                for d in self.cMedia.find({},{}):
                        yield d['_id']

        @property
        def media_count(self):
                return self.cMedia.count()

        def by_key(self, key):
                if isinstance(key, basestring):
                        key = ObjectId(key)
                d = self.cMedia.find_one({'_id': key})
                if d is None:
                        raise KeyError
                return MongoMedia(self, d)

        def _user_by_key(self, key):
                d = self.cUsers.find_one({'_id': key})
                if d is None:
                        raise KeyError
                return MongoUser(self, d)

        def stop(self):
                self.got_media_event.set()

        def add(self, mediaFile, user, extraInfo=None):
                info = mediaFile.get_info()
                if not extraInfo is None:
                        info.update(extraInfo)
                info.update({
                        'mediaFileKey': mediaFile.key,
                        'uploadedTimestamp': time.time(),
                        'randomOffset': random.random(),
                        'uploadedByKey': user.key})
                if not 'artist' in info or not 'title' in info:
                        raise MissingTagsError
                key = self.cMedia.insert(MongoMedia.normalize_dict(info))
                info['_id'] = key
                with self.lock:
                        if not self.got_media_event.is_set():
                                self.got_media_event.set()
                return MongoMedia(self, info)
        
        def _unlink_media(self, media):
                self.cMedia.remove({'_id': media.key})
        
        def _save_media(self, media):
                self.cMedia.save(media.to_dict())

        def _save_user(self, user):
                self.cUsers.save(user.to_dict())

        def _pick_random_media(self):
                offset = random.random()
                d = self.cMedia.find_one({'r': {'$gte': offset}},
                                sort=[('r', -1)])
                if d is None:
                        d = self.cMedia.find_one({'r': {'$lt': offset}},
                                        sort=[('r', 1)])
                        if d is None:
                                return None
                return MongoMedia(self, d)

class MongoMediaStore(MediaStore):
        def __init__(self, *args, **kwargs):
                super(MongoMediaStore, self).__init__(*args, **kwargs)
                self.ready = False
                self.keysCond = threading.Condition()
                self._keys = None
                self.register_on_setting_changed('db', self.osc_db)
                self.osc_db()

        def osc_db(self):
                self.db.on_changed.register(self.on_db_changed)
                self.on_db_changed()

        def on_db_changed(self):
                if not self.db.ready:
                        return
                self.fs = gridfs.GridFS(self.db.db, self.collection)
                self.threadPool.execute_named(self._do_refresh_keys,
                                '%s _do_refresh_keys' % self.l.name)
        
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
                stream.close()
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

class MongoHistory(History):
        def __init__(self, *args, **kwargs):
                super(MongoHistory, self).__init__(*args, **kwargs)
                self.register_on_setting_changed('db', self.osc_db)
                self.osc_db()

        def osc_db(self):
                self.db.on_changed.register(self._on_db_changed)
                self._on_db_changed()
        
        def _on_db_changed(self):
                if not self.db.ready:
                        return
                self.cHistory = self.db.db[self.collection]
                self.on_pretty_changed()
        
        def record(self, media, request, at):
                self.l.info(repr(media if request is None else request))
                info = {'mediaKey': media.key,
                        'byKey': None if (request is None or request.by is None)
                                        else request.by.key,
                        'at':  time.mktime(at.timetuple())}
                info['key'] = self.cHistory.insert(
                                MongoPastRequest.normalize_dict(info))
                self.on_record(MongoPastRequest(self, info))
        
        def list_past_requests(self):
                for tmp in self.cHistory.find():
                        yield MongoPastRequest(self, tmp)

class MongoUsers(Users):
        def assert_request(self, user, media):
                if not user.has_access:
                        raise Denied
                requests = self.queue.requests
                if any(map(lambda x: x.media == media, requests)):
                        raise AlreadyInQueueError
                ureqs = filter(lambda y: y.by == user, requests)
                if len(ureqs) > self.maxQueueCount:
                        raise MaxQueueCountExceededError
                if (sum(map(lambda x: x.media.length, ureqs)) >
                                self.maxQueueLength):
                        raise MaxQueueLengthExceededError
        def assert_addition(self, user, mediaFile):
                pass
        def assert_cancel(self, user, request):
                if user.is_admin:
                        return
                if request.by == user:
                        return
                if not user.may_cancel:
                        raise Denied
        def assert_move(self, user, request, amount):
                if request.by == user and amount > 0:
                        return
                if not user.may_move:
                        raise Denied
        def assert_skip(self, user, request):
                if not user.may_skip:
                        raise Denied
        def by_key(self, key):
                return self.collection._user_by_key(key)

class MongoSimpleRandom(Random):
        def pick(self):
                ret = self.collection._pick_random_media()
                if ret is None:
                        self.l.info("Waiting on collection.got_media_event")
                        self.collection.got_media_event.wait()
                        ret = self.colleciton._pick_random_media()
                return ret
