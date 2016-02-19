from __future__ import with_statement

from maried.core import MediaStore, MediaFile, Media, Collection, User, Users, \
            History, Request, PastRequest, Denied, \
            AlreadyInQueueError, MaxQueueLengthExceededError, \
            ChangeList, MissingTagsError, Random
from mirte.core import Module
from sarah.event import Event
from sarah.dictlike import AliasingMixin, AliasingDictLike

try:
    from pymongo.objectid import ObjectId
except ImportError:
    from bson import ObjectId

import unidecode
import threading
import itertools
import tempfile
import pymongo
import hashlib
import random
import gridfs
import base64
import string
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

class MongoQuery(AliasingDictLike):
    aliases = {'query': '_id',
               'is_cached': 'c',
               'last_used': 'l',
               'times_used': 't',
               'nMatches': 'n',
               'last_used_indirectly': 'L',
               'times_used_indirectly': 'T'}

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
               'queryCache': 'qc',
               'searchString': 's',
               'uploadedTimestamp': 'ut'}
    def __init__(self, coll, data):
        super(MongoMedia, self).__init__(coll,
                self.normalize_dict(data))
    def __repr__(self):
        return '<MongoMedia %s - %s (%s)>' % (self.artist.encode('utf-8'),
                self.title.encode('utf-8'), str(self.key))

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
        self.con = pymongo.MongoClient(self.host, self.port)
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
        self.register_on_setting_changed('queriesCollection',
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
            self.cQueries = self.db.db[self.queriesCollection]
            self.cQueries.ensure_index('c')
            self.cMedia.ensure_index('r')
            self.cMedia.ensure_index('ub')
            self.cMedia.ensure_index([('qc',1),('s',1)])
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
        # First, analyze the mediaFile.  This will get the tags,
        # replayGain, etc.
        self.l.info("%s: get_info", mediaFile.key)
        info = mediaFile.get_info()
        if not extraInfo is None:
            info.update(extraInfo)
        # Secondly, add more metadata.
        info.update({
            'mediaFileKey': mediaFile.key,
            'uploadedTimestamp': time.time(),
            'randomOffset': random.random(),
            'uploadedByKey': user.key,
            'queryCache': []})
        if not 'artist' in info or not 'title' in info:
            raise MissingTagsError
        info['searchString'] = unidecode.unidecode(
                info['artist'] + ' ' + info['title']).lower()
        # Thirdly, add it to the collection
        key = self.cMedia.insert(MongoMedia.normalize_dict(info))
        info['_id'] = key
        with self.lock:
            if not self.got_media_event.is_set():
                self.got_media_event.set()
        ret = MongoMedia(self, info)
        self.l.info("%s: inserted as %s", mediaFile.key, ret)
        # Finally, find which cached queries match.
        qc = []
        for d in self.cQueries.find({'c': True}, []):
            ok = True
            for bit in d['_id'].split(' '):
                if bit not in info['searchString']:
                    ok = False
                    break
            if ok:
                qc.append(d['_id'])
        self.cMedia.update({'_id': info['_id']},   # query
                {'$pushAll': {'qc': qc}})  # queryCache
        self.l.info("%s: queries matched: %s", mediaFile.key, len(qc))
        return ret
    
    def _unlink_media(self, media):
        self.cMedia.remove({'_id': media.key})
    
    def _save_media(self, media):
        self.cMedia.save(media.to_dict())

    def _save_user(self, user):
        self.cUsers.save(user.to_dict())

    def _pick_random_media(self):
        offset = random.random()
        d = self.cMedia.find_one({'r': {'$gte': offset}},
                sort=[('r', 1)])
        if d is None:
            d = self.cMedia.find_one({'r': {'$lt': offset}},
                    sort=[('r', -1)])
            if d is None:
                return None
        return MongoMedia(self, d)

    def query(self, query, skip=0, count=None):
        start_time = time.time()
        # Normalize the query: only words of a-z and 0-9 separated
        # by a single space.
        if len(query) > 1000:
            self.l.warn("Query too long.  Capping to 1000")
            query = query[:1000]
        query = ' '.join(filter(bool,
                filter(lambda x: string.digits + ' ' +
                    string.ascii_lowercase,
                unidecode.unidecode(query).lower()).split(' ')))
        # Trivial case: the query is the empty string
        if not query:
            ret = [MongoMedia(self, d)
                for d in self.cMedia.find(skip=skip,
                    limit=(0 if count is None else count), sort=[('s', 1)])]
            time_spent = time.time() - start_time
            self.l.debug("empty query; %s results; %s seconds",
                                len(ret), time_spent)
            return ret
        # There are three cases.
        #   (I)   This exact query is cached
        #   (II)  A prefix of this query is cached (eg. we want `them',
        #     but only `the' is cached.
        #   (III) The query and none of its prefixes are cached.
        # First, find out in which case we are.
        qs = [MongoQuery(d) for d in self.cQueries.find({'_id':
            {'$in': [query[:n] for n in xrange(1,len(query)+1)]}})]
        qs.sort(key=lambda q: -len(q.query))
        cached_qs = filter(lambda q: q.is_cached, qs)
        query_dict = {}
        if not cached_qs or cached_qs[0].query != query:
            # We are not in case (I), thus we need to search.
            query_dict.update({
                '$and': [{'s': {'$regex': bit}}
                    for bit in query.split(' ')]})
        if cached_qs:
            # We are in case (I) or (II), thus we can reduce
            # the search space with the prefix cached_qs[0].query.
            query_dict.update({
                'qc': cached_qs[0].query})
        if cached_qs and cached_qs[0].query == query:
            # We are in case (I).
            # The following is equivalent to
            #  q = cached_qs[0]
            #  q.last_used = time.time()
            #  q.times_used += 1
            #  self.cQueries.save(q.to_dict())
            self.cQueries.update(
                {'_id': cached_qs[0].query},
                {'$inc': {'t': 1},        # times_used
                 '$set': {'l': time.time()}}) # last_used
            ret = [MongoMedia(self, d)
                for d in self.cMedia.find(query_dict, skip=skip,
                    limit=(0 if count is None else count),
                    sort=[('s', 1)])]
            time_spent = time.time() - start_time
            self.l.debug("query %s directly from cache; "+
                    "%s results; %s seconds",
                    repr(query), len(ret), time_spent)
            return ret
        if cached_qs:
            # We are in case (II).
            # The following is equivalent to
            #  q = cached_qs[0]
            #  q.last_used_indirectly = time.time()
            #  q.times_used_indirectly += 1
            #  self.cQueries.save(q.to_dict())
            self.cQueries.update(
                {'_id': cached_qs[0].query},
                {'$inc': {'T': 1},        # times_used_ind.
                 '$set': {'L': time.time()}}) # last_used_ind.
        # We are in case (II) or (III).
        if ((cached_qs and cached_qs[0].query != query
                and cached_qs[0].nMatches
                    >= self.queryCacheMinSearch) or
                    not cached_qs):
            # We need to search through a lot of results.  Thus we
            # are going to cache this query.
            self.cMedia.update(query_dict,
                    {'$push': {'qc': query}}, multi=True)
            nMatches = self.cMedia.find({'qc': query}).count()
            if qs and qs[0].query == query:
                q = qs[0]
                q.last_used = time.time()
                q.times_used += 1
                q.is_cached = True
                q.nMatches = nMatches
            else:
                q = MongoQuery({
                    '_id': query,
                    'last_used': time.time(),
                    'times_used': 1,
                    'nMatches': nMatches,
                    'last_used_indirectly': None,
                    'times_used_indirectly': 0,
                    'is_cached': True})
            self.cQueries.save(q.to_dict())
            ret = [MongoMedia(self, d)
                for d in self.cMedia.find({'qc': query},
                    skip=skip, limit=(0 if count is None
                            else count),
                    sort=[('s', 1)])]
            time_spent = time.time() - start_time
            self.l.debug("query %s new in cache; used %s; "+
                    "%s results; %s seconds",
                    repr(query), repr(cached_qs[0].query
                        if cached_qs else ''),
                    len(ret), time_spent)
            return ret
        ret = [MongoMedia(self, d) for d in
                self.cMedia.find(query_dict, skip=skip,
                    limit=(0 if count is None else count),
                    sort=[('s', 1)])]
        if qs and qs[0].query == query:
            q = qs[0]
            q.last_used = time.time()
            q.times_used += 1
        else:
            q = MongoQuery({
                '_id': query,
                'last_used': time.time(),
                'times_used': 1,
                'last_used_indirectly': None,
                'times_used_indirectly': 0,
                'is_cached': False})
        self.cQueries.save(q.to_dict())
        time_spent = time.time() - start_time
        self.l.debug("query %s; used %s; "+
                "%s results; %s seconds",
                repr(query), repr(cached_qs[0].query
                    if cached_qs else ''),
                len(ret), time_spent)
        return ret

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
        self.cHistory.ensure_index('m')
        self.cHistory.ensure_index('b')
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
    # The interesting part
    def pick(self):
        ret = self.collection._pick_random_media()
        if ret is None:
            self.l.info("Waiting on collection.got_media_event")
            self.collection.got_media_event.wait()
            ret = self.collection._pick_random_media()
        return ret

    # The part to make sure everything works fine when at first
    # there is no media and then some media is added.
    # TODO properly handle the case that a collection is changed
    #      from one with media to one without media
    def __init__(self, *args, **kwargs):
        super(MongoSimpleRandom, self).__init__(*args, **kwargs)
        self._media_waiter_running = False
        self._media_waiter_lock = threading.Lock()
    @property
    def ready(self):
        with self._media_waiter_lock:
            ret = self.collection.got_media_event.is_set()
            if not ret and not self._media_waiter_running:
                self.threadPool.execute_named(
                    self._run_media_waiter,
                    '%s _run_media_waiter' % self.l.name)
                self._media_waiter_running = True
            return ret

    def _run_media_waiter(self):
        self.l.debug('_run_media_waiter: waiting on got_media_event')
        self.collection.got_media_event.wait()
        self.l.debug('_run_media_waiter:  woke!')
        self.on_ready()
        with self._media_waiter_lock:
            self._media_waiter_running = False
