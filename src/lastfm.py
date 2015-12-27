from core import Module
from urllib2 import URLError
import threading
import scrobbler
import time

class Scrobbler(Module):
    def __init__(self, *args, **kwargs):
        super(Scrobbler, self).__init__(*args, **kwargs)
        self.desk.on_playing_changed.register(self._on_playing_changed)
        self.register_on_setting_changed('username', self.osc_creds)
        self.register_on_setting_changed('password', self.osc_creds)
        self.cond = threading.Condition()
        self.running = True
        self.authenticated = False
        self.queue = list()
        self.osc_creds()
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

    def _on_playing_changed(self, previous_playing):
        with self.cond:
            self.queue.append(previous_playing)
            self.cond.notify()
    def scrobble(self, media, end_time):
        if not self.authenticated:
            return
        if media.length <= 30:
            self.l.info("%s is too short to be scrobbled" % media)
            return
        time_played = (media.length + time.time() -
                time.mktime(end_time.timetuple()))
        if time_played < 240 and time_played < media.length * 0.5:
            self.l.info("%s has not played long enough" % media)
            return
        scrobbler.submit(media.artist, media.title,
            int(time.mktime(end_time.timetuple()) - media.length),
            length=int(media.length))
        scrobbler.flush()
    def run(self):
        self.cond.acquire()
        while self.running:
            playing = self.desk.get_playing()[0]
            self.cond.release()
            try:
                scrobbler.now_playing(playing.artist,
                              playing.title,
                              length=int(playing.length))
            except URLError as e:
                self.l.exception("Error while scrobbler.now_playing")
            self.cond.acquire()
            while len(self.queue) > 0:
                m, r, end_time = self.queue.pop()
                if not r is None:
                    self.cond.release()
                    try:
                        self.scrobble(m, end_time)
                    except URLError as e:
                        self.l.exception("Error "+
                            "while scrobbler.submit")
                    self.cond.acquire()
            if not self.running:
                break
            self.cond.wait()
        self.cond.release()
    def stop(self):
        self.running = False
        with self.cond:
            self.cond.notify()
