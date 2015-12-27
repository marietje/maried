# gst, gobject and gi are imported in GtkMainLoop.run
gi, gst, gobject = None, None, None

import time
import os.path
import datetime
import threading

from maried.core import Player, MediaInfo, Stopped
from mirte.core import Module
from sarah.event import Event
from sarah.io import SocketPairWrappedFile

class GtkMainLoop(Module):
    def __init__(self, *args, **kwargs):
        super(GtkMainLoop, self).__init__(*args, **kwargs)
        self.ready = threading.Event()
    def run(self):
        self.do_imports()
        gobject.threads_init()
        gst.init()
        self.loop = gobject.MainLoop()
        self.ready.set()
        self.loop.run()
    def do_imports(self):
        global gi, gst, gobject
        import gi as _gi
        _gi.require_version('Gst', '1.0')
        from gi.repository import GObject as _gobject
        from gi.repository import Gst as _gst
        gst, gobject, gi = _gst, _gobject, _gi
    def stop(self):
        self.loop.quit()

class GstMediaInfo(MediaInfo):
    class Job(object):
        def __init__(self, mi, uri):
            self.mi = mi
            self.event = threading.Event()
            self.inError = False
            self.result = dict()
            self.bin = gst.ElementFactory.make(
                    'playbin', 'playbin')
            fakesink = gst.ElementFactory.make(
                    'fakesink', 'fakesink')
            rganalysis = gst.ElementFactory.make(
                    'rganalysis', 'rganalysis')
            bin2 = gst.ElementFactory.make('bin', 'bin')
            bin2.add(rganalysis)
            bin2.add(fakesink)
            rganalysis.link(fakesink)
            bin2.add_pad(gst.GhostPad.new('sink',
                    rganalysis.get_static_pad('sink')))
            self.bin.set_property('video-sink', fakesink)
            self.bin.set_property('audio-sink', bin2)
            self.bus = bus = self.bin.get_bus()
            bus.add_signal_watch()
            bus.connect('message', self.on_message)
            bus.connect('message::tag', self.on_tag)
            self.bin.set_property('uri', uri)
            if (self.bin.set_state(gst.State.PLAYING)
                    == gst.StateChangeReturn.FAILURE):
                self.mi.l.warn('set_state failed')
        
        def on_message(self, bus, message):
            t = message.type
            if t == gst.MessageType.ERROR:
                error, debug = message.parse_error()
                self.mi.l.error("Gst: %s %s" % (error, debug))
                self.inError = True
                self.finish()
            elif t == gst.MessageType.EOS:
                self.on_eos()

        def on_eos(self):
            rawpos = self.bin.query_position(
                    gst.Format.TIME)[1]
            try:
                ok, rawdur = self.bin.query_duration(
                    gst.Format.TIME)
            except gst.QueryError:
                rawdur = -1
            if not ok or rawdur == -1:
                self.mi.l.warn('query_duration failed, '+
                    'falling back to query_position')
                raw = rawpos
            else:
                raw = rawdur
            self.result['length'] = raw / (1000.0**3)
            self.finish()
        
        def on_tag(self, bus, message):
            tagList = message.parse_tag()
            for n in xrange(tagList.n_tags()):
                key = tagList.nth_tag_name(n)
                if key in ('artist', 'title'):
                    self.result[key] = tagList.get_string(key)[1].decode('utf-8')
                elif key == gst.TAG_TRACK_PEAK:
                    self.result['trackPeak'] = tagList.get_double(key)[1]
                elif key == gst.TAG_TRACK_GAIN:
                    self.result['trackGain'] = tagList.get_double(key)[1]

        def interrupt(self):
            self.inError = True
            self.finish()

        def finish(self):
            self.event.set()
            if (self.bin.set_state(gst.State.NULL)
                    == gst.StateChangeReturn.FAILURE):
                self.mi.l.warn('set_state failed')
            self.bus.remove_signal_watch()
            del(self.bin)
            del(self.bus)
    
    def __init__(self, *args, **kwargs):
        super(GstMediaInfo, self).__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self.jobs = set()
    
    def get_info(self, stream):
        wrapper = None
        if hasattr(stream, 'fileno'):
            uri = 'fd://%s' % stream.fileno()
        else:
            wrapper = SocketPairWrappedFile(stream)
            uri = 'fd://%s' % wrapper.fileno()
            self.threadPool.execute_named(wrapper.run,
                    '%s wrapper.run %s' % (self.l.name,
                        wrapper.fileno()))
        j = GstMediaInfo.Job(self, uri)
        with self.lock:
            self.jobs.add(j)
        j.event.wait()
        with self.lock:
            self.jobs.remove(j)
        if not wrapper is None:
            wrapper.close()
        if j.inError:
            raise ValueError
        return j.result

    def get_info_by_path(self, path):
        uri = 'file://' + os.path.abspath(path)
        j = GstMediaInfo.Job(self, uri)
        with self.lock:
            self.jobs.add(j)
        j.event.wait()
        with self.lock:
            self.jobs.remove(j)
        if j.inError:
            raise ValueError
        return j.result

class GstPlayer(Player):
    def __init__(self, *args, **kwargs):
        super(GstPlayer, self).__init__(*args, **kwargs)
        self.readyEvent = threading.Event()
        self.idleCond = threading.Condition()

    def run(self):
        self._initialize()

    def _initialize(self):
        self.gtkMainLoop.ready.wait()
        
        # set up the player bin
        self.bin = gst.ElementFactory.make('playbin', 'playbin')
        self.bin2 = gst.ElementFactory.make('bin', 'bin')
        self.ac = gst.ElementFactory.make('audioconvert',
                'audioconvert')
        self.ac2 = gst.ElementFactory.make('audioconvert',
                'audioconvert2')
        self.rgvolume = gst.ElementFactory.make('rgvolume', 'rgvolume')
        self.rglimiter = gst.ElementFactory.make('rglimiter', 'rglimiter')
        self.autoaudiosink = gst.ElementFactory.make('autoaudiosink',
                    'autoaudiosink')

        self.bin2.add(self.rgvolume)
        self.bin2.add(self.ac)
        self.bin2.add(self.ac2)
        self.bin2.add(self.rglimiter)
        self.bin2.add(self.autoaudiosink)
        self.ac.link(self.rgvolume)
        self.rgvolume.link(self.ac2)
        self.rgvolume.link(self.rglimiter)
        self.ac2.link(self.autoaudiosink)
        self.bin2.add_pad(gst.GhostPad.new('sink',
            self.ac.get_static_pad('sink')))
        self.bin.set_property('audio-sink', self.bin2)
        self.bus = self.bin.get_bus()
        self.bus.add_signal_watch()
        self.bus.connect('message', self.on_message)
        self.bin.connect('about-to-finish', self._on_about_to_finish)

        # internal state
        self.idle = True
        self.stopped = False
        self.playing_media = None
        self.next_media = None
        self.next_wrapper = None
        self.next_stream = None
        self.previous_wrapper = None
        self.previous_stream = None
        self.in_a_skip = False
        self.readyEvent.set()

    def queue(self, media):
        """ Queues the next media to be played """
        # Check current state
        self.readyEvent.wait()
        got_to_start_playing = False
        with self.idleCond:
            if self.stopped:
                raise Stopped
            if not self.next_media is None:
                raise RuntimeError, \
                    "Another media has been queued already"
            if self.playing_media is None:
                got_to_start_playing = True
            self.next_media = media

        # Get mediafile and set up file descriptor
        self.l.debug('queueing %s' % media)
        try:
            mf = media.mediaFile
        except KeyError:
            self.l.error("%s's mediafile doesn't exist" % media)
            return
        stream = mf.open()
        wrapper = None
        if hasattr(stream, 'fileno'):
            uri = 'fd://%s'%stream.fileno()
        else:
            wrapper = SocketPairWrappedFile(stream)
            self.threadPool.execute_named(wrapper.run,
                '%s wrapper.run %s' % (self.l.name,
                    wrapper.fileno()))
            uri = 'fd://%s'%wrapper.fileno()
        self.next_wrapper = wrapper
        self.next_stream = stream

        # Queue in the playbin
        self.bin.set_property('uri', uri)

        # Inject TRACK_GAIN and TRACK_PEAK tags
        tl = gst.TagList.new_empty()
        tl.add_value(gst.TagMergeMode.APPEND, gst.TAG_TRACK_GAIN,
                media.trackGain)
        tl.add_value(gst.TagMergeMode.APPEND, gst.TAG_TRACK_PEAK,
                media.trackPeak)
        self.rg_event = gst.Event.new_tag(tl)

        # Start playing -- if not already
        if got_to_start_playing or self.in_a_skip:
            if (self.bin.set_state(gst.State.PLAYING)
                    == gst.StateChangeReturn.FAILURE):
                self.mi.l.warn('set_state failed')
            self.idle = False

    def _on_stream_changed(self):
        """ Called when GStreamer signals that the playing stream
            has changed.  In this method we will clean up the previous
            stream (if any) and let the world know the playing
            media changed """
        now = time.time()
        
        self.l.info("playing: %s"% self.next_media)

        if self.rg_event:
            self.ac.get_static_pad('src').push_event(self.rg_event)
            self.rg_event = None
            tg = self.rgvolume.get_property('target-gain')
            rg = self.rgvolume.get_property('result-gain')
            if tg != rg:
                self.l.warn('replaygain: trg. gain not reached: trg %s res %s'
                                % (tg, rg))

        # Get old end-time and playing media
        old_endTime = self.endTime
        old_media = self.playing_media
        
        self._on_media_finished()

        # Update state
        self.playing_media, self.next_media = self.next_media, None
        self.endTime = datetime.datetime.fromtimestamp(
                now + self.playing_media.length)
        self.previous_stream, self.next_stream = self.next_stream, None
        self.previous_wrapper, self.next_wrapper = \
                        self.next_wrapper, None
        if self.in_a_skip:
            self.in_a_skip = False

        # Notify the world
        # if not old_media is None:
        #     self.on_playing_finished(old_media, old_endTime)
        self.on_playing_started(self.playing_media, self.endTime)

    def _on_about_to_finish(self, bin):
        self.l.info('about to finish')
        self.on_about_to_finish()

    def _on_eos(self):
        with self.idleCond:
            if not self.next_media is None:
                return
            if (self.bin.set_state(gst.State.NULL)
                    == gst.StateChangeReturn.FAILURE):
                self.mi.l.warn('set_state failed')
            self._on_media_finished()
            self.idle = True
            self.idleCond.notifyAll()

    def _on_media_finished(self):
        if not self.previous_wrapper is None:
            self.previous_wrapper.close()
        if hasattr(self.previous_stream, 'close'):
            self.previous_stream.close()

        # Update state
        old_playing = self.playing_media
        old_endTime = self.endTime
        self.playing_media = None
        self.previous_stream = None
        self.previous_wrapper = None
        self.endTime = None

        # Notify the world
        if not old_playing is None:
            self.on_playing_finished(old_playing, old_endTime)

    def skip(self):
        with self.idleCond:
            if self.in_a_skip:
                raise RuntimeError, "Already skipping"
            self.l.debug('Skipping')
            self.in_a_skip = True
            if (self.bin.set_state(gst.State.NULL)
                    == gst.StateChangeReturn.FAILURE):
                self.mi.l.warn('set_state failed')
            self.on_about_to_finish()

    def _interrupt(self):
        with self.idleCond:
            self.l.debug('_interrupt!')
            if (self.bin.set_state(gst.State.NULL)
                    == gst.StateChangeReturn.FAILURE):
                self.mi.l.warn('set_state failed')
            self._on_media_finished()
            self.idle = True
            self.idleCond.notifyAll()
    
    def on_message(self, bus, message):
        if message.type == gst.MessageType.ERROR:
            error, debug = message.parse_error()
            self.l.error("Gst: %s %s" % (error, debug))
            self._on_eos()
        elif message.type == gst.MessageType.STREAM_START:
            self._on_stream_changed()
        elif message.type == gst.MessageType.EOS:
            self._on_eos()

    
    def stop(self):
        self.readyEvent.wait()
        self._interrupt()
        self.bus.remove_signal_watch()
        with self.idleCond:
            if not self.idle:
                self.idleCond.wait()
            self.stopped = True
        del(self.bin)
        del(self.bus)
