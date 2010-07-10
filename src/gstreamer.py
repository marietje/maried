from __future__ import with_statement

import gst
import gtk
import time
import pygst
import os.path
import gobject
import datetime
import threading

from maried.core import Player, MediaInfo, Stopped
from mirte.core import Module
from sarah.event import Event
from sarah.io import SocketPairWrappedFile

class GtkMainLoop(Module):
	def run(self):
		gtk.gdk.threads_init()
		gtk.main()
	def stop(self):
		gtk.main_quit()

class GstMediaInfo(MediaInfo):
	class Job(object):
		def __init__(self, mi, uri):
			self.mi = mi
			self.event = threading.Event()
			self.inError = False
			self.result = dict()
			self.bin = gst.element_factory_make(
					'playbin', 'playbin')
			fakesink = gst.element_factory_make(
					'fakesink', 'fakesink')
			rganalysis = gst.element_factory_make(
					'rganalysis', 'rganalysis')
			bin2 = gst.element_factory_make('bin', 'bin')
			bin2.add(rganalysis)
			bin2.add(fakesink)
			rganalysis.link(fakesink)
			bin2.add_pad(gst.GhostPad('ghostpad',
					rganalysis.get_static_pad('sink')))
			self.bin.set_property('video-sink', fakesink)
			self.bin.set_property('audio-sink', bin2)
			self.bus = bus = self.bin.get_bus()
			bus.add_signal_watch()
			bus.connect('message', self.on_message)
			bus.connect('message::tag', self.on_tag)
			self.bin.set_property('uri', uri)
			self.bin.set_state(gst.STATE_PLAYING)
		
		def on_message(self, bus, message):
			t = message.type
			if t == gst.MESSAGE_ERROR:
				error, debug = message.parse_error()
				self.mi.l.error("Gst: %s %s" % (error, debug))
				self.inError = True
				self.finish()
			elif t == gst.MESSAGE_EOS:
				self.on_eos()

		def on_eos(self):
			rawpos = self.bin.query_position(
					gst.FORMAT_TIME)[0]
			try:
				rawdur = self.bin.query_duration(
					gst.FORMAT_TIME)[0]
			except gst.QueryError:
				rawdur = -1
			if rawdur == -1:	
				self.mi.l.warn('query_duration failed, '+
					'falling back to query_position')
				raw = rawpos
			else:
				raw = rawdur
			self.result['length'] = raw / (1000.0**3)
			self.finish()
		
		def on_tag(self, bus, message):
			tagList = message.parse_tag()
			for key in tagList.keys():
				if key == 'artist':
					self.result[key] = tagList[key]
				elif key == 'title':
					self.result[key] = tagList[key]
				elif key == gst.TAG_TRACK_PEAK:
					self.result['trackPeak'] = tagList[key]
				elif key == gst.TAG_TRACK_GAIN:
					self.result['trackGain'] = tagList[key]

		def interrupt(self):
			self.inError = True
			self.finish()

		def finish(self):
			self.event.set()
			self.bin.set_state(gst.STATE_NULL)
			self.bus.remove_signal_watch()
			del(self.bin)
			del(self.bus)
	
	def __init__(self, settings, logger):
		super(GstMediaInfo, self).__init__(settings, logger)
		self.lock = threading.Lock()
		self.jobs = set()
	
	def get_info(self, stream):
		wrapper = None
		if hasattr(stream, 'fileno'):
			uri = 'fd://%s' % stream.fileno()
		else:
			wrapper = SocketPairWrappedFile(stream)
			uri = 'fd://%s' % wrapper.fileno()
			self.threadPool.execute(wrapper.run)
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
	def __init__(self, settings, logger):
		super(GstPlayer, self).__init__(settings, logger)
		self.bin = gst.element_factory_make('playbin', 'playbin')
		self.bin2 = gst.element_factory_make('bin', 'bin')
		self.ac = gst.element_factory_make('audioconvert',
				'audioconvert')
		self.ac2 = gst.element_factory_make('audioconvert',
				'audioconvert2')
		self.rgvolume = gst.element_factory_make('rgvolume', 'rgvolume')
		self.autoaudiosink = gst.element_factory_make('autoaudiosink',
					'autoaudiosink')
		self.bin2.add(self.rgvolume)
		self.bin2.add(self.ac)
		self.bin2.add(self.ac2)
		self.bin2.add(self.autoaudiosink)
		self.ac.link(self.rgvolume)
		self.rgvolume.link(self.ac2)
		self.ac2.link(self.autoaudiosink)
		self.bin2.add_pad(gst.GhostPad('ghostpad',
			self.ac.get_static_pad('sink')))
		self.bin.set_property('audio-sink', self.bin2)
		self.bus = self.bin.get_bus()
		self.bus.add_signal_watch()
		self.bus.connect('message', self.on_message)
		self.idleCond = threading.Condition()
		self.idle = True
		self.stopped = False

	def play(self, media):
		with self.idleCond:
			if self.stopped:
				raise Stopped
			if not self.idle:
				self.l.warn("Waiting on idleCond")
				self.idleCond.wait()
			if self.stopped:
				raise Stopped
			self.idle = False
		try:
			self._play(media)
		except Exception:
			with self.idleCond:
				self.idleCond.notifyAll()
			self.idle = True
			raise

	def _play(self, media):
		self.endTime = datetime.datetime.fromtimestamp(
				time.time() + media.length)
		try:
			mf = media.mediaFile
		except KeyError:
			self.l.error("%s's mediafile doesn't exist" % media)
			return
		self.l.info("Playing %s" % media)
		stream = mf.open()
		wrapper = None
		if hasattr(stream, 'fileno'):
			uri = 'fd://%s'%stream.fileno()
		else:
			wrapper = SocketPairWrappedFile(stream)
			self.threadPool.execute(wrapper.run)
			uri = 'fd://%s'%wrapper.fileno()
		self.bin.set_property('uri', uri)
		tl = gst.TagList()
		tl[gst.TAG_TRACK_GAIN] = media.trackGain
		tl[gst.TAG_TRACK_PEAK] = media.trackPeak
		self.rg_event = gst.event_new_tag(tl)
		self.bin.set_state(gst.STATE_PLAYING)
		with self.idleCond:
			self.idleCond.wait()
		if not wrapper is None:
			wrapper.close()
		if hasattr(stream, 'close'):
			stream.close()
	
	def _reset(self):
		self.bin.set_state(gst.STATE_NULL)
		with self.idleCond:
			self.idle = True
			with self.idleCond:
				self.idleCond.notifyAll()
	
	def on_message(self, bus, message):
		if message.type == gst.MESSAGE_ERROR:
			error, debug = message.parse_error()
			self.l.error("Gst: %s %s" % (error, debug))
			self._reset()
		elif (not self.rg_event is None and
				message.type == gst.MESSAGE_STATE_CHANGED and
				message.src == self.rgvolume and
				message.parse_state_changed()[1] ==
					gst.STATE_PAUSED):
			self.ac.get_static_pad('src').push_event(
					self.rg_event)
			self.rg_event = None
			tg = self.rgvolume.get_property('target-gain')
			rg = self.rgvolume.get_property('result-gain')
			if tg != rg:
				self.l.warn('replaygain: target gain '+
					'not reached: trg %s res %s' % (
						tg, rg))
		elif message.type == gst.MESSAGE_EOS:
			self._reset()
	
	def stop(self):
		self._reset()
		self.bus.remove_signal_watch()
		with self.idleCond:
			if not self.idle:
				self.idleCond.wait()
			self.stopped = True
		del(self.bin)
		del(self.bus)
