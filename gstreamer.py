from __future__ import with_statement

import gst
import gtk
import time
import pygst
import gobject
import datetime
import threading

from maried.core import Module, Event, Player, MediaInfo

class GtkMainLoop(Module):
	def run(self):
		gtk.gdk.threads_init()
		gtk.main()
	def stop(self):
		gtk.main_quit()

class GstMediaInfo(MediaInfo):
	class Job(object):
		def __init__(self, mi, path):
			self.mi = mi
			self.event = threading.Event()
			self.inError = False
			self.result = dict()
			self.bin = gst.element_factory_make(
					'playbin', 'playbin')
			fakesink = gst.element_factory_make(
					'fakesink', 'fakesink')
			self.bin.set_property('video-sink', fakesink)
			self.bin.set_property('audio-sink', fakesink)
			self.bus = bus = self.bin.get_bus()
			bus.add_signal_watch()
			bus.connect('message', self.on_message)
			bus.connect('message::tag', self.on_tag)
			self.bin.set_property('uri', 'file://'+path)
			self.bin.set_state(gst.STATE_PLAYING)
		
		def on_message(self, bus, message):
			t = message.type
			if t == gst.MESSAGE_ERROR:
				error, debug = message.parse_error()
				self.mi.l.error("Gst: %s %s" % (error, debug))
				self.inError = True
				self.finish()
			elif t == gst.MESSAGE_EOS:
				raw = self.bin.query_duration(
						gst.FORMAT_TIME)[0]
				self.result['length'] = raw / (1000.0**3)
				self.finish()
		
		def on_tag(self, bus, message):
			tagList = message.parse_tag()
			for key in tagList.keys():
				if key == 'artist':
					self.result[key] = tagList[key]
				elif key == 'title':
					self.result[key] = tagList[key]

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

	def get_info_by_path(self, path):
		j = GstMediaInfo.Job(self, path)
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
		self.bus = self.bin.get_bus()
		self.bus.add_signal_watch()
		self.bus.connect('message', self.on_message)
		self.idleCond = threading.Condition()
		self.idle = True
		self.stopped = False

	def play(self, media):
		with self.idleCond:
			if self.stopped:
				return
			if not self.idle:
				self.l.warn("Waiting on idleCond")
				self.idleCond.wait()
			if self.stopped:
				return
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
		self.bin.set_property('uri', 
			"file:///"+mf.get_named_file())
		self.bin.set_state(gst.STATE_PLAYING)
		with self.idleCond:
			self.idleCond.wait()
	
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
