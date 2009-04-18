from __future__ import with_statement

import gst
import gtk
import pygst
import threading
import gobject

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
			bus = self.bin.get_bus()
			bus.add_signal_watch()
			bus.connect('message', self.on_message)
			bus.connect('message::tag', self.on_tag)
			self.bin.set_property('uri', 'file://'+path)
			self.bin.set_state(gst.STATE_PAUSED)
			states = self.bin.get_state(gst.CLOCK_TIME_NONE)
			assert any(map(lambda x: isinstance(x, gst.State) and 
					x == gst.STATE_PAUSED, states))
			raw = self.bin.query_duration(
					gst.format_get_by_nick('time'))[0]
			self.result['length'] = raw/1000000000.0
			self.event.set()
		
		def on_message(self, bus, message):
			t = message.type
			if t == gst.MESSAGE_ERROR:
				self.bin.set_state(gst.STATE_NULL)
				error, debug = message.parse_error()
				self.mi.l.error("Gst: %s %s" % (error, debug))
				self.inError = True
				self.event.set()
		
		def on_tag(self, bus, message):
			tagList = message.parse_tag()
			for key in tagList.keys():
				if key == 'artist':
					self.result['artist'] = tagList[key]
				elif key == 'title':
					self.result['title'] = tagList[key]

		def interrupt(self):
			self.inError = True
			self.event.set()
	
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
