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
