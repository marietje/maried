import time

def sanitize_collection_info(collection, fix=False):
	for media in collection.media:
		info = media.mediaFile.get_info()
		ok = True
		if info['length'] != media.length:
			print "wrong length: %s != %s for %s" % (
					info['length'],
					media.length, media)
			if fix:
				media.length = length
			ok = False
		if (info['track_peak'], info['track_gain']) != (
				media.trackPeak, media.trackGain):
			print "wrong gain/peak: %s != %s for %s" % (
				(info['track_peak'], info['track_gain']),
				(media.trackPeak, media.trackGain), media)
			if fix:
				media.trackGain = info['track_gain']
				media.trackPeak = info['track_peak']
			ok = False
		if fix and not ok:
			media.save()
