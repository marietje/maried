import time

def sanitize_collection_info(collection, fix=False):
	for media in collection.media:
		length = media.mediaFile.get_info()['length']
		if length != media.length:
			print "wrong length: %s != %s for %s" % (
					length,
					media.length, media)
			if fix:
				media.length = length
				media.save()
