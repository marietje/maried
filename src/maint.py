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
                media.length = info['length']
            ok = False
        if (info['track_peak'], info['trackGain']) != (
                media.trackPeak, media.trackGain):
            print "wrong gain/peak: %s != %s for %s" % (
                (info['trackGain'], info['trackPeak']),
                (media.trackPeak, media.trackGain), media)
            if fix:
                media.trackGain = info['trackGain']
                media.trackPeak = info['trackPeak']
            ok = False
        if fix and not ok:
            media.save()
