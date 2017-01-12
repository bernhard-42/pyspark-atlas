import time

def atlasNewGuid():
    return str(-int(time.time() * 1000000))
