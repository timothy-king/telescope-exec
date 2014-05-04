try:
    import progressbar
    progressbar_loaded = True
except ImportError:
    progressbar_loaded = False

import threading

class WrappedProgressBar:
    """Set up the command-line progress bar with max_value
    Wraps the progressbar.py tool to avoid LGPL'ing this code
    """
    def __init__(self, max_value):
        self.max = max_value
        self.done = 0
        self.lock = threading.Lock()
        if progressbar_loaded:
            foo = [progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()]
            self.bar = progressbar.ProgressBar(maxval=max_value, widgets=foo)

    def increment(self):
        with self.lock:
            self.done += 1
            if progressbar_loaded:
                self.bar.update(self.done)
            else:
                print "\t", self.done, "/", self.max
                self.update()

    def finish(self):
        with self.lock:
            if progressbar_loaded:
                self.bar.finish()
