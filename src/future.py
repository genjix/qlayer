from Queue import Queue

class Future:

    def __init__(self):
        self.syncq = Queue()

    def __call__(self, *args):
        self.syncq.put(args)

    def get(self):
        return self.syncq.get()

