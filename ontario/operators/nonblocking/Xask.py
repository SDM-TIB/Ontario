from multiprocessing import Queue


class Xask(object):

    def __init__(self, vars):
        self.input = Queue()
        self.qresults = Queue()
        self.vars = vars

    def execute(self, left, dummy, out, processqueue=Queue()):
        # Executes the Xproject.
        self.left = left
        self.qresults = out
        tuple = self.left.get(True)
        if tuple == 'EOF':
            self.qresults.put(False)
            self.qresults.put("EOF")
            return
        else:
            self.qresults.put(True)
            self.qresults.put("EOF")
            return
