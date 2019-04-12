'''
Created on Jul 10, 2011

Implements the Xoffset operator.
The intermediate results are represented in a queue. 

@author: Maribel Acosta Deibe
'''
from multiprocessing import Queue


class Xoffset(object):
    
    def __init__(self, vars, offset):
        self.input = Queue()
        self.qresults = Queue()
        self.vars = vars
        self.offset = int(offset)
        
    def execute(self, left, dummy, out, processqueue=Queue()):
        # Executes the Xoffset.
        self.left = left
        self.qresults = out
        tuple = self.left.get(True)
        count = 0
        
        # OFFSET
        while (count < self.offset) and (tuple != "EOF"):
            count = count + 1
            tuple = self.left.get(True)
            
        # Producing the remaining results. 
        while tuple != "EOF":
            self.qresults.put(tuple)
            tuple = self.left.get(True)
            
        # Put EOF in queue and exit. 
        self.qresults.put("EOF")
        processqueue.put("EOF")
        p = processqueue.get()
        import os
        while p != 'EOF':
            try:
                os.kill(p, 9)
            except Exception as ex:
                pass
            except:
                pass
            p = processqueue.get()
        return

