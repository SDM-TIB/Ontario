'''
NestedHashOptionalFilter.py

Implements a depending operator, similar to block nested join and symmetric
hash join

Autor: Maribel Acosta
Autor: Maria-Esther Vidal
Autor: Gabriela Montoya
Date: January 29th, 2014

'''
from multiprocessing import Queue
from time import time
from multiprocessing.queues import Empty
from ontario.operators.Optional import Optional
from ontario.operators.nonblocking.NHJFOperatorStructures import Record


WINDOW_SIZE = 10


class NestedHashOptionalFilter(Optional):

    def __init__(self, vars_left, vars_right):
        self.left_table = dict()
        self.right_table = dict()
        self.qresults = Queue()
        self.bag = []
        self.vars_left = set(vars_left)
        self.vars_right = set(vars_right)
        self.vars = list(self.vars_left & self.vars_right)

    def instantiate(self, d):
        newvars_left = self.vars_left - set(d.keys())
        newvars_right = self.vars_right - set(d.keys())
        return NestedHashOptionalFilter(newvars_left, newvars_right)

    def instantiateFilter(self, d, filter_str):
        newvars_left = self.vars_left - set(d)
        newvars_right = self.vars_right - set(d)
        return NestedHashOptionalFilter(newvars_left, newvars_right)

    def execute(self, left_queue, right_operator, out, processqueue=Queue()):
        #print "execute NestedHashOptionalFilter"
        self.left_queue = left_queue
        self.right_operator = right_operator
        self.qresults = out

        tuple1 = None
        tuple2 = None
        right_queues = dict()
        filter_bag = []
        count = 0
        while (not(tuple1 == "EOF") or (len(right_queues) > 0)):

            try:
                tuple1 = self.left_queue.get(False)
                #print "tuple1", tuple1
                # Try to get and process tuple from left queue
                if not(tuple1 == "EOF"):
                    #tuple1 = self.left_queue.get(False)
                    #print "tuple1: "+str(tuple1)
                    self.bag.append(tuple1)
                    instance = self.probeAndInsert1(tuple1, self.right_table, self.left_table, time())
                    #print "sali de probe and insert 1 con tuple", tuple1
                    if instance: # the join variables have not been used to instanciate the right_operator
                        filter_bag.append(tuple1)
                    #print "filter_bag", len(filter_bag)

                    if len(filter_bag) >= WINDOW_SIZE:
                        new_right_operator = self.makeInstantiation(filter_bag,  self.right_operator)
                        #print "Here in makeInstantation with filter"
                        #resource = self.getResource(tuple1)
                        queue = Queue()
                        right_queues[count] = queue
                        new_right_operator.execute(queue)
                        filter_bag = []
                        count = count + 1
            
                else:
                    if len(filter_bag) > 0:
                        #print "here", len(filter_bag), filter_bag
                        new_right_operator = self.makeInstantiation(filter_bag, self.right_operator)
                        #resource = self.getResource(tuple1)
                        queue = Queue()
                        right_queues[count] = queue
                        new_right_operator.execute(queue)
                        filter_bag = []
                        count = count + 1

            except Empty:
                pass
            except Exception as e:
                #print "Unexpected error:", sys.exc_info()[0]
                #print e
                pass

            toRemove = [] # stores the queues that have already received all its tuples
            #print "right_queues", right_queues
            for r in right_queues:
                try:
                    q = right_queues[r]
                    tuple2 = None
                    while tuple2 != "EOF":
                        tuple2 = q.get(False)
                        
                        if tuple2 == "EOF":
                            toRemove.append(r)
                        else:
                            resource = self.getResource(tuple2)
                            for v in self.vars:
                                del tuple2[v]
                            #print "new tuple2", tuple2
                            self.probeAndInsert2(resource, tuple2, self.left_table, self.right_table, time())
                except Exception:
                    # This catch:
                    # Empty: in tuple2 = self.right.get(False), when the queue is empty.
                    # TypeError: in att = att + tuple[var], when the tuple is "EOF".
                    #print "Unexpected error:", sys.exc_info()
                    pass

            for r in toRemove:
                del right_queues[r]

        # This is the optional: Produce tuples that haven't matched already.    
        for tuple in self.bag:
            res_right = {}
            for var in self.vars_right:
                res_right.update({var: ''})
            res = res_right
            res.update(tuple)
            self.qresults.put(res)

        # Put EOF in queue and exit.
        self.qresults.put("EOF")
        return

    def getResource(self, tuple):
        resource = ''
        for var in self.vars:
            # val = tuple[var]
            # if "^^<" in val:
            #     val = val[:val.find('^^<')]
            #
            # resource = resource + val

            if var in tuple:
                val = tuple[var]
                # if isinstance(val, dict) and 'value' in val:
                #     val = val['value']

                suffix = ''
                # if 'datatype' in val:
                #     if isinstance(val['datatype'], bytes):
                #         suffix = "^^<" + val['datatype'].decode('utf-8') + ">"
                #     else:
                #         suffix = "^^<" + val['datatype'] + ">"
                #
                if isinstance(val, dict):

                    if "xml:lang" in val:
                        suffix = '@' + val['xml:lang']
                    try:
                        if isinstance(val['value'], bytes):
                            val = val['value'].decode('utf-8') + suffix
                        else:
                            val = val['value'] + suffix
                    except:
                        val = val['value'] + suffix
                # if "^^<" in val:
                #     val = val[:val.find('^^<')]
                resource = resource + str(val)

        return resource

    def makeInstantiation(self, filter_bag, operator):
        filter_str = ''
        filter_str = " . ".join(map(str, operator.tree.service.filters))
        new_vars = ['?'+v for v in self.vars] #TODO: this might be $
        #print "operator type ",operator
        #print "making instantiation join filter", filter_bag 
        # When join variables are more than one: FILTER ( )
        if len(self.vars) >= 1:
            filter_str += ' . FILTER (__expr__)'

            or_expr = []
            for tuple in filter_bag:
                and_expr = []
                for var in self.vars:
                    #aux = "?" + var + "==" + tuple[var]
                    if var in tuple:
                        v = tuple[var]
                        if isinstance(v, dict):
                            suffix = ''
                            if v['type'] == 'uri':
                                v = "<" + v['value'] + ">"
                                v = "?" + var + "=" + v
                                and_expr.append(v)
                            elif v[
                                'type'] == 'bnode':  # this could be a problem (semantically), since scope of blank nodes is within a data source
                                v = v['value']
                                v = "?" + var + "=" + v
                                and_expr.append(v)
                            else:
                                if 'datatype' in v:
                                    if isinstance(v['datatype'], bytes):
                                        suffix = "^^<" + v['datatype'].decode('utf-8') + ">"
                                    else:
                                        suffix = "^^<" + v['datatype'] + ">"

                                if "xml:lang" in v:
                                    suffix = '@' + v['xml:lang']
                                try:
                                    if isinstance(v['value'], bytes):
                                        vf = v['value'].decode('utf-8') + suffix
                                    else:
                                        vf = v['value'] + suffix
                                except:
                                    vf = v['value'] + suffix

                                v = "(?" + var + "=" + vf + ' || ' + "?" + var + '="' + v['value'].decode(
                                    'utf-8') + '")'
                                and_expr.append(v)

                        else:
                            if v.find("http") == 0:  # uris must be passed between < .. >
                                v = "<" + v + ">"
                                v = "?" + var + "=" + v
                                and_expr.append(v)
                            else:
                                if '^^<' not in v:
                                    v = '"' + v + '"'
                                    vf = "?" + var + "=" + v  # + " || " + "?" + var + "=" + v + "^^<http://www.w3.org/2001/XMLSchema#string>)"
                                    and_expr.append(vf)
                                else:
                                    loc = v.find('^^<')
                                    vf = '"' + v[:loc] + '"' + v[loc:]
                                    v = "(?" + var + "=" + vf + ' || ' + "?" + var + '="' + v[:loc] + '")'
                                    and_expr.append(v)
                if len(and_expr) > 1:
                    or_expr.append('(' + ' && '.join(and_expr) + ')')
                else:
                    or_expr.append(' && '.join(and_expr))

                # or_expr.append('(' + ' && '.join(and_expr) + ')')
            filter_str = filter_str.replace('__expr__', ' || '.join(or_expr))

        new_operator = operator.instantiateFilter(set(new_vars), filter_str)
        #print "type(new_operator)", type(new_operator)
        return new_operator

    def probeAndInsert1(self, tuple, table1, table2, time):
        #print "in probeAndInsert1", tuple
        record = Record(tuple, time, 0)
        r = self.getResource(tuple)
        #print "resource", r, tuple
        if r in table1:
            records = table1[r]
            for t in records:
                if t.ats > record.ats:
                    continue
                x = t.tuple.copy()
                x.update(tuple)
                self.qresults.put(x)
                # Delete tuple from bag.
                try:
                    self.bag.remove(tuple)
                except ValueError:
                    pass

        p = table2.get(r, [])
        i = (p == [])
        p.append(record)
        table2[r] = p
        return i

    def probeAndInsert2(self, resource, tuple, table1, table2, time):
        #print "probeAndInsert2", resource, tuple
        record = Record(tuple, time, 0)
        if resource in table1:
            records = table1[resource]
            for t in records:
                if t.ats > record.ats:
                    continue
                x = t.tuple.copy()
                x.update(tuple)
                self.qresults.put(x)
                # Delete tuple from bag.
                try:
                    self.bag.remove(t.tuple)
                except ValueError:
                    pass

        p = table2.get(resource, [])
        p.append(record) 
        table2[resource] = p
