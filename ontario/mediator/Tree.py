
__author__ = 'Kemele M. Endris'

import abc
from ontario.sparql.parser.services import Service, Triple


class Tree(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def instantiate(self, d):
        return

    @abc.abstractmethod
    def instantiateFilter(self, d, filter_str):
        return

    def degree(self):
        return get_degree(self.vars, self.dict)

    def __leq__(self, other):
        return (self.size < other.size or (self.size == other.size
                                           and self.degree() <= other.degree()))

    def __lt__(self, other):
        return (self.size < other.size or (self.size == other.size
                                           and self.degree() < other.degree()))

    @abc.abstractmethod
    def __eq__(self, other):
        return

    @abc.abstractmethod
    def __hash__(self):
        return

    def __ne__(self, other):
        return not self == other

    @abc.abstractmethod
    def __repr__(self):
        return

    @abc.abstractmethod
    def aux(self, n):
        return

    @abc.abstractmethod
    def aux2(self, n):
        return

    def show(self, n):
        return self.aux(n)

    def show2(self, n):
        return self.aux2(n)

    @abc.abstractmethod
    def getVars(self):
        return

    @abc.abstractmethod
    def places(self):
        return

    @abc.abstractmethod
    def constantNumber(self):
        return

    def constantPercentage(self):
        if self.places() == 0:
            return self.constantNumber()

        return self.constantNumber()/self.places()


class Node(Tree):

    def __init__(self, l, r, filters=None):
        if filters is None:
            filters = []
        self.left = l
        self.right = r
        self.vars = unify(l.vars, r.vars, l.dict)
        self.dict = l.dict = r.dict
        self.size = l.size + r.size
        self.filters = []
        serviceVars = l.getVars() + r.getVars()
        for f in filters:
            vars_f = f.getVars()
            if set(serviceVars) & set(vars_f) == set(vars_f):
                self.filters.append(f)

    def instantiate(self, d):
        return Node(self.left.instantiate(d), self.right.instantiate(d))

    def instantiateFilter(self, d, filter_str):
        return Node(self.left.instantiateFilter(d, filter_str), self.right.instantiateFilter(d, filter_str))

    def __eq__(self, other):
        return ((isinstance(other, Node)) and (self.vars == other.vars) and
                (self.dict == other.dict) and (self.degree() == other.degree()) and
                (self.size == other.size) and (self.left == other.left) and
                (self.right == other.right))

    def __hash__(self):
        return hash((self.vars, self.dict, self.size, self.degree(),
                     self.left, self.right))

    def __repr__(self):
        return self.aux(" ")

    def aux(self, n):
        s = ""
        if self.left:
            s = s + n + "{\n" + self.left.aux(n + "  ") + "\n" + n + "}\n" + n + "  . \n"

        if self.right:
            s = s + n + "{\n" + self.right.aux(n + "  ") + "\n" + n + "}"
        for f in self.filters:
            s += str(f)
        return s

    def show(self, n):
        return self.aux(n)

    def show2(self, n):
        return self.aux2(n)

    def aux2(self, n):
        s = ""
        if self.left:
            s = s + n + "{\n" + self.left.aux2(n + "  ") + "\n" + n + "}\n" + n + "  UNION \n"

        if self.right:
            s = s + n + "{\n" + self.right.aux2(n + "  ") + "\n" + n + "}"
        return s

    def places(self):
        return self.left.places() + self.right.places()

    def constantNumber(self):
        return self.left.constantNumber() + self.right.constantNumber()

    def getVars(self):
        vs = []
        if self.left:
            vs = vs + self.left.getVars()
        if self.right:
            vs = vs + self.right.getVars()
        return vs

    def getConsts(self):
        cvs = []
        if self.left:
            cvs = cvs + self.left.getConsts()
        if self.right:
            cvs = cvs + self.right.getConsts()
        return cvs


class Leaf(Tree):
    def __init__(self, s, vs, dc, filter=None):
        if filter is None:
            filter = []
        self.vars = vs
        self.dict = dc
        self.size = 1
        self.service = s
        self.filters = []
        serviceVars = s.getVars()
        for f in filter:
            vars_f = f.getVars()
            if set(serviceVars) & set(vars_f) == set(vars_f):
                self.filters.append(f)

    def __hash__(self):
        return hash((self.vars, self.dict, self.size, self.degree(),
                     self.service))

    def __repr__(self):
        return str(self.service)

    def __eq__(self, other):
        return ((isinstance(other, Leaf)) and (self.vars == other.vars) and
                (self.dict == other.dict) and (self.degree() == other.degree()) and
                (self.service == other.service))

    def instantiate(self, d):
        newvars = self.vars - set(d.keys())
        newdict = self.dict.copy()
        for c in d:
            if c in newdict:
                del newdict[c]
        return Leaf(self.service.instantiate(d), newvars, newdict)

    def instantiateFilter(self, d, filter_str):
        newvars = self.vars - set(d)
        newdict = self.dict.copy()
        for c in d:
            if c in newdict:
                del newdict[c]
        return Leaf(self.service.instantiateFilter(d, filter_str), newvars, newdict)

    def aux(self, n):
        return self.service.show(n)

    def aux2(self, n):
        return self.service.show2(n)

    def show(self, n):
        return self.aux(n)

    def show2(self, n):
        return self.aux2(n)

    def getInfoIO(self, query):
        subquery = self.service.getTriples()
        vs = list(set(self.service.getVars()))  # - set(self.service.filters_vars)) # Modified this by mac: 31-01-2014
        cvs = list(set(self.service.getConsts()))

        predictVar = set(self.service.getPredVars())
        variables = [v.lstrip("?$") for v in vs]
        constants = [v for v in cvs]
        if query.args == []:
            projvars = vs
        else:
            projvars = list(set([v.name for v in query.args if not v.constant]))
        subvars = list((query.join_vars | set(projvars)) & set(vs))
        vars_order_by = [x for v in query.order_by for x in v.getVars() if x in subvars]
        if subvars == []:
            subvars = vs
        filter_vars = [v for v in query.getFilterVars() if v in vs]

        subvars = list(set(subvars) | predictVar | set(vars_order_by) | set(filter_vars))

        # This corresponds to the case when the subquery is the same as the original query.
        # In this case, we project the variables of the original query.

        if query.body.show(" ").count("SERVICE") == 1:
            subvars = list(set(projvars) | set(vars_order_by))

        subvars = " ".join(subvars)
        # MEV distinct pushed down to the sources
        if query.distinct:
            d = "DISTINCT "
        else:
            d = ""

        subquery = "SELECT " + d + subvars + " WHERE {" + subquery + "\n" + query.filter_nested + "\n}"
        return self.service.endpoint, query.getPrefixes() + subquery, set(variables), set(constants)

    def getCount(self, query, vars, endpointType):
        subquery = self.service.getTriples()
        if len(vars) == 0:
            vs = self.service.getVars()
            variables = [v.lstrip("?$") for v in vs]
            vars_str = "*"
        else:
            variables = vars
            service_vars = self.service.getVars()
            vars2 = []
            for v1 in vars:
                for v2 in service_vars:
                    if v1 == v2[1:]:
                        vars2.append(v2)
                        break
            if len(vars2) > 0:
                vars_str = " ".join(vars2)
            else:
                vars_str = "*"

        d = "DISTINCT "
        if endpointType == "V":
            subquery = "SELECT COUNT " + d + vars_str + "  WHERE {" + subquery + "\n" + query.filter_nested + "}"
        else:
            subquery = "SELECT ( COUNT (" + d + vars_str + ") AS ?cnt)  WHERE {" + subquery + "\n" + query.filter_nested + "}"

        return self.service.endpoint, query.getPrefixes() + subquery

    def getVars(self):
        return self.service.getVars()

    def getConsts(self):
        return self.service.getConsts()

    def places(self):
        return self.service.places()

    def constantNumber(self):
        return self.service.constantNumber()


def get_degree(vars0, dict0):
    s = 0
    for v in vars0:
        s = s + dict0[v]
    return s


def unify(vars0, vars1, dict0):

    vars2 = set(vars0)
    for v in vars1:
        if v in vars2:
            dict0[v] = dict0[v] - 1
            if v in dict0 and dict0[v] == 0:
                del dict0[v]
                vars2.remove(v)
        else:
            vars2.add(v)

    return vars2


def shareAtLeastOneVar(l, r):
    return len(l.vars & r.vars) > 0


def get_so_variables(triples):
    tvars = []
    for t in triples:
        if isinstance(t, Triple):
            if not t.subject.constant:
                tvars.append(t.subject.name)
            if not t.predicate.constant:
                tvars.append(t.predicate.name)
            # exclude variables that are not projected
            if not t.theobject.constant:
                tvars.append(t.theobject.name)
        else:
            tvars.extend(get_so_variables(t.triples))

    return tvars


def sort(lss):
    lss = sorted(lss)
    lo = []
    while not(lss == []):
        m = 0
        for i in range(len(lss)):
            if lss[i].constantPercentage() > lss[m].constantPercentage():
                m = i

            # Ordering between two leaf operators, i.e., SSQs
            if isinstance(lss[i], Service) and isinstance(lss[m], Service) and \
                    lss[i].constantPercentage() == lss[m].constantPercentage():
                if len(lss[i].star['triples']) > len(lss[m].star['triples']):
                    m = i
                if len(get_so_variables(lss[i].star['triples'])) < len(get_so_variables(lss[m].star['triples'])):
                    m = i
        # Ordering between two leaf operators, i.e., SSQs,
        # based on type of data source (scores given manually from experience)
        for i in range(len(lss)):
            if isinstance(lss[i], Service) and isinstance(lss[m], Service) and \
                    getdsscore(lss[i].datasource.dstype) > getdsscore(lss[m].datasource.dstype):
                m = i
        lo.append(lss[m])
        lss.pop(m)

    llo = []
    # Global ordering based on data source type scores
    while not(lo == []):
        m = 0
        for i in range(len(lo)):
            if isinstance(lo[i], Service) and isinstance(lo[m], Service) and \
                    getdsscore(lo[i].datasource.dstype) < getdsscore(lo[m]):
                m = i
        llo.append(lo[m])
        lo.pop(m)
    return llo


def getdsscore(dstype):
    from ontario.model import DataSourceType

    if dstype == DataSourceType.SPARQL_ENDPOINT:
        return 20
    elif dstype == DataSourceType.MONGODB:
        return 15
    elif dstype == DataSourceType.NEO4J:
        return 15
    elif dstype == DataSourceType.HADOOP_CSV:
        return 12
    elif dstype == DataSourceType.HADOOP_XML:
        return 5
    elif dstype == DataSourceType.HADOOP_JSON:
        return 8
    elif dstype == DataSourceType.HADOOP_TSV:
        return 12
    elif dstype == DataSourceType.SPARK_CSV:
        return 12
    elif dstype == DataSourceType.SPARK_XML:
        return 5
    elif dstype == DataSourceType.SPARK_JSON:
        return 8
    elif dstype == DataSourceType.SPARK_TSV:
        return 12
    elif dstype == DataSourceType.REST_SERVICE:
        return 5
    elif dstype == DataSourceType.LOCAL_CSV:
        return 10
    elif dstype == DataSourceType.LOCAL_TSV:
        return 10
    elif dstype == DataSourceType.LOCAL_JSON:
        return 7
    elif dstype == DataSourceType.LOCAL_XML:
        return 4
    elif dstype == DataSourceType.MYSQL:
        return 18
    else:
        return 0


def createLeafs(lss, filters=None):

    if filters is None:
        filters = []
    d = dict()
    for s in lss:
        l = s.getVars()
        l = set(l)
        for e in l:
            v = d.get(e) if e in d else 0
            d[e] = v + 1
    el = []
    for e in d:
        d[e] = d[e] - 1
        if d[e] <= 0:
            el.append(e)
    for e in el:
        del d[e]
    ls = []
    slo = [s for s in lss if isinstance(s, Service)]
    lo = sort(slo)
    lo.extend([s for s in lss if not isinstance(s, Service)])

    for s in lo:
        e = set()
        l = s.getVars()
        for v in l:
            if v in d:
                e.add(v)
        ls.append(Leaf(s, e, d, filters))

    return d, ls


def makeNode(l, r, filters=None):
    if filters is None:
        filters = []
    if l.constantPercentage() > r.constantPercentage():
        n = Node(l, r, filters)
    else:
        n = Node(r, l, filters)
    return n


def makeBushyTree(ss, filters=None):
    if filters is None:
        filters = []
    (d, pq) = createLeafs(ss, filters)
    others = []
    while len(pq) > 1:
        done = False
        l = pq.pop(0)  # heapq.heappop(pq)
        lpq = pq  # heapq.nsmallest(len(pq), pq)

        for i in range(0, len(pq)):
            r = lpq[i]

            if shareAtLeastOneVar(l, r):
                pq.remove(r)
                n = makeNode(l, r, filters)
                pq.append(n)  # heapq.heappush(pq, n)
                done = True
                break
        if not done:
            others.append(l)

    if len(pq) == 1:
        for e in others:
            pq[0] = makeNode(pq[0], e, filters)
        return pq[0]
    elif others:
        while len(others) > 1:
            l = others.pop(0)
            r = others.pop(0)

            n = Node(l, r, filters)
            others.append(n)
        if others:
            return others[0]
        return None
