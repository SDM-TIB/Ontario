from ontario.mediator.Tree import *
from ontario.sparql.parser.services import *
from ontario.mediator.PlanOperators import *
from ontario.operators.sparql.Xgjoin import Xgjoin
from ontario.operators.sparql.NestedHashJoinFilter import NestedHashJoinFilter
from ontario.operators.sparql.NestedHashOptional import NestedHashOptional
from ontario.operators.sparql.Xunion import Xunion
from ontario.operators.sparql.Xdistinct import Xdistinct
from ontario.operators.sparql.Xfilter import Xfilter
from ontario.operators.sparql.Xproject import Xproject
from ontario.operators.sparql.Xoffset import Xoffset
from ontario.operators.sparql.Xlimit import Xlimit
from ontario.operators.sparql.Xgoptional import Xgoptional


def getPrefs(ps):
    prefDict = dict()
    for p in ps:
         pos = p.find(":")
         c = p[0:pos].strip()
         v = p[(pos+1):len(p)].strip()
         prefDict[c] = v
    return prefDict


def getUri(p, prefs):
    prefs = getPrefs(prefs)
    if "'" in p.name or '"' in p.name:
        return p.name
    hasPrefix = prefix(p)
    if hasPrefix:
        (pr, su) = hasPrefix
        n = prefs[pr]
        n = n[:-1]+su+">"
        return n
    return p.name


def prefix(p):
    s = p.name
    pos = s.find(":")
    if (not (s[0] == "<")) and pos > -1:
        return (s[0:pos].strip(), s[(pos+1):].strip())

    return None


class LakePlanner(object):

    def __init__(self, query, decompositions, config):
        self.query = query
        self.decompositions = decompositions
        self.config = config

    def get_filters(self, triples, filters):
        result = []
        t_vars = []
        for t in triples:
            t_vars.extend(t.getVars())

        for f in filters:
            f_vars = f.getVars()
            if len(set(f_vars).intersection(t_vars)) == len(set(f_vars)):
                result.append(f)

        return result

    def make_tree(self):
        services = []
        unionplans = []
        for star in self.decompositions:
            unions = []
            wpreds = {}
            ptrs = {}
            ppreds = []
            for s in self.decompositions[star]:
                dc = self.decompositions[star][s]
                wpreds[s] = dc['predicates']
                triples = dc['triples']
                trps = {getUri(tr.predicate, self.query.prefs)[1:-1]: tr for tr in triples if
                        tr.predicate.constant}
                for p in trps:
                    ptrs[p] = trps[p]

            i = 0
            skipp = False
            for s in self.decompositions[star]:
                dc = self.decompositions[star][s]
                triples = dc['triples']

                if skipp:
                    continue

                ds = dc['datasource']

                rdfmts = dc['rdfmts']

                if i == 0:
                    ppreds = dc['predicates']
                else:
                    if len(set(ppreds).intersection(dc['predicates'])) != len(ppreds):
                        unions = []
                        break
                # datasources = [dss['datasource'] for dss in bywtype[star][wt]]

                serv = Service(endpoint="<" + ds.url + ">", triples=triples, datasource=ds, rdfmts=rdfmts, star=dc)
                serv.filters = self.get_filters(triples, dc['filters'])
                unions.append(serv)
                i += 1

            if len(unions) == 0:
                inall = []
                difs = {}
                i = 0
                for e in wpreds:
                    if i == 0:
                        inall = wpreds[e]
                    else:
                        inall = list(set(inall).intersection(wpreds[e]))

                    if e not in difs:
                        difs[e] = wpreds[e]
                    for d in difs:
                        if e == d:
                            continue
                        dd = list(set(difs[d]).difference(wpreds[e]))
                        if len(dd) > 0:
                            difs[d] = dd

                        dd = list(set(difs[e]).difference(wpreds[d]))
                        if len(dd) > 0:
                            difs[e] = dd
                    i += 1

                oneone = {}
                for e1 in wpreds:
                    for e2 in wpreds:
                        if e1 == e2 or e2 + '|-|' + e1 in oneone:
                            continue
                        pp = set(wpreds[e1]).intersection(wpreds[e2])
                        pp = list(set(pp).difference(inall))
                        if len(pp) > 0:
                            oneone[e1 + '|-|' + e2] = pp
                onv = []
                [onv.extend(d) for d in list(oneone.values())]
                difv = []
                [difv.extend(d) for d in list(difs.values())]
                for o in onv:
                    if o in difv:
                        toremov = []
                        for d in difs:
                            if o in difs[d]:
                                difs[d].remove(o)
                                difv.remove(o)
                            if len(difs[d]) == 0:
                                toremov.append(d)
                        for d in toremov:
                            del difs[d]

                ddd = onv + difv
                if len(set(inall + ddd)) == len(list(ptrs.keys())):
                    if len(inall) > 0:
                        if len(inall) == 1 and inall[0] == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                            pass
                        else:
                            trps = [ptrs[p] for p in inall]

                            elems = [JoinBlock([Service(endpoint="<" + self.decompositions[star][wt]['datasource'].url + ">",
                                                        triples=list(set(trps)),
                                                        datasource=self.decompositions[star][wt]['datasource'],
                                                        rdfmts=self.decompositions[star][wt]['rdfmts'],
                                                        star=self.decompositions[star][wt])],
                                                        filters=self.get_filters(list(set(trps)), self.decompositions[star][wt]['filters']))
                                                        for wt in list(wpreds.keys())]
                            ub = UnionBlock(elems)
                            unionplans = unionplans + [ub]
                            # qpl1.append(ub)
                    if len(oneone) > 0:

                        for ee in oneone:
                            e1, e2 = ee.split("|-|")
                            pp = oneone[ee]
                            if len(pp) == 1 and pp[0] == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                                pass
                            else:
                                trps = [ptrs[p] for p in pp]
                                elems = [JoinBlock([Service(endpoint="<" + self.decompositions[star][e1]['datasource'].url + ">",
                                                            triples=list(set(trps)),
                                                            datasource=self.decompositions[star][e1]['datasource'],
                                                            rdfmts=self.decompositions[star][e1]['rdfmts'],
                                                            star=self.decompositions[star][e1])],
                                                        filters=self.get_filters(list(set(trps)), self.decompositions[star][e1]['filters'])),
                                         JoinBlock([Service(endpoint="<" + self.decompositions[star][e2]['datasource'].url + ">",
                                                            triples=list(set(trps)),
                                                            datasource=self.decompositions[star][e2]['datasource'],
                                                            rdfmts=self.decompositions[star][e2]['rdfmts'],
                                                            star=self.decompositions[star][e2],
                                                        filters=self.get_filters(list(set(trps)), self.decompositions[star][e2]['filters']))])]
                                ub = UnionBlock(elems)
                                unionplans = unionplans + [ub]
                                # unions.append(elems)
                    if len(difs) > 0:
                        for d in difs:
                            trps = [ptrs[p] for p in difs[d]]
                            services.append(Service(endpoint="<" + self.decompositions[star][d]['datasource'].url + ">",
                                    triples=list(set(trps)),
                                    datasource=self.decompositions[star][d]['datasource'],
                                    rdfmts=self.decompositions[star][d]['rdfmts'],
                                    star=self.decompositions[star][d],
                                    filters=self.get_filters(list(set(trps)), self.decompositions[star][d]['filters'])))
                            # services.append(Service("<" + d + ">", list(set(trps))))

            if len(unions) > 1:
                elems = [JoinBlock([s]) for s in unions]
                ub = UnionBlock(elems)
                unionplans = unionplans + [ub]
            else:
                services.extend(unions)

        if services and unionplans:
            unionplans = services + unionplans
        elif services:
            unionplans = services

        self.query.body = UnionBlock([JoinBlock(unionplans)])
        self.query.body = self.makePlanQuery(self.query)
        return self.query

    def make_plan(self):
        operatorTree = self.includePhysicalOperatorsQuery()

        # Adds the project operator to the plan.
        operatorTree = NodeOperator(Xproject(self.query.args), operatorTree.vars, self.config, operatorTree)

        # Adds the distinct operator to the plan.
        if (self.query.distinct):
            operatorTree = NodeOperator(Xdistinct(None), operatorTree.vars, self.config, operatorTree)

        # Adds the offset operator to the plan.
        if (self.query.offset != -1):
            operatorTree = NodeOperator(Xoffset(None, self.query.offset), operatorTree.vars, self.config, operatorTree)

        # Adds the limit operator to the plan.
        if (self.query.limit != -1):
            # print "query.limit", query.limit
            operatorTree = NodeOperator(Xlimit(None, self.query.limit), operatorTree.vars, self.config, operatorTree)

        return operatorTree

    def includePhysicalOperatorsQuery(self):
        return self.includePhysicalOperatorsUnionBlock(self.query.body)

    def includePhysicalOperatorsUnionBlock(self, ub):

        r = []
        for jb in ub.triples:
            pp = self.includePhysicalOperatorsJoinBlock(jb)
            r.append(pp)

        while len(r) > 1:
            left = r.pop(0)
            right = r.pop(0)
            all_variables = left.vars | right.vars
            n = NodeOperator(Xunion(left.vars, right.vars), all_variables, self.config, left, right)
            r.append(n)

        if len(r) == 1:
            n = r[0]
            for f in ub.filters:
                n = NodeOperator(Xfilter(f), n.vars, self.config, n)
            return n
        else:
            return None

    def includePhysicalOperatorsJoinBlock(self, jb):

        tl = []
        ol = []
        if isinstance(jb.triples, list):
            for bgp in jb.triples:
                if isinstance(bgp, Node) or isinstance(bgp, Leaf):
                    tl.append(self.includePhysicalOperators(bgp))
                elif isinstance(bgp, Optional):
                    ol.append(self.includePhysicalOperatorsUnionBlock(bgp.bgg))
                elif isinstance(bgp, UnionBlock):
                    tl.append(self.includePhysicalOperatorsUnionBlock(bgp))
        elif isinstance(jb.triples, Node) or isinstance(jb.triples, Leaf):
            tl = [self.includePhysicalOperators(jb.triples)]
        else:  # this should never be the case..
            pass

        while len(tl) > 1:
            l = tl.pop(0)
            r = tl.pop(0)
            n = self.make_joins(l, r)
            tl.append(n)

        if len(tl) == 1:
            nf = self.includePhysicalOperatorsOptional(tl[0], ol)

            if isinstance(tl[0], NodeOperator) and isinstance(tl[0].operator, Xfilter):
                return nf
            else:
                if len(jb.filters) > 0:
                    for f in jb.filters:
                        nf = NodeOperator(Xfilter(f), nf.vars, self.config, nf)
                    return nf
                else:
                    return nf
        else:
            return None

    def includePhysicalOperatorsOptional(self, left, rightList):

        l = left
        for right in rightList:
            all_variables = left.vars | right.vars
            lowSelectivityLeft = l.allTriplesLowSelectivity()
            lowSelectivityRight = right.allTriplesLowSelectivity()
            join_variables = l.vars & right.vars
            dependent_op = False
            l = NodeOperator(Xgoptional(left.vars, right.vars), all_variables, self.config, l, right)


            # Case 1: left operator is highly selective and right operator is low selective
            # if not (lowSelectivityLeft) and lowSelectivityRight and not (isinstance(right, NodeOperator)):
            #     l = NodeOperator(NestedHashOptional(left.vars, right.vars), all_variables, self.config, l, right)
            #     dependent_op = True
            #
            # # Case 2: left operator is low selective and right operator is highly selective
            # elif lowSelectivityLeft and not (lowSelectivityRight) and not (isinstance(right, NodeOperator)):
            #     l = NodeOperator(NestedHashOptional(left.vars, right.vars), all_variables, self.config, right, l)
            #     dependent_op = True
            #
            # elif not lowSelectivityLeft and lowSelectivityRight and not (isinstance(left, NodeOperator) and (left.operator.__class__.__name__ == "NestedHashJoinFilter" or left.operator.__class__.__name__ == "Xgjoin")) \
            #         and not (isinstance(right, LeafOperator)) \
            #         and not (right.operator.__class__.__name__ == "NestedHashJoinFilter" or right.operator.__class__.__name__ == "Xgjoin") \
            #         and (right.operator.__class__.__name__ == "Xunion"):
            #     l = NodeOperator(NestedHashOptional(left.vars, right.vars), all_variables, self.config, l, right)
            #     dependent_op = True
            # # Case 3: both operators are low selective
            # else:
            #     l = NodeOperator(Xgoptional(left.vars, right.vars), all_variables, self.config, l, right)
            #     # print "Planner CASE 3: xgoptional"

            if isinstance(l.left, LeafOperator) and isinstance(l.left.tree, Leaf) and not l.left.tree.service.allTriplesGeneral():
                if l.left.constantPercentage() <= 0.5:
                    l.left.tree.service.limit = 10000  # Fixed value, this can be learnt in the future
                    # print "modifying limit optional left ..."

            if isinstance(l.right, LeafOperator) and isinstance(l.right.tree, Leaf):
                if not dependent_op:
                    if (l.right.constantPercentage() <= 0.5) and not (l.right.tree.service.allTriplesGeneral()):
                        l.right.tree.service.limit = 10000  # Fixed value, this can be learnt in the future
                        # print "modifying limit optional right ..."
                else:
                    new_constants = 0
                    for v in join_variables:
                        new_constants = new_constants + l.right.query.show().count(v)
                    if ((l.right.constantNumber() + new_constants) / l.right.places() <= 0.5) and not l.right.tree.service.allTriplesGeneral():
                        l.right.tree.service.limit = 10000  # Fixed value, this can be learnt in the future
                        # print "modifying limit optional right ..."

        return l

    def includePhysicalOperators(self, tree):

        if isinstance(tree, Leaf):
            if isinstance(tree.service, Service):
                if len(tree.filters) == 0:
                    return LeafOperator(self.query, tree, None, self.config)
                else:
                    n = LeafOperator(self.query, tree, None, self.config)
                    for f in tree.filters:
                        vars_f = f.getVarsName()
                        if set(n.vars) & set(n.vars) == set(vars_f):
                            n = NodeOperator(Xfilter(f), set(vars_f), self.config, n)
                    return n
            elif isinstance(tree.service, UnionBlock):
                return self.includePhysicalOperatorsUnionBlock(tree.service)
            elif isinstance(tree.service, JoinBlock):
                if len(tree.filters) == 0:
                    return self.includePhysicalOperatorsJoinBlock(tree.service)
                else:
                    n = self.includePhysicalOperatorsJoinBlock(tree.service)
                    for f in tree.filters:
                        vars_f = f.getVarsName()
                        # if set(n.vars) & set(vars_f) == set(vars_f):
                        n = NodeOperator(Xfilter(f), set(vars_f), self.config, n)
                    return n
            else:
                print("tree.service" + str(type(tree.service)) + str(tree.service))
                print("Error Type not considered")

        elif isinstance(tree, Node):
            left_subtree = self.includePhysicalOperators(tree.left)
            right_subtree = self.includePhysicalOperators(tree.right)
            if tree.filters == []:
                return self.make_joins(left_subtree, right_subtree)
            else:
                n = self.make_joins(left_subtree, right_subtree)
                for f in tree.filters:
                    vars_f = f.getVarsName()
                    # if set(n.vars) & set(vars_f) == set(vars_f):
                    n = NodeOperator(Xfilter(f), set(vars_f), self.config, n)
            return n

    '''
        ===================================================
        ========= MAKE PLAN =================================
        ===================================================
    '''

    def makePlanQuery(self, q):
        x = self.makePlanUnionBlock(q.body)
        return x

    def makePlanUnionBlock(self, ub):
        r = []
        for jb in ub.triples:
            r.append(self.makePlanJoinBlock(jb))
        return UnionBlock(r, ub.filters)

    def makePlanJoinBlock(self, jb):
        sl = []
        ol = []

        for bgp in jb.triples:
            if type(bgp) == list:
                sl.extend(bgp)
            elif isinstance(bgp, Optional):

                for f in jb.filters:
                    vars_f = f.getVars()
                    if set(bgp.getVars()) & set(vars_f) == set(vars_f):
                        for t in bgp.bgg.triples:
                            if set(t.getVars()) & set(vars_f) == set(vars_f):
                                t.filters.extend(jb.filters)

                ol.append(Optional(self.makePlanUnionBlock(bgp.bgg)))
            elif isinstance(bgp, UnionBlock):

                for f in jb.filters:
                    vars_f = f.getVars()
                    if set(bgp.getVars()) & set(vars_f) == set(vars_f):
                        for t in bgp.triples:
                            if set(t.getVars()) & set(vars_f) == set(vars_f):
                                t.filters.extend(jb.filters)

                sl.append(self.makePlanUnionBlock(bgp))
            elif isinstance(bgp, JoinBlock):

                for f in jb.filters:
                    vars_f = f.getVars()
                    if set(bgp.getVars()) & set(vars_f) == set(vars_f):
                        bgp.filters.extend(jb.filters)

                sl.append(self.makePlanJoinBlock(bgp))
            elif isinstance(bgp, Service):

                for f in jb.filters:
                    vars_f = f.getVars()
                    if set(bgp.getVars()) & set(vars_f) == set(vars_f):
                        bgp.filters.extend(jb.filters)

                sl.append(bgp)

        pl = self.makePlanAux(sl, jb.filters)
        if ol:
            pl = [pl]
            pl.extend(ol)

        return JoinBlock(pl, filters=jb.filters)

    def makePlanAux(self, ls, filters=[]):
        return self.makeBushyTree(ls, filters)

    def makeBushyTree(self, ls, filters=[]):
        return makeBushyTree(ls, filters)

    def make_joins(self, left, right):
        join_variables = left.vars & right.vars
        all_variables = left.vars | right.vars
        consts = left.consts & right.consts
        lowSelectivityLeft = left.allTriplesLowSelectivity()
        lowSelectivityRight = right.allTriplesLowSelectivity()

        n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, left, right, consts, self.query)

        if isinstance(left, LeafOperator) and isinstance(right, LeafOperator):
            if (n.right.constantPercentage() <= 0.5):
                n.right.tree.service.limit = 10000
            if (n.left.constantPercentage() <= 0.5):
                n.left.tree.service.limit = 10000

            # if ('SPARQL' in left.datasource.dstype.value or 'SQL' in left.datasource.dstype.value) and \
            #      ('SPARQL' in right.datasource.dstype.value or 'SQL' in right.datasource.dstype.value):
            if 'SPARQL' in left.datasource.dstype.value and 'SPARQL' in right.datasource.dstype.value :
                nhj = True
                if not lowSelectivityLeft and not lowSelectivityRight:
                    if left.constantPercentage() > right.constantPercentage():
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, left, right,
                                         consts, self.query)

                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, right, left,
                                         consts, self.query)
                elif not lowSelectivityLeft:
                    n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, left, right,
                                     consts, self.query)
                elif not lowSelectivityRight:
                    n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, right, left,
                                     consts, self.query)
                else:
                    nhj = False

                if nhj:
                    new_constants = 0
                    for v in join_variables:
                        new_constants = new_constants + n.right.query.show().count(v)
                    if (n.right.constantNumber() + new_constants) / n.right.places() <= 0.5:
                        n.right.tree.service.limit = 10000

        return n

    def make_joinsSQ(self, left, right):
        join_variables = left.vars & right.vars
        all_variables = left.vars | right.vars
        consts = left.consts & right.consts
        lowSelectivityLeft = left.allTriplesLowSelectivity()
        lowSelectivityRight = right.allTriplesLowSelectivity()
        n = None
        dependent_join = False

        n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, left, right, consts, self.query)

        if isinstance(left, LeafOperator) and left.tree.service.triples[0].subject.constant:
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, left, right, consts, self.query)
                dependent_join = True
        elif isinstance(right, LeafOperator) and right.tree.service.triples[0].subject.constant:
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, right, left, consts, self.query)
                dependent_join = True
        elif isinstance(right, NodeOperator) and right.operator.__class__.__name__ == "Xunion" and \
                isinstance(left, NodeOperator) and left.operator.__class__.__name__ == "Xunion":
            # both are Union operators
            n = NodeOperator(Xgjoin(join_variables), all_variables,self.config, left, right, consts, self.query)

        elif not lowSelectivityLeft and not lowSelectivityRight and \
                (not isinstance(left, NodeOperator) or not isinstance(right, NodeOperator)):
            # if both are selective and one of them (or both) are Independent Operator
            if len(join_variables) > 0:
                if left.constantPercentage() > right.constantPercentage():
                    if not isinstance(right, NodeOperator):
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, left, right, consts, self.query)
                        dependent_join = True
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, right, left, consts, self.query)
                else:
                    if not isinstance(left, NodeOperator):
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, right, left, consts, self.query)
                        dependent_join = True
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, left, right, consts, self.query)

        elif not lowSelectivityLeft and lowSelectivityRight and not isinstance(right, NodeOperator) and \
                not isinstance(left.operator, Xunion) and \
                (isinstance(left.left, LeafOperator) or left.left.operator.__class__.__name__ != "Xunion" ) and  \
                (isinstance(left.right, LeafOperator) or left.right.operator.__class__.__name__ != "Xunion"):

            # If left is selective, if left != NHJ and right != NHJ -> NHJ (l,r)
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, left, right, consts, self.query)
                dependent_join = True

        elif lowSelectivityLeft and not lowSelectivityRight and not isinstance(left, NodeOperator):
            # if right is selective if left != NHJ and right != NHJ -> NHJ (r,l)
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, right, left, consts, self.query)
                dependent_join = True

        elif not lowSelectivityLeft and lowSelectivityRight \
                and (isinstance(left, NodeOperator) and not left.operator.__class__.__name__ == "NestedHashJoinFilter") \
                and (isinstance(right, NodeOperator) and not (right.operator.__class__.__name__ == "NestedHashJoinFilter"
                                                      or right.operator.__class__.__name__ == "Xgjoin")):
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, left, right, consts, self.query)
                dependent_join = True
        elif lowSelectivityLeft and not lowSelectivityRight and (isinstance(right, NodeOperator) and
                not right.operator.__class__.__name__ == "NestedHashJoinFilter") \
                and (isinstance(left, NodeOperator) and
                     not (left.operator.__class__.__name__ == "NestedHashJoinFilter" or
                     left.operator.__class__.__name__ == "Xgjoin")):
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, right, left, consts, self.query)
                dependent_join = True
        elif lowSelectivityLeft and lowSelectivityRight and isinstance(left, LeafOperator) and isinstance(right, LeafOperator):
            # both are non-selective and both are Independent Operators
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, right, left, consts, self.query)

        if n is None:
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, left, right, consts, self.query)

        if n and isinstance(n.left, LeafOperator) and isinstance(n.left.tree, Leaf):
            if (n.left.constantPercentage() <= 0.5) and not (n.left.tree.service.allTriplesGeneral()):
                n.left.tree.service.limit = 10000  # Fixed value, this can be learnt in the future

        if isinstance(n.right, LeafOperator) and isinstance(n.right.tree, Leaf):
            if not dependent_join:
                if (n.right.constantPercentage() <= 0.5) and not (n.right.tree.service.allTriplesGeneral()):
                    n.right.tree.service.limit = 10000  # Fixed value, this can be learnt in the future
                    # print "modifying limit right ..."
            else:
                new_constants = 0
                for v in join_variables:
                    new_constants = new_constants + n.right.query.show().count(v)
                if ((n.right.constantNumber() + new_constants) / n.right.places() <= 0.5) and not (n.right.tree.service.allTriplesGeneral()):
                    n.right.tree.service.limit = 10000  # Fixed value, this can be learnt in the future

        return n
