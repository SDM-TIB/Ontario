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


class LakePlanner(object):

    def __init__(self, query, decompositions, config):
        self.query = query
        self.decompositions = decompositions
        self.config = config

    def make_tree(self):
        bywtype = {}
        services = []
        unionplans = []
        for star in self.decompositions:
            for s in self.decompositions[star]:
                ds = self.decompositions[star][s]['datasource']
                bywtype.setdefault(star, {}).setdefault(str(ds.dstype.value), []).append(self.decompositions[star][s])

        for star in bywtype:
            unions = []

            for wt in bywtype[star]:
                for dc in bywtype[star][wt]:
                    ds = dc['datasource']
                    triples = dc['triples']
                    rdfmts = dc['rdfmts']
                    # datasources = [dss['datasource'] for dss in bywtype[star][wt]]
                    serv = Service(endpoint="<" + ds.url + ">", triples=triples, datasource=ds, rdfmts=rdfmts, star=dc)
                    unions.append(serv)

            if len(unions) > 1:
                elems = [JoinBlock([s]) for s in unions]
                ub = UnionBlock(elems)
                unionplans = unionplans + [ub]
            else:
                services.extend(unions)

        if services and unionplans:
            unionplans.insert(0, services)
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
            # if left.operator.__class__.__name__ == "NestedHashJoinFilter":
            #    l = TreePlan(Xgoptional(left.vars, right.vars), all_variables, l, right)
            # Case 1: left operator is highly selective and right operator is low selective
            if not (lowSelectivityLeft) and lowSelectivityRight and not (isinstance(right, NodeOperator)):
                l = NodeOperator(NestedHashOptional(left.vars, right.vars), all_variables, self.config, l, right)
                dependent_op = True

            # Case 2: left operator is low selective and right operator is highly selective
            elif lowSelectivityLeft and not (lowSelectivityRight) and not (isinstance(right, NodeOperator)):
                l = NodeOperator(NestedHashOptional(left.vars, right.vars), all_variables, self.config, right, l)
                dependent_op = True

            elif not lowSelectivityLeft and lowSelectivityRight and not (isinstance(left, NodeOperator) and (left.operator.__class__.__name__ == "NestedHashJoinFilter" or left.operator.__class__.__name__ == "Xgjoin")) \
                    and not (isinstance(right, LeafOperator)) \
                    and not (right.operator.__class__.__name__ == "NestedHashJoinFilter" or right.operator.__class__.__name__ == "Xgjoin") \
                    and (right.operator.__class__.__name__ == "Xunion"):
                l = NodeOperator(NestedHashOptional(left.vars, right.vars), all_variables, self.config, l, right)
                dependent_op = True
            # Case 3: both operators are low selective
            else:
                l = NodeOperator(Xgoptional(left.vars, right.vars), all_variables, self.config, l, right)
                # print "Planner CASE 3: xgoptional"

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
                        if set(n.vars) & set(vars_f) == set(vars_f):
                            n = NodeOperator(Xfilter(f), n.vars, self.config, n)
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
                        if set(n.vars) & set(vars_f) == set(vars_f):
                            n = NodeOperator(Xfilter(f), n.vars, self.config, n)
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
                    if set(n.vars) & set(vars_f) == set(vars_f):
                        n = NodeOperator(Xfilter(f), n.vars, self.config, n)
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
        n = None
        dependent_join = False

        n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, left, right, consts, self.query)

        return n