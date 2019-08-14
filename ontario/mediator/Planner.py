from ontario.mediator.PlanOperators import *
from ontario.operators.nonblocking.Xgjoin import Xgjoin
from ontario.operators.nonblocking.NestedHashJoinFilter import NestedHashJoinFilter
from ontario.operators.nonblocking.NestedHashOptionalFilter import NestedHashOptionalFilter as NestedHashOptional
from ontario.operators.nonblocking.Xunion import Xunion
from ontario.operators.nonblocking.Xdistinct import Xdistinct
from ontario.operators.nonblocking.Xfilter import Xfilter
from ontario.operators.nonblocking.Xproject import Xproject
from ontario.operators.nonblocking.Xoffset import Xoffset
from ontario.operators.nonblocking.Xlimit import Xlimit
from ontario.operators.nonblocking.Xgoptional import Xgoptional
from ontario.sparql.parser.services import Filter, Triple, Optional, UnionBlock, JoinBlock
from .utility import *


class MetaWrapperPlanner(object):
    def __init__(self, query, decompositions, config):
        self.query = query
        self.decompositions = decompositions
        self.config = config

    def _make_tree(self):
        return self.create_plan_tree(self.decompositions)

    def create_plan_tree(self, decomp):
        unionblocks = []
        sblocks = []
        opblocks = []
        for ub in decomp:
            BGP = ub['BGP']
            joinplans, non_match_filters = decompose_block(BGP, ub['Filter'], self.config, isTreeBlock=True)

            if len(ub['JoinBlock']) > 0:
                joinBlock = self.create_plan_tree(ub['JoinBlock'])
                sblocks.append(JoinBlock(joinBlock))
            if len(ub['UnionBlock']) > 0:
                unionBlock = self.create_plan_tree(ub['UnionBlock'])
                sblocks.append(UnionBlock(unionBlock))

            if len(ub['Optional']) > 0:
                opblocks.append(Optional(UnionBlock(self.create_plan_tree(ub['Optional']))))

            bgp_triples = []
            [bgp_triples.extend(BGP['stars'][s]['triples']) for s in BGP['stars']]
            [joinplans.append(s) for s in sblocks]

            gp = makeBushyTree(joinplans, get_filters(bgp_triples, non_match_filters))

            gp = [gp] + opblocks

            gp = JoinBlock(gp)
            unionblocks.append(gp)

        return unionblocks

    def make_plan(self):
        trree = self._make_tree()
        # print("------------->>", trree)
        # print(self.decompositions)
        self.query.body = UnionBlock(trree)
        operatorTree = self.includePhysicalOperatorsQuery()
        # print(operatorTree)
        if operatorTree is None:
            return []
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
            if isinstance(l, NodeOperator) and (l.dstype is None or l.dstype != DataSourceType.SPARQL_ENDPOINT) or \
                    isinstance(l, LeafOperator) and l.datasource.dstype != DataSourceType.SPARQL_ENDPOINT:
                return NodeOperator(Xgoptional(left.vars, right.vars), all_variables, self.config, l, right)

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

    def make_joins(self, left, right):
        # return self.make_sparql_endpoint_plan(left, right)

        if isinstance(left, LeafOperator) and isinstance(right, LeafOperator):
            if ('SPARQL' in left.datasource.dstype.value and 'SQL' in right.datasource.dstype.value) or \
                 ('SQL' in left.datasource.dstype.value and 'SPARQL' in right.datasource.dstype.value):
                return self.make_sparql_endpoint_plan(left, right)

            if 'SPARQL' in left.datasource.dstype.value and 'SPARQL' in right.datasource.dstype.value:
                return self.make_mulder_joins(left, right)
            # if 'SQL' in left.datasource.dstype.value and 'SQL' in right.datasource.dstype.value:
            #     return self.make_sparql_endpoint_plan(left, right)
        elif isinstance(left, LeafOperator):
            if 'SPARQL' in left.datasource.dstype.value or 'SQL' in left.datasource.dstype.value :
                return self.make_sparql_endpoint_plan(left, right)
        elif isinstance(right, LeafOperator):
            if 'SPARQL' in right.datasource.dstype.value or 'SQL' in right.datasource.dstype.value :
                return self.make_sparql_endpoint_plan(left, right)

        join_variables = left.vars & right.vars
        all_variables = left.vars | right.vars
        consts = left.consts & right.consts
        # lowSelectivityLeft = left.allTriplesLowSelectivity()
        # lowSelectivityRight = right.allTriplesLowSelectivity()
        n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, left, right, consts, self.query)
        if isinstance(left, LeafOperator) and isinstance(right, LeafOperator):
            if (n.right.constantPercentage() <= 0.5):
                n.right.tree.service.limit = 1000
            if (n.left.constantPercentage() <= 0.5):
                n.left.tree.service.limit = 1000

        return n

    def make_sparql_endpoint_plan(self, l, r):
        join_variables = l.vars & r.vars
        all_variables = l.vars | r.vars
        consts = l.consts & l.consts
        lowSelectivityLeft = l.allTriplesLowSelectivity()
        lowSelectivityRight = r.allTriplesLowSelectivity()
        n = None
        dependent_join = False

        if isinstance(l, LeafOperator) and l.tree.service.triples[0].subject.constant:
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                dependent_join = True
        elif isinstance(r, LeafOperator) and r.tree.service.triples[0].subject.constant:
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                dependent_join = True
        elif isinstance(r, LeafOperator) and isinstance(l, LeafOperator):
            if len(join_variables) > 0:

                if "SQL" in l.datasource.dstype.value and 'SPARQL' in r.datasource.dstype.value:
                    if lowSelectivityLeft and lowSelectivityRight:
                        n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, l, r, consts, self.query)
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                        dependent_join = True
                elif "SQL" in r.datasource.dstype.value and 'SPARQL' in l.datasource.dstype.value:
                    if lowSelectivityLeft and lowSelectivityRight:
                        n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, l, r, consts, self.query)
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                else:
                    if not lowSelectivityLeft and lowSelectivityRight:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                    elif lowSelectivityLeft and not lowSelectivityRight:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                    elif not lowSelectivityLeft and not lowSelectivityRight:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                    elif lowSelectivityLeft and lowSelectivityRight:
                        n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, l, r, consts, self.query)
                    dependent_join = True
        elif isinstance(r, NodeOperator) and r.operator.__class__.__name__ == "Xunion" and \
                isinstance(l, NodeOperator) and l.operator.__class__.__name__ == "Xunion":
            # both are Union operators
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, l, r, consts, self.query)

        elif not lowSelectivityLeft and not lowSelectivityRight and (not isinstance(l, NodeOperator) or not isinstance(r, NodeOperator)):
            # if both are selective and one of them (or both) are Independent Operator
            if len(join_variables) > 0:
                if l.constantPercentage() > r.constantPercentage():
                    if not isinstance(r, NodeOperator):
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                        dependent_join = True
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                        dependent_join = True
                else:
                    if not isinstance(l, NodeOperator):
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                        dependent_join = True
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                        dependent_join = True

        elif not lowSelectivityLeft and lowSelectivityRight and not isinstance(r, NodeOperator):

            # If left is selective, if left != NHJ and right != NHJ -> NHJ (l,r)
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                dependent_join = True

        elif lowSelectivityLeft and not lowSelectivityRight and not isinstance(l, NodeOperator):
            # if right is selective if left != NHJ and right != NHJ -> NHJ (r,l)
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                dependent_join = True

        elif not lowSelectivityLeft and lowSelectivityRight \
                and (isinstance(l, NodeOperator) and not l.operator.__class__.__name__ == "NestedHashJoinFilter") \
                and (isinstance(r, NodeOperator) and not (r.operator.__class__.__name__ == "NestedHashJoinFilter"
                                                      or r.operator.__class__.__name__ == "Xgjoin")):
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                dependent_join = True
        elif lowSelectivityLeft and not lowSelectivityRight and (
                isinstance(r, NodeOperator) and not r.operator.__class__.__name__ == "NestedHashJoinFilter") \
                and (isinstance(l, NodeOperator) and not (
                l.operator.__class__.__name__ == "NestedHashJoinFilter" or l.operator.__class__.__name__ == "Xgjoin")):
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                dependent_join = True
        elif lowSelectivityLeft and lowSelectivityRight and isinstance(l, LeafOperator) and isinstance(r, LeafOperator):
            # both are non-selective and both are Independent Operators
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, r, l, consts, self.query)

        if n is None:
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, l, r, consts, self.query)

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
                if ((n.right.constantNumber() + new_constants) / n.right.places() <= 0.5) and not (
                n.right.tree.service.allTriplesGeneral()):
                    n.right.tree.service.limit = 10000  # Fixed value, this can be learnt in the future
        return n

    def make_mulder_joins(self, l, r):
        join_variables = l.vars & r.vars
        all_variables = l.vars | r.vars
        consts = l.consts & l.consts
        # noInstantiatedLeftStar = False
        # noInstantiatedRightStar = False
        lowSelectivityLeft = l.allTriplesLowSelectivity()
        lowSelectivityRight = r.allTriplesLowSelectivity()
        n = None
        dependent_join = False

        if isinstance(l, LeafOperator) and l.tree.service.triples[0].subject.constant:
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                dependent_join = True
        elif isinstance(r, LeafOperator) and r.tree.service.triples[0].subject.constant:
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                dependent_join = True
        elif isinstance(r, NodeOperator) and r.operator.__class__.__name__ == "Xunion" and \
                isinstance(l, NodeOperator) and l.operator.__class__.__name__ == "Xunion":
            # both are Union operators
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, l, r, consts, self.query)

        elif not lowSelectivityLeft and not lowSelectivityRight and (
                not isinstance(l, NodeOperator) or not isinstance(r, NodeOperator)):
            # if both are selective and one of them (or both) are Independent Operator
            if len(join_variables) > 0:
                if l.constantPercentage() > r.constantPercentage():
                    if not isinstance(r, NodeOperator):
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                        dependent_join = True
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                else:
                    if not isinstance(l, NodeOperator):
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                        dependent_join = True
                    else:
                        n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)

        elif not lowSelectivityLeft and lowSelectivityRight and not isinstance(r, NodeOperator):

            # If left is selective, if left != NHJ and right != NHJ -> NHJ (l,r)
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                dependent_join = True
                #
                # if not isinstance(l, TreePlan):
                #     if isinstance(r, TreePlan):
                #         if not isinstance(r.operator, NestedHashJoin) and not isinstance(r.operator, Xgjoin):
                #             n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
                #             dependent_join = True
                #     else:
                #         # IF both are Independent Operators
                #         n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
                #         dependent_join = True
                # elif isinstance(l, TreePlan) and not isinstance(l.operator, NestedHashJoin):
                #     if not isinstance(r, TreePlan):
                #         n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
                #         dependent_join = True
                #     elif isinstance(r, TreePlan) and not isinstance(r.operator, NestedHashJoin) and not isinstance(r.operator, Xgjoin):
                #         n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
                #         dependent_join = True

        elif lowSelectivityLeft and not lowSelectivityRight and not isinstance(l, NodeOperator):
            # if right is selective if left != NHJ and right != NHJ -> NHJ (r,l)
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                dependent_join = True
                #
                # if not isinstance(r, TreePlan):
                #     if isinstance(l, TreePlan):
                #         if not isinstance(l.operator, NestedHashJoin) and not isinstance(l.operator, Xgjoin):
                #             n = TreePlan(NestedHashJoin(join_variables), all_variables, r, l)
                #             dependent_join = True
                #     else:
                #         # IF both are Independent Operators
                #         n = TreePlan(NestedHashJoin(join_variables), all_variables, r, l)
                #         dependent_join = True
                # elif isinstance(r, TreePlan) and not isinstance(r.operator, NestedHashJoin):
                #     if not isinstance(l, TreePlan):
                #         n = TreePlan(NestedHashJoin(join_variables), all_variables, r, l)
                #         dependent_join = True
                #     elif isinstance(l, TreePlan) and not isinstance(l.operator, NestedHashJoin) and not isinstance(l.operator, Xgjoin):
                #         n = TreePlan(NestedHashJoin(join_variables), all_variables, r, l)
                #         dependent_join = True
        elif not lowSelectivityLeft and lowSelectivityRight \
                and (isinstance(l, NodeOperator) and not l.operator.__class__.__name__ == "NestedHashJoinFilter") \
                and (isinstance(r, NodeOperator) and not (r.operator.__class__.__name__ == "NestedHashJoinFilter"
                                                      or r.operator.__class__.__name__ == "Xgjoin")):
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, l, r, consts, self.query)
                dependent_join = True
        elif lowSelectivityLeft and not lowSelectivityRight and (
                isinstance(r, NodeOperator) and not r.operator.__class__.__name__ == "NestedHashJoinFilter") \
                and (isinstance(l, NodeOperator) and not (
                l.operator.__class__.__name__ == "NestedHashJoinFilter" or l.operator.__class__.__name__ == "Xgjoin")):
            if len(join_variables) > 0:
                n = NodeOperator(NestedHashJoinFilter(join_variables), all_variables, self.config, r, l, consts, self.query)
                dependent_join = True
        elif lowSelectivityLeft and lowSelectivityRight and isinstance(l, LeafOperator) and isinstance(r,   LeafOperator):
            # both are non-selective and both are Independent Operators
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, r, l, consts, self.query)

        if n is None:
            n = NodeOperator(Xgjoin(join_variables), all_variables, self.config, l, r, consts, self.query)

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
                if ((n.right.constantNumber() + new_constants) / n.right.places() <= 0.5) and not (
                n.right.tree.service.allTriplesGeneral()):
                    n.right.tree.service.limit = 10000  # Fixed value, this can be learnt in the future
        return n
