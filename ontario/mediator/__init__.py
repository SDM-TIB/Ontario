
__author__ = 'Kemele M. Endris'

from ontario.mediator.Decomposer import MediatorCatalyst
from ontario.mediator.Planner import MetaWrapperPlanner


class Catalyst(object):

    def __init__(self, query, config):
        self.query = query
        self.config = config

    def catalyze(self):
        mc = MediatorCatalyst(self.query, self.config)
        r = mc.decompose()
        self.query = mc.query
        mwp = MetaWrapperPlanner(self.query, r, self.config)
        plan = mwp.make_plan()
        return plan

    def get_vars(self, tps):
        variables = set()
        for t in tps:
            if not t.subject.constant:
                variables.add(t.subject.name)
            if not t.predicate.constant:
                variables.add(t.predicate.name)
            if not t.theobject.constant:
                variables.add(t.theobject.name)

        return variables


def finalize(processqueue):
    import os
    p = processqueue.get()

    while p != "EOF":
        try:
            os.kill(p, 9)
        except OSError as ex:
            pass
        p = processqueue.get()
