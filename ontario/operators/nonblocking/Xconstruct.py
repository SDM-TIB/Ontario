
from multiprocessing import Queue


class Xconstruct(object):

    def __init__(self, triples, prefixes, limit=-1):
        self.input = Queue()
        self.qresults = Queue()
        self.triples = triples
        self.prefixes = prefixes
        self.limit = int(limit)
        self.vars = []
        for t in self.triples:
            self.vars.extend(t.getVars())
            self.vars.extend(t.getPredVars())
        self.vars = list(set(self.vars))

    def execute(self, left, dummy, out, processqueue=Queue()):
        self.left = left
        self.qresults = out
        tuple = self.left.get(True)
        i =0
        while not(tuple == "EOF"):
            res = {}
            for var in self.vars:
                var = var[1:]
                aux = tuple.get(var, None)
                res.update({var: aux})
            result = self.get_template_impl(res)
            self.qresults.put(".\n".join(result) + '.')
            i += 1
            if self.limit > 0 and i >= self.limit:
                break

            tuple = self.left.get(True)

        # Put EOF in queue and exit.
        self.qresults.put("EOF")
        return

    def get_template_impl(self, res):
        result = []
        for t in self.triples:
            if t.subject.constant:
                subj = getUri(t.subject, self.prefixes)
            else:
                subj = res.get(t.subject.name[1:], None)
                if subj is not None and is_url(subj):
                    subj = '<' + subj + '>'

            if t.predicate.constant:
                pred = getUri(t.predicate, self.prefixes)
            else:
                pred = res.get(t.predicate.name[1:], None)
                if pred is not None:
                    pred = '<' + pred + '>'

            if t.theobject.constant:
                obj = getUri(t.theobject, self.prefixes)
            else:
                obj = res.get(t.theobject.name[1:], None)
                if obj is not None:
                    if is_url(obj):
                        obj = '<' + obj + '>'
                    else:
                        obj = '"' + obj + '"'

            if subj is None or pred is None or obj is None:
                continue
            else:
                val = subj + " " + pred + " " + obj
                result.append(val)

        return result


def is_url(val):
    if len(val) >= 6:
        if 'http' in val[:6] or 'https' in val[:7]:
            return True
    return False

def getUri(p, prefs):
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

