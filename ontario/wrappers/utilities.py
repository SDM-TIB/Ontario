__author__ = 'Kemele M. Endris'


def getVars(sg):

    s = []
    if not sg.subject.constant:
        s.append(sg.subject.name)
    if not sg.theobject.constant:
        s.append(sg.theobject.name)
    return s


def getPrefs(ps):
    prefDict = dict()
    for p in ps:
        pos = p.find(":")
        c = p[0:pos].strip()
        v = p[(pos+1):len(p)].strip()
        prefDict[c] = v
    return prefDict


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


def getTemplatesPerMolecule(molecules):
    moltemp = dict()
    for m in molecules:
        for t in molecules[m].templates:
            if m in moltemp:
                moltemp[m].append(t.pred)
            else:
                moltemp[m] = [t.pred]
    return moltemp


def aux(e ,x, op):

    def pp(t):
        return t.show(x + "  ")
    if type(e) == tuple:
        (f, s) = e
        r = ""
        if f:
            r = x + "{\n" + aux(f, x + "  ", op) + "\n" + x + "}\n"
        if f and s:
            r = r + x + op + "\n"
        if s:
            r = r + x + "{\n" + aux(s,x + "  ", op) + "\n" + x +"}"
        return r
    elif type(e) == list:
        return (x + " . \n").join(map(pp, e))
    elif e:
        return e.show(x + "  ")

    return ""


def aux2(e,x, op):
    def pp (t):
        return t.show2(x + "  ")
    if type(e) == tuple:
        (f, s) = e
        r = ""
        if f:
            r = x + "{\n" + aux2(f, x + "  ", op) + "\n" + x + "}\n"
        if f and s:
            r = r + x + op + "\n"
        if s:
            r = r + x + "{\n" + aux2(s, x + "  ", op) + "\n" + x + "}"
        return r
    elif type(e) == list:
        return (x + " . \n").join(map(pp, e))
    elif e:
        return e.show2(x + "  ")
    return ""


def nest(l):

    l0 = list(l)
    while len(l0) > 1:
        l1 = []
        while len(l0) > 1:
            x = l0.pop()
            y = l0.pop()
            l1.append((x,y))
        if len(l0) == 1:
            l1.append(l0.pop())
        l0 = l1
    if len(l0) == 1:
        return l0[0]
    else:
        return None


