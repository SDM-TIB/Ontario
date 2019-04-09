
__author__ = 'Kemele M. Endris'

from ontario.model.rdfmt_model import *
from ontario.sparql.utilities import *
from ontario.sparql.parser.services import *


def decomposeQuery(query):
    """
    decomposes a query to set of Triples and set of Filters
    :param query: sparql
    :return: triple composed of triplepatters, filters and optional
    """
    tp = []
    filters = []
    opts = []
    for b in query.body.triples:  # UnionBlock
        if isinstance(b, JoinBlock):
            for j in b.triples:  # JoinBlock
                if isinstance(j, Triple):
                    if j.subject.constant:
                        j.subject.name = getUri(j.subject, getPrefs(query.prefs))
                    if j.predicate.constant:
                        j.predicate.name = getUri(j.predicate, getPrefs(query.prefs))
                    if j.theobject.constant:
                        j.theobject.name = getUri(j.theobject, getPrefs(query.prefs))
                    tp.append(j)
                if isinstance(j, Filter):
                    filters.append(j)
                elif isinstance(j, Optional):
                    opts.append(j)
    return tp, filters, opts


def getFROMClause(subjectColumn, maxnumofobj):
    if maxnumofobj > 1:
        tables = [subjectColumn + " table" + str(i) for i in range(1, maxnumofobj + 1)]
        fromcaluse = " FROM " + ", ".join(tables)
    else:
        fromcaluse = " FROM " + subjectColumn
    return fromcaluse + " "


def getProjectionClause(variablemap, sparqlprojected, maxnumofobj):

    firstprojection = True
    projections = " SELECT "
    for var in sparqlprojected:
        if var in variablemap:
            column = variablemap[var]
            if not firstprojection:
                projections += ","

            if maxnumofobj > 1:
                projections += " table" + str(maxnumofobj) + "." + column + " AS " + var[1:]
            else:
                projections += " " + column + " AS " + var[1:]

            firstprojection = False
        #else:
        #    print sparqlprojected, var

    projections += " "

    return projections


def getSubjectFilters(ifilters, maxnumofobj):
    subjectfilters = ""
    firstfilter = True
    if len(ifilters) > 0:
        if maxnumofobj == 0:
            if not firstfilter:
                subjectfilters += ' AND '

            subjectfilters += ifilters[0][0] + " = " + ' "' + ifilters[0][2] + '" '
            firstfilter = False

        else:
            for i in range(1, maxnumofobj + 1):
                if not firstfilter:
                    subjectfilters += ' AND '

                subjectfilters += "table" + str(i) + "." + ifilters[0][0] + " = " + ' "' + ifilters[0][2] + '" '
                firstfilter = False

    else:
        return None, firstfilter

    return subjectfilters, firstfilter


def getPredObjDict(triplepatterns, prefixes):
    predobjdict = {}
    needselfjoin = False
    maxnumofobj = 0
    for t in triplepatterns:
        if t.predicate.constant:
            pred = getUri(t.predicate, prefixes)[1:-1]
            if pred in predobjdict:
                needselfjoin = True
                val = predobjdict[pred]
                if type(val) == list:
                    predobjdict[pred].append(t)
                else:
                    predobjdict[pred] = [val, t]
                if len(predobjdict[pred]) > maxnumofobj:
                    maxnumofobj = len(predobjdict[pred])
            else:
                predobjdict[pred] = t

    return predobjdict, needselfjoin, maxnumofobj


def getObjectFilters(mapping, triplepatterns, subjectVar, maxnumofobj, firstfilter, sparqlprojected):

    objectfilters = ""
    filtersmap = {}
    nans = []
    filters = [(t.predicate.name[1:-1], " = ", t.theobject.name[1:-1])
               for t in triplepatterns if t.predicate.constant and t.theobject.constant]

    predvars = [(t.predicate.name[1:-1], " = ", t.theobject.name)
                for t in triplepatterns if t.predicate.constant and not t.theobject.constant]

    if len(filters) == 0 and len(predvars) == 0:
        return None

    predmap = {t.predicate[1:-1]: t.refmap.name for t in mapping.predicates}
    for v in predvars:
        if v[0] not in predmap:
            continue

        if v[2] not in sparqlprojected:
            nans.append(v[0])

    for f in filters:
        if f[0] not in predmap:
            continue
        if f[0] in filtersmap:
            filtersmap[f[0]].append(f[2])
        else:
            filtersmap[f[0]] = [f[2]]

    for v in filtersmap:
        for idx, val in zip(range(1, len(filtersmap[v])+1), filtersmap[v]):
            if not firstfilter:
                objectfilters += ' AND '
            if maxnumofobj > 1:
                objectfilters += " table" + str(idx) + "." + predmap[v] + " = " + ' "' + val + '" '
            else:
                objectfilters += predmap[v] + " = " + ' "' + val + '" '

            firstfilter = False

    for v in set(nans):
        if not firstfilter:
            objectfilters += ' AND '

        if maxnumofobj > 1:
            objectfilters += " table" + str(maxnumofobj) + "." + predmap[v] + " is not null "

        else:
            objectfilters += ' ' + predmap[v] + " is not null "

        firstfilter = False

    joinvars = ""
    firstjoin = True
    if len(filters) > 0:
        for i in range(1, maxnumofobj+1):
            if maxnumofobj > 1 and i-1 > 0:
                if not firstjoin:
                    joinvars += " AND "

                joinvars += "table" + str(i-1) + "." + subjectVar + " = " + "table" + str(i) + "." + subjectVar
                firstjoin = False

        if not firstfilter and maxnumofobj > 1:
            objectfilters += " AND " + joinvars
        else:
            objectfilters += " " + joinvars + " "

    return objectfilters, firstfilter


def getSparqlFilters(filters, mapping, maxnumofobj, firstfilter, varmap):
    op = filters.expr.op
    left = filters.expr.left
    right = filters.expr.right
    simplefilters = ""
    predmap = {t.predicate[1:-1]: t.refmap.name for t in mapping.predicates}
    numoperators = ['<', '>', '>=', '<=']
    if isinstance(left, Argument) and isinstance(right, Argument):
        if not left.constant and right.constant:
            if not firstfilter:
                simplefilters += ' AND '

            if "<" in right.name and ">" in right.name:
                val = right.name[1:-1]

            else:
                val = right.name

            if maxnumofobj > 1:
                simplefilters += " table" + str(maxnumofobj) + "." + varmap[left.name] + ' ' \
                                 + str(op) + (' ' if str(op) in numoperators else ' "') + val \
                                 + ('' if str(op) in numoperators else '" ')

            else:
                simplefilters += varmap[left.name] + ' ' + str(op) + (' ' if str(op) in numoperators else ' "') +\
                                 val + ('' if str(op) in numoperators else '" ')
        elif left.constant and not right.constant:
            if not firstfilter:
                simplefilters += ' AND '

            if "<" in left.name and ">" in left.name:
                val = left.name[1:-1]

            else:
                val = left.name

            if maxnumofobj > 1:
                simplefilters += " table" + str(maxnumofobj) + "." + varmap[right.name] + ' ' + str(op) + (' ' if str(op) in numoperators else ' "') + val + ('' if str(op) in numoperators else '" ')

            else:
                simplefilters += varmap[right.name] + ' ' + str(op) + (' ' if str(op) in numoperators else ' "') + val + ('' if str(op) in numoperators else '" ')
    else:
        return None

    return simplefilters


def var2map(mapping, rdfmt, starpredicates, triples, prefixes):

    coltotemplate = dict()
    res = dict()
    for s in mapping:
        if rdfmt not in mapping[s]:
            continue
        smap = mapping[s][rdfmt]
        subject = smap.subject
        predintersects = set(starpredicates).intersection(set(list(smap.predicateObjMap.keys())))
        if len(predintersects) != len(set(starpredicates)):
            continue
        # TODO: for now only template subject types are supported.
        # Pls. include Reference (in case col values are uris) and Costants (in case collection is about one subject)
        if smap.subjectType == TermType.TEMPLATE:
            varmap = {}
            predmaps = {}
            predobjConsts = {}
            subjectCols = smap.subjectCols

            for t in triples:
                if not t.subject.constant:
                    coltotemplate[t.subject.name[1:]] = subject
                for subj in subjectCols:
                    if subj not in varmap and not t.subject.constant:
                        varmap[t.subject.name] = str(subj)
                if t.predicate.constant and not t.theobject.constant:
                    pred = getUri(t.predicate, prefixes)[1:-1]
                    predobj = smap.predicateObjMap[pred]

                    if predobj.objectType == TermType.REFERENCE:
                        pp = predobj.object
                    elif predobj.objectType == TermType.TEMPLATE:
                        pp = predobj.object[predobj.object.find('{') + 1: predobj.object.find('}')]

                    elif predobj.objectType == TermType.CONSTANT:
                        predobjConsts[pred] = predobj.object
                        continue
                    else:
                        tpm = predobj.object
                        rmol = list(mapping[tpm].keys())[0]
                        rsubject = mapping[tpm][rmol].subject

                        rsubj = mapping[tpm][rmol].joinChild
                        rsubject = rsubject.replace(mapping[tpm][rmol].joinParent, rsubj)
                        pp = rsubj
                        coltotemplate[t.theobject.name[1:]] = rsubject
                    if pp is not None:
                        varmap[t.theobject.name] = pp
                        predmaps[pred] = pp

            if len(varmap) > 0:
                res.setdefault(smap.source, {})['varmap'] = varmap
                res[smap.source]['coltotemp'] = coltotemplate
                res[smap.source]['subjcol'] = subjectCols
                res[smap.source]['triples'] = triples
                res[smap.source]['predmap'] = predmaps
                res[smap.source]['predObjConsts'] = predobjConsts

    return res


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


def vartocolumnmapping(mapping, triplepatterns, rdfmts, prefixes):
    res = dict()
    coltotemplate = dict()
    subjcols = dict()
    dbcols = dict()
    starcollects = dict()
    molsubj = dict()
    for s in mapping:
        for m in mapping[s]:
            if m in rdfmts:
                subject = mapping[s][m]['subject']
                db, col = mapping[s][m]['ls']['iterator'].split('/')
                dbcols[m] = {'db': db, 'col': col}
                varmap = dict()
                if mapping[s][m]['subjtype'] == TermType.TEMPLATE:
                    subj = mapping[s][m]['subjectCol']
                    predicates = mapping[s][m]['predConsts']
                    predObjMap = mapping[s][m]['predObjMap']
                    predmap = {p: predObjMap[p] for p in predicates}

                    coltotemplate[subj] = subject
                    subjcols[subj] = subject
                    starcollects.setdefault(mapping[s][m]['ls']['iterator'], []).append(subj)

                    for t in triplepatterns:
                        if subj not in varmap and not t.subject.constant:
                            varmap[t.subject.name] = str(subj)
                        if t.predicate.constant and not t.theobject.constant:
                            pred = getUri(t.predicate, prefixes)[1:-1]
                            pp = [predmap[p]['object'] for p in predmap if p == pred and predmap[p]['objType'] == TermType.REFERENCE]
                            pp.extend([predmap[p]['object'][predmap[p]['object'].find('{') + 1: predmap[p]['object'].find('}')] for p in predmap if p == pred and predmap[p]['objType'] == TermType.TEMPLATE])
                            tpm = [predmap[p]['object'] for p in predmap if p == pred and predmap[p]['objType'] == TermType.TRIPLEMAP]
                            for tp in tpm:
                                rmol = list(mapping[tp].keys())[0]
                                rsubject = mapping[tp][rmol]['subject']
                                rsubj = rsubject[rsubject.find('{') + 1: rsubject.find('}')]
                                pp.append(rsubj)
                                coltotemplate[rsubj] = rsubject
                            if len(pp) > 0:
                                varmap[t.theobject.name] = pp[0]
                            else:
                                varmap = {}
                                break
                    if len(varmap) > 0:
                        res[m] = varmap
                        #molsubj.setdefault(m, []).append(subj)
    return res, coltotemplate, subjcols, dbcols, starcollects
