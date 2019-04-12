
__author__ = 'Kemele M. Endris'

from ontario.model.rdfmt_model import *
from ontario.sparql.utilities import *


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


def var2map(mapping, rdfmt, starpredicates, triples, prefixes):

    coltotemplate = dict()
    res = dict()
    relationprops = {}
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
        if smap.subjectType == TermType.TEMPLATE or smap.subjectType == TermType.REFERENCE:
            varmap = {}
            predmaps = {}
            predobjConsts = {}
            subjectCols = smap.subjectCols

            for t in triples:
                if not t.subject.constant:
                    coltotemplate[t.subject.name[1:]] = subject
                # for subj in subjectCols:
                if t.subject.name not in varmap and not t.subject.constant:
                    varmap[t.subject.name] = subjectCols
                if t.predicate.constant:
                    pred = getUri(t.predicate, prefixes)[1:-1]
                    predobj = smap.predicateObjMap[pred]

                    if predobj.objectType == TermType.REFERENCE:
                        pp = predobj.object
                    elif predobj.objectType == TermType.TEMPLATE:
                        pp = predobj.object[predobj.object.find('{') + 1: predobj.object.find('}')]
                        coltotemplate[t.theobject.name[1:]] = predobj.object
                    elif predobj.objectType == TermType.CONSTANT:
                        predobjConsts[pred] = predobj.object
                        continue
                    else:

                        tpm = predobj.object
                        rmol = list(mapping[tpm].keys())[0]
                        rsubject = mapping[tpm][rmol].subject

                        if predobj.joinChild is not None:
                            rsubj = predobj.joinChild
                            relationprops[rsubj] = mapping[tpm][rmol].source
                            rsubject = rsubject.replace(predobj.joinParent, rsubj)
                            pp = rsubj
                        else:
                            rsubject = mapping[tpm][rmol].subject
                            pp = mapping[tpm][rmol].subjectCols
                            relationprops.update({p: mapping[tpm][rmol].source for p in pp})
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
                res[smap.source]['relationprops'] = relationprops

    return res


def getReturnClause(mappings, variablemap, sparqlprojected, relationprops):
    firstprojection = True
    projections = " RETURN "
    projFilter = []
    projvartocol = {}
    for var in sparqlprojected:
        if var in variablemap:
            if isinstance(variablemap[var], list):
                for column in variablemap[var]:
                    column = column.strip()

                    if not firstprojection:
                        projections += ","
                    projvartocol.setdefault(var[1:], []).append(column)
                    if column not in relationprops:
                        projections += " n." + column + " AS " + var[1:] + "_" + column
                        projFilter.append('EXISTS(n.' + column + ")")
                        projFilter.append('n.' + column + " IS NOT NULL ")
                    else:
                        rel = relationprops[column]
                        mapping = [mappings[s][mt] for s in mappings for mt in mappings[s] if mappings[s][mt].source == rel]
                        if len(mapping) > 0:
                            mapping = mapping[0]
                            if len(mapping.subjectCol) > 1:
                                subjs = {column: " " + rel + "_" + column + "." + s for s in mapping.subjectCol}
                                projections += " collect(" + str(subjs) + ") AS " + var[1:] + "_" + column
                                for s in mapping.subjectCol:
                                    projFilter.append('EXISTS(' + rel + "_" + column + '.' + s + ")")
                                    projFilter.append(
                                        " " + rel + "_" + column + '.' + s + " IS NOT NULL ")
                            else:
                                projections += " " + rel + "_" + column + "." + mapping.subjectCol[0] + " AS " + var[1:] + "_" + column
                                projFilter.append('EXISTS(' + rel + "_" + column + '.' + mapping.subjectCol[0] + ")")
                                projFilter.append(" " + rel + "_" + column + '.' + mapping.subjectCol[0] + " IS NOT NULL ")
                    firstprojection = False
            else:
                column = variablemap[var].strip()
                if not firstprojection:
                    projections += ","
                projvartocol[var[1:]] = column
                if column not in relationprops:
                    projections += " n." + column + " AS " + var[1:]
                    projFilter.append('EXISTS(n.' + column + ")")
                    projFilter.append('n.' + column + " IS NOT NULL ")
                else:
                    rel = relationprops[column]
                    mapping = [mappings[s][mt] for s in mappings for mt in mappings[s] if mappings[s][mt].source == rel]
                    if len(mapping) > 0:
                        mapping = mapping[0]
                        if len(mapping.subjectCols) > 1:
                            subjs = {column: " " + rel + "_" + column + "." + s for s in mapping.subjectCols}
                            projections += " colelct(" + str(subjs) + ") AS " + var[1:]
                            for s in mapping.subjectCol:
                                projFilter.append('EXISTS(' + rel + "_" + column + '.' + s + ")")
                                projFilter.append(
                                    " " + rel + "_" + column + '.' + s + " IS NOT NULL ")
                        else:
                            projections += " " + rel + "_" + column + "." + mapping.subjectCols[0] + " AS " + var[1:]
                            projFilter.append('EXISTS(' + rel + "_" + column + '.' + mapping.subjectCols[0] + ")")
                            projFilter.append(" " + rel + "_" + column + '.' + mapping.subjectCols[0] + " IS NOT NULL ")

                firstprojection = False

    projections += " "

    return projections, projvartocol, projFilter


def getSubjectFilters(ifilters, maxnumofobj):
    subjectfilters = ""
    firstfilter = True
    if len(ifilters) > 0:
        if not firstfilter:
            subjectfilters += ' AND '

        subjectfilters += ifilters[0][0].strip() + " = " + ' "' + ifilters[0][2] + '" '
        firstfilter = False

    else:
        return "", firstfilter

    return subjectfilters, firstfilter


def get_filters(triples, prefixes):
    filters = [(getUri(t.predicate, prefixes)[1:-1], " = ", getUri(t.theobject, prefixes)[1:-1])
               for t in triples if t.predicate.constant and t.theobject.constant and
               getUri(t.predicate, prefixes)[1:-1] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type']

    return filters


def get_pred_vars(triples, prefixes):
    predvars = [(getUri(t.predicate, prefixes)[1:-1], " = ", t.theobject.name)
                for t in triples if t.predicate.constant and not t.theobject.constant]
    return predvars


def getObjectFilters(mappings, prefixes, triples, varmaps, maxnumofobj, sparqlprojected, query):
    objectfilters = []
    filtersmap = {}
    nans = []
    nontnulls = []
    predmap = varmaps['predmap']
    subjcols = varmaps['subjcol']
    print(varmaps['relationprops'])
    predvars = get_pred_vars(triples, prefixes)
    filters = get_filters(triples, prefixes)
    firstfilter = True
    if len(filters) == 0 and len(predvars) == 0:
        return None

    for v in predvars:
        if v[0] not in predmap:
            continue

        if v[2] in sparqlprojected:
            nans.append(v[0])

    for f in filters:
        if f[0] not in predmap:
            continue
        if f[0] in filtersmap:
            filtersmap[f[0]].append(f[2])
        else:
            filtersmap[f[0]] = [f[2]]

    for v in filtersmap:
        for idx, val in zip(range(1, len(filtersmap[v]) + 1), filtersmap[v]):
            if isinstance(predmap[v], list):
                for col in predmap[v]:
                    if col not in varmaps['relationprops']:
                        col = col.strip()
                        objectfilters.append('EXISTS(n.' + col + ")")
                        objectfilters.append(' n.' + col + " = " + ' "' + val + '" ')
                    else:
                        rel = varmaps['relationprops'][predmap[v]]
                        mapping = [mappings[s][mt] for s in mappings for mt in mappings[s] if mappings[s][mt].source == rel]
                        if len(mapping) > 0:
                            mapping = mapping[0]
                            if len(mapping.subjectCols) > 1:
                                column = col.strip()
                                subjs = {column: " " + rel + "_" + column + "." + s for s in mapping.subjectCols}

                                objectfilters.append(' ' + rel + "_" + col.strip() + '.' + mapping.subjectCols[0].strip() + " = " + ' "' + val + '" ')
                            else:
                                objectfilters.append(' ' + rel + "_" + col.strip() + '.' + mapping.subjectCols[0].strip() + " = " + ' "' + val + '" ')
            else:
                if predmap[v] not in varmaps['relationprops']:
                    objectfilters.append('EXISTS(n.' + predmap[v].strip() + ")")
                    objectfilters.append(' n.' + predmap[v].strip() + " = " + ' "' + val + '" ')
                else:
                    rel = varmaps['relationprops'][predmap[v]]
                    mapping = [mappings[s][mt] for s in mappings for mt in mappings[s] if mappings[s][mt].source == rel]
                    if len(mapping) > 0:
                        mapping = mapping[0]
                        if len(mapping.subjectCols) > 1:
                            column = predmap[v].strip()
                            subjs = {column: " " + rel + "_" + column + "." + s for s in mapping.subjectCols}

                            objectfilters.append(' ' + rel + "_" + column + '.' + mapping.subjectCols[
                                0].strip() + " = " + ' "' + val + '" ')
                        else:
                            objectfilters.append(' ' + rel + "_" + predmap[v].strip() + '.' + mapping.subjectCols[0].strip() + " = " + ' "' + val + '" ')

    for v in set(nans):
        if isinstance(predmap[v], list):
            for col in predmap[v]:
                if col in subjcols:
                    continue
                if col not in varmaps["relationprops"]:
                    nontnulls.append('EXISTS(n.' + col + ")")
                    nontnulls.append('n.' + col.strip() + " IS NOT NULL ")
                else:
                    rel = varmaps['relationprops'][predmap[v]]
                    mapping = [mappings[s][mt] for s in mappings for mt in mappings[s] if mappings[s][mt].source == rel]
                    if len(mapping) > 0:
                        mapping = mapping[0]
                        if len(mapping.subjectCols) > 1:
                            column = col.strip()
                            for s in mapping.subjectCols:
                                nontnulls.append(
                                    ' ' + rel + "_" + col.strip() + '.' + s + " IS NOT NULL ")
                        else:
                            nontnulls.append(' ' + rel + "_" + col.strip() + '.' + mapping.subjectCols[0] + " IS NOT NULL ")

        else:
            if predmap[v] in subjcols:
                continue
            if predmap[v] not in varmaps["relationprops"]:
                nontnulls.append('EXISTS(n.' + predmap[v].strip() + ")")
                nontnulls.append('n.' + predmap[v].strip() + " IS NOT NULL ")
            else:
                rel = varmaps['relationprops'][predmap[v]]
                mapping = [mappings[s][mt] for s in mappings for mt in mappings[s] if mappings[s][mt].source == rel]
                if len(mapping) > 0:
                    mapping = mapping[0]
                    if len(mapping.subjectCols) > 1:
                        column = predmap[v].strip()
                        for s in mapping.subjectCol:
                            nontnulls.append(
                                ' ' + rel + "_" + column + '.' + s + " IS NOT NULL ")
                    else:
                        nontnulls.append(' ' + rel + "_" + predmap[v].strip() + '.' + mapping.subjectCols[0] + " IS NOT NULL ")

    for subj in subjcols:
        if isinstance(subj, list):
            for s in subj:
                nontnulls.append('EXISTS(n.' + s.strip() + ")")
                nontnulls.append('n.' + s.strip() + " IS NOT NULL ")
        else:
            nontnulls.append('EXISTS(n.' + subj.strip() + ")")
            nontnulls.append('n.' + subj.strip() + " IS NOT NULL ")

    return objectfilters, nontnulls


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
