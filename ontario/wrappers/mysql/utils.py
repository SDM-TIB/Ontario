
__author__ = 'Kemele M. Endris'

from ontario.model.rdfmt_model import *
from ontario.sparql.utilities import *


def var2map(mapping, rdfmt, starpredicates, triples, prefixes):

    coltotemplate = dict()
    res = dict()
    for s in mapping:
        if rdfmt not in mapping[s]:
            continue
        smap = mapping[s][rdfmt]
        subject = smap.subject
        predintersects = set(starpredicates).intersection(set(list(smap.predicateObjMap.keys())))
        if len(predintersects) == 0:
            continue
        # if len(predintersects) != len(set(starpredicates)):
        #     continue
        # TODO: for now only template subject types are supported.
        # Pls. include Reference (in case col values are uris) and Costants (in case collection is about one subject)
        if smap.subjectType == TermType.TEMPLATE or smap.subjectType == TermType.REFERENCE:
            varmap = {}
            predmaps = {}
            predobjConsts = {}
            subjectCols = smap.subjectCols
            matchingtriples = []
            for t in triples:
                if t.predicate.constant:
                    pred = getUri(t.predicate, prefixes)[1:-1]
                    if pred not in smap.predicateObjMap:
                        continue

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
                            rsubject = rsubject.replace(predobj.joinParent, rsubj)
                            pp = rsubj
                        else:
                            rsubject = mapping[tpm][rmol].subject
                            pp = mapping[tpm][rmol].subjectCols
                        coltotemplate[t.theobject.name[1:]] = rsubject
                    if pp is not None:
                        varmap[t.theobject.name] = pp
                        predmaps[pred] = pp

                if not t.subject.constant:
                    coltotemplate[t.subject.name[1:]] = subject
                # for subj in subjectCols:
                if t.subject.name not in varmap and not t.subject.constant:
                    varmap[t.subject.name] = subjectCols
                matchingtriples.append(t)
            if len(varmap) > 0:
                res.setdefault(smap.source+"_"+smap.iterator, {})['varmap'] = varmap
                res[smap.source+"_"+smap.iterator]['coltotemp'] = coltotemplate
                res[smap.source+"_"+smap.iterator]['subjcol'] = subjectCols
                res[smap.source+"_"+smap.iterator]['triples'] = matchingtriples
                res[smap.source+"_"+smap.iterator]['predmap'] = predmaps
                res[smap.source+"_"+smap.iterator]['predObjConsts'] = predobjConsts
                res[smap.source+"_"+smap.iterator]['iterator'] = smap.iterator
                res[smap.source + "_" + smap.iterator]['source'] = smap.source

    return res


def getFROMClause(tablename, alias):
    fromcaluse = tablename + " " + alias
    return fromcaluse


def getProjectionClause(variablemap, sparqlprojected, tablealias):
    projections = {}
    projvartocol = {}
    for var in sparqlprojected:
        if var in variablemap:
            if isinstance(variablemap[var], list):
                for column in variablemap[var]:
                    column = column.strip()
                    if '[' in column:
                        column = column[:column.find('[')]
                    if var[1:] in projections:
                        projections[var[1:]] += " , " + tablealias + ".`" + column + "` AS " + var[1:] + "_" + column
                    else:
                        projections[var[1:]] = tablealias + ".`" + column + "` AS " + var[1:] + "_" + column
                    projvartocol.setdefault(var[1:], []).append(column)
            else:
                # colname = variablemap[var].strip().split('[*]')
                col = variablemap[var].strip()
                if '[' in col:
                    column = col
                    column = "`" + column[:column.find('[')]
                    projvartocol[var[1:]] = col
                elif '/' in col and '[*]' not in col:
                    column = col.replace('/', '.')
                    column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
                    projvartocol[var[1:]] = col[col.find('/')+1:]
                else:
                    column = "`" + col + '`'
                    projvartocol[var[1:]] = col

                projections[var[1:]] = tablealias + "." + column + " AS " + var[1:]

    return projections, projvartocol


def getSubjectFilters(ifilters, tablealias):
    subjectfilters = []
    if len(ifilters) > 0:
        subjectfilters.append(tablealias + ".`" + ifilters[0][0].strip() + " = " + ' "' + ifilters[0][2] + '" ')
    else:
        return None

    return subjectfilters


def get_filters(triples, prefixes):
    filters = [(getUri(t.predicate, prefixes)[1:-1], " = ", getUri(t.theobject, prefixes)[1:-1])
               for t in triples if t.predicate.constant and t.theobject.constant and
               getUri(t.predicate, prefixes)[1:-1] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type']

    return filters


def get_pred_vars(triples, prefixes):
    predvars = [(getUri(t.predicate, prefixes)[1:-1], " = ", t.theobject.name)
                for t in triples if t.predicate.constant and not t.theobject.constant]
    return predvars


def get_matching_filters(triples, filters):
    result = []
    t_vars = []
    for t in triples:
        t_vars.extend(t.getVars())

    for f in filters:
        f_vars = f.getVars()
        if len(set(f_vars).intersection(t_vars)) == len(set(f_vars)):
            result.append(f)

    return result


def getObjectFilters(mappings, prefixes, triples, varmaps, tablealias, sparqlprojected, starfilters):
    objectfilters = []
    filtersmap = {}
    nans = []
    predmap = varmaps['predmap']
    subjcols = varmaps['subjcol']
    predvars = get_pred_vars(triples, prefixes)
    filters = get_filters(triples, prefixes)
    mtfilter = get_matching_filters(triples, starfilters)

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
            column = predmap[v].strip()
            subj = predmap[v].strip()
            if '[' in column:
                column = column[:column.find('[')]
            if '[' in column:
                objectfilters.append(tablealias + '.`' + column + " = " + ' "' + val + '" ')
                objectfilters.append(tablealias + '.' + (subj[subj.find('[') + 1: subj.find('=')]).strip() + " = " + ' "' + (
                    subj[subj.find('=') + 1: subj.find(']')]).strip() + '" ')
            elif '/' in subj and '[*]' not in subj:
                column = subj.replace('/', '.')
                column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
                objectfilters.append(tablealias + '.`' + column + "` = " + ' "' + val + '" ')
            else:
                objectfilters.append(tablealias + '.`' + column + "` = " + ' "' + val + '" ')
    for f in mtfilter:
        f_vars = f.getVars()

        for v in set(f_vars):
            prefix = ""
            if v in varmaps['coltotemp']:
                prefix = varmaps['coltotemp'][v].replace('{' + v + '}', "")
            left = f.expr.left.name
            right = f.expr.right.name
            if right is not None:
                if f.expr.left.constant and not f.expr.right.constant:
                    column = varmaps['varmap'][v].strip()
                    objectfilters.append(tablealias + '.`' + column + f.expr.op + ' ' + left.replace(prefix, "") + ' ')
                elif not f.expr.left.constant and f.expr.right.constant:
                    column = varmaps['varmap'][v].strip()
                    objectfilters.append(tablealias + '.`' + column + '` ' + f.expr.op + ' ' + right.replace(prefix, "") + ' ')

    for v in set(nans):
        if predmap[v] in subjcols:
            continue

        column = predmap[v].strip()
        subj = predmap[v].strip()

        if '[' in column:
            column = column[:column.find('[')]
            objectfilters.append(tablealias + '.`' + column + "`" + " is not null ")
            objectfilters.append(tablealias + '.`' + column + "`" + " <> '' ")

            objectfilters.append(tablealias + '.' + (subj[subj.find('[')+1: subj.find('=')]).strip() + " = " + ' "' + (subj[subj.find('=')+1: subj.find(']')]).strip() + '" ')
        elif '/' in subj and '[*]' not in subj:
            columns = [c for c in subj.strip().split('/') if len(c) > 0]

            column = subj.replace('/', '.')
            column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
            objectfilters.append(tablealias + '.' + column + " is not null ")
            objectfilters.append(tablealias + '.' + column + " <> '' ")
        else:
            objectfilters.append(tablealias + '.`' + column + "` is not null ")
            objectfilters.append(tablealias + '.`' + column + "` <> '' ")

    for subj in subjcols:
        column = subj.strip()
        subj = subj.strip()
        if '[' in column:
            column = column[:column.find('[')]
            objectfilters.append(tablealias + '.`' + column + "`" + " is not null ")
            objectfilters.append(tablealias + '.`' + column + "`" + " <> '' ")
            objectfilters.append(tablealias + '.' + (subj[subj.find('[') + 1: subj.find('=')]).strip() + " = " + ' "' + (
                subj[subj.find('=') + 1: subj.find(']')]).strip() + '" ')
        elif '/' in subj and '[*]' not in subj:
            columns = [c for c in subj.strip().split('/') if len(c) > 0]

            column = subj.replace('/', '.')
            column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
            objectfilters.append(tablealias + '.' + column + " is not null ")
            objectfilters.append(tablealias + '.' + column + " <> '' ")
        else:
            objectfilters.append(tablealias + '.`' + column + "` is not null ")
            objectfilters.append(tablealias + '.`' + column + "` <> '' ")

    return objectfilters
