
__author__ = 'Kemele M. Endris'

from ontario.model.rdfmt_model import *
from ontario.sparql.utilities import *
from ontario.sparql.parser.services import Expression, Argument


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
                        if t.theobject.constant:
                            if '<' in t.theobject.name or '"' in t.theobject.name or "'" in t.theobject.name:
                                colnval = t.theobject.name[1:-1]
                            else:
                                colnval = t.theobject.name
                        else:
                            colnval = t.theobject.name[1:]
                        coltotemplate[colnval] = predobj.object
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

                        if t.theobject.constant:
                            if '<' in t.theobject.name or '"' in t.theobject.name or "'" in t.theobject.name:
                                colnval = t.theobject.name[1:-1]
                            else:
                                colnval = t.theobject.name
                        else:
                            colnval = t.theobject.name[1:]
                        coltotemplate[colnval] = rsubject
                    if pp is not None:
                        varmap[t.theobject.name] = pp
                        predmaps[pred] = pp

                if not t.subject.constant:
                    if t.subject.constant:
                        if '<' in t.subject.name or '"' in t.subject.name or "'" in t.subject.name:
                            colnval = t.subject.name[1:-1]
                        else:
                            colnval = t.subject.name
                    else:
                        colnval = t.subject.name[1:]
                    coltotemplate[colnval] = subject
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


def getPredObjDict(triplepatterns, query):
    predobjdict = {}
    needselfjoin = False
    maxnumofobj = 0
    prefixes = getPrefs(query.prefs)
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


def getFROMClause(tablename, alias):
    fromcaluse = tablename + " " + alias
    return fromcaluse


def getSubjectFilters(ifilters, tablealias):
    subjectfilters = []
    if len(ifilters) > 0:
        subjectfilters.append(tablealias + ".`" + ifilters[0][0].strip() + " = " + ' "' + ifilters[0][2] + '" ')
    else:
        return None

    return subjectfilters


def getProjectionClause(variablemap, sparqlprojected, tablealias):
    projections = {}
    projvartocol = {}
    for var in sparqlprojected:
        if var in variablemap:
            if isinstance(variablemap[var], list):
                for column in variablemap[var]:
                    column = column.strip()
                    if var[1:] in projections:
                        projections[var[1:]] += " , " + tablealias + ".`" + column + "` AS " + var[1:] + "_" + column.replace(' ', '')
                    else:
                        projections[var[1:]] = tablealias + ".`" + column + "` AS " + var[1:] + "_" + column.replace(' ', '')
                    projvartocol.setdefault(var[1:], []).append(column)
            else:
                # colname = variablemap[var].strip().split('[*]')
                col = variablemap[var].strip()
                if '[' in col:
                    column = col
                    column = "`" + column[:column.find('[')] + ("`._VALUE" if '_VALUE' not in column else "")
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


def getExplodeProjectionClause(variablemap, sparqlprojected, maxnumofobj):
    firstprojection = True
    projections = " SELECT "
    projvartocol = {}
    for var in sparqlprojected:
        if var in variablemap:
            if not firstprojection:
                projections += ","
            col = variablemap[var].strip()
            if '[' in variablemap[var].strip():
                column = variablemap[var].strip()
                col = "`" + column[:column.find('[')] + "`"
                column = "explode(`" + column[:column.find('[')] + "`)"
            elif '/' in variablemap[var].strip() and '[*]' not in variablemap[var].strip():
                subj = variablemap[var].strip()
                column = "`" + subj[:subj.find('/')] + '`'
                col = subj[:subj.find('/')]
                # col = "`" + col[col.find('/')+1:] + "`"
                # projvartocol[var[1:]] = col
            else:
                column = "`" + variablemap[var].strip() + '`'

            if maxnumofobj > 1:
                projections += " table" + str(maxnumofobj) + "." + column + " AS " + col + ''
            else:
                projections += " " + column + " AS " + col + ''

            firstprojection = False
            # else:
            #    print sparqlprojected, var

    projections += " "

    return projections, projvartocol


def vartocolumnmapping(mapping, triplepatterns, rdfmts, prefixes):
    res = dict()
    coltotemplate = dict()
    subjcols = dict()
    for s in mapping:
        for m in rdfmts:
            if m in mapping[s]:
                subject = mapping[s][m]['subject']
                varmap = dict()
                if mapping[s][m]['subjtype'] == TermType.TEMPLATE:
                    subj = subject[subject.find('{') + 1: subject.find('}')]
                    predicates = mapping[s][m]['predConsts']
                    predObjMap = mapping[s][m]['predObjMap']
                    predmap = {p: predObjMap[p] for p in predicates}
                    coltotemplate[subj] = subject
                    subjcols[subj] = subject
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

    return res, coltotemplate, subjcols


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
    schema = {}
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
            if val in varmaps['coltotemp']:
                prefix = varmaps['coltotemp'][val].replace('{' + column + '}', "")
                val = val.replace(prefix, "")
            if '[' in column:
                column = column[:column.find('[')]
            if '[' in column:
                objectfilters.append(tablealias + '.`' + column + "`._VALUE" + " = " + ' "' + val + '" ')
                objectfilters.append(tablealias + '.`' + column + '`._' + (subj[subj.find('@') + 1: subj.find('=')]).strip() + " = " + ' "' + (
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
            if isinstance(f.expr.left, Argument) and isinstance(f.expr.right, Argument):
                left = f.expr.left.name
                right = f.expr.right.name
                if right is not None:
                    if f.expr.left.constant and not f.expr.right.constant:
                        column = varmaps['varmap'][v].strip()
                        objectfilters.append(tablealias + '.`' + column + f.expr.op + ' ' + left.replace(prefix, "") + ' ')
                    elif not f.expr.left.constant and f.expr.right.constant:
                        column = varmaps['varmap'][v].strip()
                        objectfilters.append(tablealias + '.`' + column + '` ' + f.expr.op + ' ' + right.replace(prefix, "") + ' ')
                else:
                    pass  # do check unary operators
            else:
                leftop = None
                rightop = None
                fil = None
                opr = f.expr.op
                if isinstance(f.expr.left, Expression):
                    left = f.expr.left.left.name
                    right = f.expr.left.right.name
                    if right is not None:
                        if f.expr.left.left.constant and not f.expr.left.right.constant:
                            column = varmaps['varmap'][v].strip()
                            objectfilters.append(
                                tablealias + '.`' + column + f.expr.left.op + ' ' + left.replace(prefix, "") + ' ')
                        elif not f.expr.left.left.constant and f.expr.left.right.constant:
                            column = varmaps['varmap'][v].strip()
                            leftop = tablealias + '.`' + column + '` ' + f.expr.left.op + ' ' + right.replace(prefix, "") + ' '
                if isinstance(f.expr.right, Expression):
                    left = f.expr.right.left.name
                    right = f.expr.right.right.name
                    if right is not None:
                        if f.expr.right.left.constant and not f.expr.right.right.constant:
                            column = varmaps['varmap'][v].strip()
                            objectfilters.append(
                                tablealias + '.`' + column + f.expr.right.op + ' ' + left.replace(prefix, "") + ' ')
                        elif not f.expr.right.left.constant and f.expr.right.right.constant:
                            column = varmaps['varmap'][v].strip()
                            rightop = tablealias + '.`' + column + '` ' + f.expr.right.op + ' ' + right.replace(
                                prefix, "") + ' '
                if leftop is not None:
                    if rightop is not None:
                        if opr.strip() == '||' or opr.strip() == '|':
                            fil = '(' + leftop + ' OR ' + rightop + ')'
                        elif opr.strip() == "&&" or opr.strip() == '&':
                            fil = leftop + ' AND ' + rightop
                        else:
                            fil = leftop + ' AND ' + rightop
                    else:
                        fil = leftop
                elif rightop is not None:
                    fil = rightop

                if fil is not None:
                    objectfilters.append(fil)

    for v in set(nans):
        if predmap[v] in subjcols:
            continue

        column = predmap[v].strip()
        subj = predmap[v].strip()
        if '[' in column:
            column = column[:column.find('[')]

        if '[' in column:
            schema[column] = column
            objectfilters.append(tablealias + '.`' + column + "`._VALUE" + " is not null ")
            objectfilters.append(tablealias + '.`' + column + '`._' + (subj[subj.find('@')+1: subj.find('=')]).strip() + " = " + ' "' + (subj[subj.find('=')+1: subj.find(']')]).strip() + '" ')
        elif '/' in subj and '[*]' not in subj:
            columns = [c for c in subj.strip().split('/') if len(c) > 0]
            if len(columns) > 1:
                nested = getnextStruct(columns)
                if isinstance(nested, dict):
                    schema.setdefault(columns[0], {}).update(nested)
                else:
                    schema[columns[0]] = nested
            else:
                schema[columns[0]] = columns[0]

            column = subj.replace('/', '.')
            column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
            objectfilters.append(tablealias + '.' + column + " is not null ")
        else:
            schema[column] = column
            objectfilters.append(tablealias + '.`' + column + "` is not null ")

    for subj in subjcols:
        column = subj.strip()
        subj = subj.strip()
        if '[' in column:
            column = column[:column.find('[')]
            schema[column] = column
            objectfilters.append(tablealias + '.`' + column + "`._VALUE" + " is not null ")
            objectfilters.append(tablealias + '.`' + column + '`._' + (subj[subj.find('@') + 1: subj.find('=')]).strip() + " = " + ' "' + (
                subj[subj.find('=') + 1: subj.find(']')]).strip() + '" ')
        elif '/' in subj and '[*]' not in subj:
            columns = [c for c in subj.strip().split('/') if len(c) > 0]
            if len(columns) > 1:
                nested = getnextStruct(columns)
                if isinstance(nested, dict):
                    schema.setdefault(columns[0], {}).update(nested)
                else:
                    schema[columns[0]] = nested
            else:
                schema[columns[0]] = columns[0]

            column = subj.replace('/', '.')
            column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
            objectfilters.append(tablealias + '.' + column + " is not null ")
        else:
            schema[column] = column
            objectfilters.append(tablealias + '.`' + column + "` is not null ")

    return objectfilters, schema


def getLVFROMClause(tablename, alias):
    fromcaluse = tablename
    return fromcaluse


def addprojection(colnames, variablemap, projvartocol, projections, wherenotnull, lateralviews, schema, var, l):
    conds = {}

    if len(colnames) == 1:
        if isinstance(variablemap[var], list):
            columns = [c for c in variablemap[var][0].strip().split('/') if len(c) > 0]
        else:
            columns = [c for c in variablemap[var].strip().split('/') if len(c) > 0]

        if len(columns) > 1:
            nested = getnextStruct(columns)
            if isinstance(nested, dict):
                schema.setdefault(columns[0], {}).setdefault(columns[0], {}).update(nested)
            else:
                schema[columns[0]] = nested
        else:
            schema[columns[0]] = columns[0]

        column = "`" + '`.`'.join(columns) + '`'
        if isinstance(variablemap[var], list):
            projvartocol[var[1:]] = variablemap[var][0].strip().replace('/', '.')
        else:
            projvartocol[var[1:]] = variablemap[var].strip().replace('/', '.')
        pp = ""
        if l == 1:
            pp = "_" + column.replace("`", '').replace('.', '_').replace(" ", '')
        if var[1:] in projections:
            projections[var[1:]] += ", " + column + " AS `" + var[1:] + pp + '`'
        else:
            projections[var[1:]] = column + " AS `" + var[1:] + pp + '`'

        wherenotnull.append(column + " is not null ")
    elif len(colnames) == 2:
        val = colnames[1].strip()
        columns = [c for c in colnames[0].strip().split('/') if len(c) > 0]
        if len(columns) > 1:
            nested = getnextStruct(columns)
            if isinstance(nested, dict):
                schema.setdefault(columns[0], {}).update(nested)
            else:
                schema[columns[0]] = nested
        else:

            if '[' in val:
                values = val[val.find(']')+1:].split('/')
                if '=' in val:
                    col = val[1:-1].split('=')[0]
                    schema[columns[0]] = {columns[0]: [col, "_VALUE" if len(values) == 1 else values[1]]}
            else:
                schema[columns[0]] = columns[0]

        column = "`" + '_'.join(columns) + '`'
        columnexp = "`" + '`.`'.join(columns) + '`'
        lateralviews['`exp_' + column[1:]] = "explode(" + columnexp + ")"
        column = '`' + 'exp_' + column[1:]
        if len(val) > 0:
            if '[' not in val:
                columns = [c for c in val.split('/') if len(c) > 0]
                col = "`" + '`.`'.join(columns) + '`'
                column += "." + col
            else:
                columns = [c for c in val.split('/') if len(c) > 0]
                cols = []

                for c in columns:
                    if c[0] == '[' and c[-1] == ']':
                        conds.setdefault(column, []).append(c[1:-1])
                    else:
                        cols.append(c)

                if len(cols) > 0:
                    col = "`" + '`.`'.join(cols) + '`'
                    column += "." + col
                if len(cols) == 0 and "_VALUE" not in column:
                    column += "._VALUE"

        pp = ""
        if l == 1:
            pp = "_" + column.replace("`", '').replace('.', '_').replace(" ", '')

        if var[1:] in projections:
            projections[var[1:]] = column + " AS `" + var[1:] + pp + '`'
        else:
            projections[var[1:]] = column + " AS `" + var[1:] + pp + '`'

        wherenotnull.append(column + " is not null ")
        if l == 1:
            projvartocol.setdefault(var[1:], []).append(column.replace('`', '').replace('.', '_'))
        else:
            projvartocol[var[1:]] = column.replace('`', '').replace('.', '_')

    if len(conds) > 0:
        for c in conds:
            for d in conds[c]:
                cceq = d.split('=')
                ccneq = d.split('!=')
                if len(cceq) > 1:
                    if cceq[1] != 'null':
                        wherenotnull.append(c + '.' + cceq[0].strip() + '=' + "\"" + cceq[1].strip() + '"')
                elif len(ccneq) > 1:
                    wherenotnull.append(c + '.' + ccneq[0].strip() + '!=' + "\"" + ccneq[1].strip() + '"')


def getLVProjectionClause(variablemap, sparqlprojected, tablealias):
    projections = {}
    projvartocol = {}
    lateralviews = {}
    wherenotnull = []
    schema = {}
    for var in sparqlprojected:
        if var in variablemap:
            if isinstance(variablemap[var], list):
                for column in variablemap[var]:
                    colnames = column.strip().split('[*]')
                    addprojection(colnames, variablemap, projvartocol, projections, wherenotnull, lateralviews, schema, var, 1)

            else:
                if isinstance(variablemap[var], list):
                    colnames = variablemap[var][0].strip().split('[*]')
                else:
                    colnames = variablemap[var].strip().split('[*]')
                addprojection(colnames, variablemap, projvartocol, projections, wherenotnull, lateralviews, schema, var, 0)

    return projections, wherenotnull, projvartocol, lateralviews, schema


def getnextStruct(cols):
    if len(cols) == 1:
        return cols[0]
    else:
        return {cols[0]: getnextStruct(cols[1:])}


def getnextArray(cols):
    if len(cols) == 1:
        return cols[0]
    else:
        return {cols[0]: [getnextArray(cols[1])]}


def getLVObjectFilters(prefixes, triples, varmaps, tablealias, sparqlprojected):
    objectfilters = []
    filtersmap = {}
    projvartocol = {}
    lateralviews = {}
    schema = {}
    nans = []
    predmap = varmaps['predmap']
    subjcols = varmaps['subjcol']

    predvars = get_pred_vars(triples, prefixes)
    filters = get_filters(triples, prefixes)
    predvarmap = {v[0]: v[2] for v in predvars}
    if len(filters) == 0 and len(predvars) == 0:
        return None

    for v in predvars:
        if v[0] not in predmap:
            continue

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
                objectfilters.append('`' + column + "`._VALUE" + " = " + ' "' + val + '" ')
                objectfilters.append('`' + column + '`._' + (subj[subj.find('@') + 1: subj.find('=')]).strip() + " = " + ' "' + (subj[subj.find('=') + 1: subj.find(']')]).strip() + '" ')
            elif '/' in subj and '[*]' not in subj:
                column = subj.replace('/', '.')
                column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
                objectfilters.append('`' + column + "` = " + ' "' + val + '" ')
            else:
                objectfilters.append('`' + column + "` = " + ' "' + val + '" ')

    for v in set(nans):
        # if predmap[v] in subjcols:
        #     continue
        conds = {}
        colnames = [p for p in predmap[v].strip().split('[*]') if len(p) > 0]
        if len(colnames) == 1:
            columns = [c for c in predmap[v].strip().split('/') if len(c) > 0]
            if len(columns) > 1:
                nested = getnextStruct(columns)
                if isinstance(nested, dict):
                    schema.setdefault(columns[0], {}).setdefault(columns[0], {}).update(nested)
                else:
                    schema[columns[0]] = nested
            else:
                schema[columns[0]] = columns[0]
            column = "`" + '_'.join(columns) + '`'
            if v in sparqlprojected:
                projvartocol[v[1:]] = predmap[v].strip().replace('/', '.')
            objectfilters.append(column + " is not null ")
        else:
            columns = [c for c in colnames[0].strip().split('/') if len(c) > 0]
            if len(columns) > 1:
                nested = getnextStruct(columns)
                if isinstance(nested, dict):
                    schema.setdefault(columns[0], {}).update(nested)
                else:
                    schema[columns[0]] = nested
            else:
                schema[columns[0]] = columns[0]
            column = "`" + '_'.join(columns) + '`'
            columnexp = "`" + '`.`'.join(columns) + '`'
            lateralviews['`exp_' + column[1:]] = "explode(" + columnexp + ")"
            column = '`' + 'exp_' + column[1:]

            val = colnames[1].strip()
            arrayc = schema
            cc = columns[0]
            for cc in columns[:-2]:
                if arrayc is None:
                    arrayc = schema[cc]
                else:
                    arrayc = arrayc[cc]
            if len(val) > 0:
                postcolumns = [c for c in val.split('/') if len(c) > 0]

                if '[' not in val:
                    arrayc[cc][columns[-1]] = [getnextStruct(postcolumns)]
                    col = "`" + '`.`'.join(postcolumns) + '`'
                    column += "." + col
                else:
                    cols = []
                    for c in postcolumns:
                        if c[0] == '[' and c[-1] == ']':
                            conds.setdefault(column, []).append(c[1:-1])
                            if isinstance(arrayc[cc], str):
                                arrayc[cc] = {}
                                if cc == columns[-1]:
                                    arrayc[cc].setdefault(columns[-1], []).append(c[1:-1].split('=')[0])
                                else:
                                    arrayc[cc][cc] = {}
                                    arrayc[cc][cc].setdefault(columns[-1], []).append(c[1:-1].split('=')[0])
                            elif cc in arrayc[cc]:
                                if isinstance(arrayc[cc][cc], str):
                                    arrayc[cc][cc] = {}
                                arrayc[cc][cc].setdefault(columns[-1], []).append(c[1:-1].split('=')[0])
                            else:
                                arrayc[cc].setdefault(cc, {}).setdefault(columns[-1], []).append(c[1:-1].split('=')[0])
                        else:
                            arrayc.setdefault(cc, {}).setdefault(cc, {}).setdefault(columns[-1], []).append(c)
                            cols.append(c)

                    if predmap[v] in subjcols and len(cols) == 0:
                        arrayc.setdefault(cc, {}).setdefault(columns[-1], []).append("_VALUE")
                        cols.append('_VALUE')

                    if len(cols) > 0:
                        col = "`" + '`.`'.join(cols) + '`'
                        column += "." + col

            if v in sparqlprojected:
                projvartocol[v[1:]] = column.replace('`', '')

            objectfilters.append(column + " is not null ")
        # column = predmap[v].strip()
        # subj = predmap[v].strip()

        if len(conds) > 0:
            for c in conds:
                for d in conds[c]:
                    cceq = d.split('=')
                    ccneq = d.split('!=')
                    if len(cceq) > 1:
                        if cceq[1] != 'null':
                            objectfilters.append(c + '.' + cceq[0].strip() + '=' + "\"" + cceq[1].strip() + '"')
                    elif len(ccneq) > 1:
                        objectfilters.append(c + '.' + ccneq[0].strip() + '!=' + "\"" + ccneq[1].strip() + '"')

    return objectfilters, projvartocol, lateralviews, schema


if __name__ == "__main__":
    columns = ["synonyms", "synonym"]
    post = ['name']
    schema = {}
    schema[columns[0]] = getnextStruct(columns[1:])
    arrayc = schema
    cc = columns[0]
    for cc in columns[:-2]:
        print(cc, arrayc)
        if arrayc is None:
            arrayc = schema[cc]
        else:
            arrayc = arrayc[cc]
    print(cc, arrayc)
    arrayc[cc] = {}
    arrayc[cc][columns[-1]] = [getnextStruct(post)]
    print(cc, arrayc)
