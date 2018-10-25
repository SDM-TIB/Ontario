from ontario.config.model import *
from ontario.wrappers import *
from ontario.sparql.utilities import *
from pyspark.sql.types import *


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
            colname = variablemap[var].strip().split('[*]')
            if '[' in variablemap[var].strip():
                column = variablemap[var].strip()
                column = "`" + column[:column.find('[')] + ("`._VALUE" if '_VALUE' not in column else "")
                projvartocol[var[1:]] = variablemap[var].strip()
            elif '/' in variablemap[var].strip() and '[*]' not in variablemap[var].strip():
                column = variablemap[var].strip().replace('/', '.')
                column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
                col = variablemap[var].strip()
                projvartocol[var[1:]] = col[col.find('/')+1:]
            else:
                column = "`" + variablemap[var].strip() + '`'
                projvartocol[var[1:]] = variablemap[var].strip()

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
                    predmap = {p:predObjMap[p] for p in predicates}
                    coltotemplate[subj] = subject
                    subjcols[subj] = subject
                    for t in triplepatterns:
                        if subj not in varmap and not t.subject.constant:
                            varmap[t.subject.name] = str(subj)
                        if t.predicate.constant and not t.theobject.constant:
                            pred = getUri(t.predicate, prefixes)[1:-1]
                            pp = [predmap[p]['object'] for p in predmap if p == pred and  predmap[p]['objType'] == TermType.REFERENCE ]
                            pp.extend([predmap[p]['object'][predmap[p]['object'].find('{') + 1: predmap[p]['object'].find('}')]  for p in predmap if p == pred and  predmap[p]['objType'] == TermType.TEMPLATE ])
                            tpm = [predmap[p]['object']  for p in predmap if p == pred and  predmap[p]['objType'] == TermType.TRIPLEMAP ]
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

    return res,coltotemplate, subjcols


def get_filters(triples, prefixes):
    filters = [(getUri(t.predicate, prefixes)[1:-1], " = ", getUri(t.theobject, prefixes)[1:-1])
               for t in triples if t.predicate.constant and t.theobject.constant and \
               getUri(t.predicate, prefixes)[1:-1] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type']

    return filters


def get_pred_vars(triples, prefixes):
    predvars = [(getUri(t.predicate, prefixes)[1:-1], " = ", t.theobject.name)
                for t in triples if t.predicate.constant and not t.theobject.constant]
    return predvars


def getObjectFilters(mappings, prefixes, triples, varmaps, tablealias, sparqlprojected, query):
    objectfilters = []
    filtersmap = {}
    nans = []
    predmap = varmaps['predmap']
    subjcols = varmaps['subjcol']
    schema = {}
    predvars = get_pred_vars(triples, prefixes)
    filters = get_filters(triples, prefixes)

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
                objectfilters.append(tablealias + '.`' + column + "`._VALUE" + " = " + ' "' + val + '" ')
                objectfilters.append(tablealias +  '.`' + column + '`._' + (subj[subj.find('@') + 1: subj.find('=')]).strip() + " = " + ' "' + (
                    subj[subj.find('=') + 1: subj.find(']')]).strip() + '" ')
            elif '/' in subj and '[*]' not in subj:
                column = subj.replace('/', '.')
                column = "`" + column[:column.find('.')] + '`' + column[column.find('.'):]
                objectfilters.append(tablealias + '.`' + column + "` = " + ' "' + val + '" ')
            else:
                objectfilters.append(tablealias + '.`' + column + "` = " + ' "' + val + '" ')

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


def getLVProjectionClause(variablemap, sparqlprojected, tablealias):
    projections = {}
    projvartocol = {}
    lateralviews = {}
    wherenotnull = []
    for var in sparqlprojected:
        if var in variablemap:
            colnames = variablemap[var].strip().split('[*]')
            column = colnames[0]
            conds = {}
            if len(colnames) == 1:
                columns = [c for c in variablemap[var].strip().split('/') if len(c) > 0]
                column = "`" + '`.`'.join(columns) + '`'
                projvartocol[var[1:]] = variablemap[var].strip().replace('/', '.')
                projections[var[1:]] = column + " AS " + var[1:]
                wherenotnull.append(column + " is not null ")
            elif len(colnames) == 2:
                val = colnames[1].strip()
                columns = [c for c in colnames[0].strip().split('/') if len(c) > 0]
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
                        if "_VALUE" not in column:
                            column += "._VALUE"

                projections[var[1:]] = column + " AS " + var[1:]
                wherenotnull.append(column + " is not null ")
                projvartocol[var[1:]] = column.replace('`', '')

            if len(conds) > 0:
                for c in conds:
                    for d in conds[c]:
                        cceq = d.split('=')
                        ccneq = d.split('!=')
                        if len(cceq) > 1:
                            if cceq[1] != 'null':
                                wherenotnull.append(c + '.' + cceq[0].strip() + '=' + "\"" + cceq[1].strip().lower() + '"')
                        elif len(ccneq) > 1:
                            wherenotnull.append(c + '.' + ccneq[0].strip() + '!=' + "\"" + ccneq[1].strip().lower() + '"')

    return projections, wherenotnull, projvartocol, lateralviews


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
    predvarmap = {v[0]:v[2] for v in predvars}
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
                    schema.setdefault(columns[0], {}).update(nested)
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
                            arrayc[cc] = {}
                            arrayc[cc][columns[-1]] = ['_VALUE', c[1:-1].split('=')[0]]
                        else:
                            cols.append(c)

                    if predmap[v] in subjcols:
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
                            objectfilters.append(c + '.' + cceq[0].strip() + '=' + "\"" + cceq[1].strip().lower() + '"')
                    elif len(ccneq) > 1:
                        objectfilters.append(c + '.' + ccneq[0].strip() + '!=' + "\"" + ccneq[1].strip().lower() + '"')

    return objectfilters, projvartocol, lateralviews, schema


if __name__=="__main__":
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