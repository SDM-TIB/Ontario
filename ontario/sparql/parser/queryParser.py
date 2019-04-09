
__author__ = 'Kemele M. Endris'

from ply import lex, yacc

from ontario.sparql.parser.services import Query, Argument, Triple, UnionBlock, JoinBlock, Optional, Filter, Expression

# Lexer

reserved = {
    'UNION': 'UNION',
    'FILTER': 'FILTER',
    'OPTIONAL': 'OPTIONAL',
    'SELECT': 'SELECT',
    'ASK': 'ASK',
    'DISTINCT': 'DISTINCT',
    'WHERE': 'WHERE',
    'PREFIX': 'PREFIX',
    'LIMIT': 'LIMIT',
    'OFFSET': 'OFFSET',
    'ORDER': 'ORDER',
    'BY': 'BY',
    'DESC': 'DESC',
    'ASC': 'ASC',
    'BOUND': 'BOUND',
    'REGEX': 'REGEX',
    'ISIRI': 'ISIRI',
    'ISURI': 'ISURI',
    'ISBLANK': 'ISBLANK',
    'ISLITERAL': 'ISLITERAL',
    'LANG': 'LANG',
    'DATATYPE': 'DATATYPE',
    'SAMETERM': 'SAMETERM',
    'LANGMATCHES': 'LANGMATCHES',
    'STR': 'STR',
    'UCASE': 'UCASE',
    'LCASE': 'LCASE',
    'CONTAINS': 'CONTAINS',
    'UPPERCASE': 'UPPERCASE'
}

tokens = [
    # "RDFTYPE",
    "DOUBLECONST",
    "DECIMALCONST",
    "CONSTANT",
    "NUMBER",
    "VARIABLE",
    "LKEY",
    "RKEY",
    "COLON",
    "POINT",
    "COMA",
    "URI",
    "ALL",
    "LPAR",
    "RPAR",
    "EQUALS",
    "NEQUALS",
    "LESS",
    "LESSEQ",
    "GREATER",
    "GREATEREQ",
    "ID",
    "NEG",
    "AND",
    "PLUS",
    "MINUS",
    "TIMES",
    "DIV",
    "DOUBLE",
    "INTEGER",
    "DECIMAL",
    "FLOAT",
    "STRING",
    "BOOLEAN",
    "DATETIME",
    "NONPOSINT",
    "NEGATIVEINT",
    "LONG",
    "INT",
    "SHORT",
    "BYTE",
    "NONNEGINT",
    "UNSIGNEDLONG",
    "UNSIGNEDINT",
    "UNSIGNEDSHORT",
    "UNSIGNEDBYTE",
    "POSITIVEINT",
    "OR"
    ] + list(reserved.values())

#t_RDFTYPE = r"a"


def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9\-]*'
    #print t
    t.type = reserved.get(t.value.upper(), 'ID')    # Check for reserved words
    return t


#t_CONSTANT = r"(\"|\')[^\"\'\n\r]*(\"|\')((@[a-z][a-z]) | (\^\^[<](https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|][>]))?" #[<]http[://]+www[.]w3[.]org[/]2001[/]XMLSchema[#]\w+
t_DOUBLECONST = r"[0-9]+'.'[0-9]*[eE][+-]?[0-9]+|'.'([0-9])+[eE][+-]?[0-9]+|([0-9])+[eE][+-]?[0-9]+"
t_DECIMALCONST = r"[0-9]*'.'[0-9]+"
# t_CONSTANT = r"(\"|\')[^\"\'\n\r]*(\"|\')((@[a-z][a-z]) | (\^\^[<](https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|][>]))?"
t_CONSTANT = r"((\')[^\"\n\r]*(\')|(\")[^\"\n\r]*(\"))((@[a-z][a-z]) | (\^\^[<](https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|][>]))?"
# ([^'\n]|(\'))*
t_NUMBER = r"([0-9])+"
t_VARIABLE = r"([\?]|[\$])([A-Z]|[a-z])\w*"
t_LKEY = r"\{"
t_LPAR = r"\("
t_RPAR = r"\)"
t_COLON = r"\:"
#t_RDFTYPE = r"a"
#t_RKEY = r"(\.)?\}"
t_RKEY = r"(\.)?\s*\}"
t_POINT = r"\."
t_COMA = r"\,"
t_EQUALS = r"="
t_NEQUALS = r"\!="
t_LESS = r"<"
t_LESSEQ = r"<="
t_GREATER = r">"
t_GREATEREQ = r">="
t_URI = r"<\S+>"
t_ALL = r"[*]"
t_NEG = r"\!"
t_AND = r"\&\&"
t_OR = r"\|\|"
t_PLUS = r"\+"
t_MINUS = r"\-"
t_TIMES = r"\*"
t_DIV = r"/"
t_DOUBLE = r"xsd\:double"
t_INTEGER = r"xsd\:integer"
t_DECIMAL = r"xsd\:decimal"
t_FLOAT = r"xsd\:float"
t_STRING = r"xsd\:string"
t_BOOLEAN = r"xsd\:boolean"
t_DATETIME = r"xsd\:dateTime"
t_NONPOSINT = r"xsd\:nonPositiveInteger"
t_NEGATIVEINT = r"xsd\:negativeInteger"
t_LONG = r"xsd\:long"
t_INT = r"xsd\:int"
t_SHORT = r"xsd\:short"
t_BYTE = r"xsd\:byte"
t_NONNEGINT = r"xsd\:nonNegativeInteger"
t_UNSIGNEDLONG = r"xsd\:unsignedLong"
t_UNSIGNEDINT = r"xsd\:unsignedInt"
t_UNSIGNEDSHORT = r"xsd\:unsignedShort"
t_UNSIGNEDBYTE = r"xsd\:unsignedByte"
t_POSITIVEINT = r"xsd\:positiveInteger"

t_ignore = ' \t\n'

xsd = "http://www.w3.org/2001/XMLSchema#"


def t_error(t):
    print(t, repr(xstring))

    if t is None:
        raise TypeError("Unknown text '%s' in line %d " % (t.value, t.lexer.lineno,))
    else:
        raise TypeError("Unknown text ")


# Define a rule so we can track line numbers
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)


lexer = lex.lex()


# Parser
def p_parse_sparql_0(p):
    """
    parse_sparql : prefix_list query order_by limit offset
    """
    (vs, ts, d) = p[2]
    p[0] = Query(p[1], vs, ts, d, p[3], p[4], p[5])


def p_parse_sparql_1(p):
    """
    parse_sparql : prefix_list query order_by offset limit
    """
    (vs, ts, d) = p[2]
    p[0] = Query(p[1], vs, ts, d, p[3], p[5], p[4])


def p_prefix_list(p):
    """
    prefix_list : prefix prefix_list
    """
    p[0] = [p[1]] + p[2]


def p_empty_prefix_list(p):
    """
    prefix_list : empty
    """
    p[0] = []


def p_empty(p):
    """
    empty :
    """
    pass


def p_prefix(p):
    """
    prefix : PREFIX uri
    """
    p[0] = p[2]


def p_uri_0(p):
    """
    uri : ID COLON ID
    """
    p[0] = p[1]+p[2]+p[3]


def p_uri_1(p):
    """
    uri : ID COLON URI
    """
    p[0] = p[1]+p[2]+p[3]


def p_uri_2(p):
    """
    uri : URI
    """
    p[0] = p[1]


def p_order_by_0(p):
    """
    order_by : ORDER BY var_order_list desc_var
    """
    p[0] = p[3] + [p[4]]


def p_order_by_1(p):
    """
    order_by : empty
    """
    p[0] = []


def p_var_order_list_0(p):
    """
    var_order_list : empty
    """
    p[0] = []


def p_var_order_list_1(p):
    """
    var_order_list : var_order_list desc_var
    """
    p[0] = p[1] + [p[2]]


def p_desc_var_0(p):
    """
    desc_var : DESC LPAR VARIABLE RPAR
    """
    p[0] = Argument(p[3], False, True)


def p_desc_var_1(p):
    """
    desc_var : VARIABLE
    """
    p[0] = Argument(p[1], False, False)


def p_desc_var_2(p):
    """
    desc_var : ASC LPAR VARIABLE RPAR
    """
    p[0] = Argument(p[3], False, False)


def p_desc_var_3(p):
    """
    desc_var : unary_func LPAR desc_var RPAR
    """
    p[0] = Expression(p[1], p[3], None)


def p_limit_0(p):
    """
    limit : LIMIT NUMBER
    """
    p[0] = p[2]


def p_limit_1(p):
    """
    limit : empty
    """
    p[0] = -1


def p_offset_0(p):
    """
    offset : OFFSET NUMBER
    """
    p[0] = p[2]


def p_offset_1(p):
    """
    offset : empty
    """
    p[0] = -1


def p_query_0(p):
    """
    query : SELECT distinct var_list WHERE LKEY group_graph_pattern RKEY
    """
    p[0] = (p[3], p[6], p[2])


def p_query_1(p):
    """
    query : SELECT distinct ALL WHERE LKEY group_graph_pattern RKEY
    """
    p[0] = ([], p[6], p[2])


def p_query_2(p):
    """
    query : ASK WHERE LKEY group_graph_pattern RKEY
    """
    p[0] = (None, p[4], None)


def p_query_3(p):
    """
    query : ASK LKEY group_graph_pattern RKEY
    """
    p[0] = (None, p[3], None)


def p_distinct_0(p):
    """
    distinct : DISTINCT
    """
    p[0] = True


def p_distinct_1(p):
    """
    distinct : empty
    """
    p[0] = False


def p_ggp_0(p):
    """
    group_graph_pattern : union_block
    """
    p[0] = UnionBlock(p[1])


def p_union_block_0(p):
    """
    union_block : pjoin_block rest_union_block POINT pjoin_block
    """
    punion = [JoinBlock(p[1])] + p[2]
    pjoin = [UnionBlock(punion)] + p[4]
    p[0] = [JoinBlock(pjoin)]


def p_union_block_1(p):
    """
    union_block : pjoin_block rest_union_block pjoin_block
    """
    punion = [JoinBlock(p[1])] + p[2]
    if p[3] != []:
        pjoin = [UnionBlock(punion)] + p[3]
        p[0] = [JoinBlock(pjoin)]
    else:
        p[0] = [JoinBlock(p[1])] + p[2]


def p_union_block_2(p):
    """
    union_block : pjoin_block rest_union_block
    """
    p[0] = [JoinBlock(p[1])] + p[2]


def p_ppjoin_block_0(p):
    """
    pjoin_block : LKEY join_block RKEY
    """
    p[0] = p[2]


def p_ppjoin_block_1(p):
    """
    pjoin_block : join_block
    """
    p[0] = p[1]


def p_ppjoin_block_2(p):
    """
    pjoin_block : empty
    """
    p[0] = []


def p_rest_union_block_0(p):
    """
    rest_union_block : empty
    """
    p[0] = []


def p_rest_union_block_1(p):
    """
    rest_union_block : UNION LKEY join_block rest_union_block RKEY rest_union_block
    """
    p[0] = [JoinBlock(p[3])] + p[4] + p[6]


def p_join_block_0(p):
    """
    join_block : LKEY union_block RKEY rest_join_block
    """
    if (p[4] != [] and isinstance(p[4][0], Filter)):
        p[0] = [UnionBlock(p[2])] + p[4]
    elif p[4]!=[]:
        p[0] = [UnionBlock(p[2])] + [JoinBlock(p[4])]
    else:
        p[0] = [UnionBlock(p[2])]


def p_join_block_1(p):
    """
    join_block : bgp rest_join_block
    """
    p[0] = [p[1]] + p[2]


def p_rest_join_block_0(p):
    """
    rest_join_block : empty
    """
    p[0] = []


def p_rest_join_block_1(p):
    """
    rest_join_block : POINT bgp rest_join_block
    """
    p[0] = [p[2]]+p[3]


def p_rest_join_block_2(p):
    """
    rest_join_block : bgp rest_join_block
    """
    p[0] = [p[1]]+p[2]


def p_bgp_0(p):
    """
    bgp :  LKEY bgp UNION bgp rest_union_block RKEY
    """
    ggp = [JoinBlock([p[2]])] + [JoinBlock([p[4]])] + p[5]
    p[0] = UnionBlock(ggp)


def p_bgp_01(p):
    """
    bgp : bgp UNION bgp rest_union_block
    """
    ggp = [JoinBlock([p[1]])] + [JoinBlock([p[3]])] + p[4]
    p[0] = UnionBlock(ggp)


def p_bgp_1(p):
    """
    bgp : triple
    """
    p[0] = p[1]


def p_bgp_2(p):
    """
    bgp : FILTER LPAR expression RPAR
    """
    p[0] = Filter(p[3])


def p_bgp_3(p):
    """
    bgp : FILTER express_rel
    """
    p[0] = Filter(p[2])


def p_bgp_4(p):
    """
    bgp : OPTIONAL LKEY group_graph_pattern RKEY
    """
    p[0] = Optional(p[3])

#def p_bgp_5(p):
#    """
#    bgp : LKEY join_block rest_union_block RKEY
#    """
#    bgp_arg = p[2] + p[3]
#    p[0] = UnionBlock(JoinBlock(bgp_arg))


def p_bgp_6(p):
    """
    bgp : LKEY join_block RKEY
    """
    if len(p[2]) == 1:
        p[0] = p[2][0]
    else:
        p[0] = JoinBlock(p[2])


def p_expression_0(p):
    """
    expression : express_rel LOGOP expression
    """
    p[0] = Expression(p[2], p[1], p[3])


def p_expression_1(p):
    """
    expression : express_rel
    """
    p[0] = p[1]


def p_expression_2(p):
    """
    expression : LPAR expression RPAR
    """
    p[0] = p[2]


def p_express_rel_0(p):
    """
    express_rel : express_arg RELOP express_rel
    """
    p[0] = Expression(p[2], p[1], p[3])


def p_express_rel_1(p):
    """
    express_rel : express_arg
    """
    p[0] = p[1]


def p_express_rel_2(p):
    """
    express_rel : LPAR express_rel RPAR
    """
    p[0] = p[2]


def p_express_rel_3(p):
    """
    express_rel : NEG LPAR expression RPAR
    """
    p[0] = Expression(p[1], p[3], None)


def p_express_rel_4(p):
    """
    express_rel : NEG express_rel
    """
    p[0] = Expression(p[1], p[2], None)


def p_express_arg_0(p):
    """
    express_arg : uri
    """
    p[0] = Argument(p[1], True, isuri=True)


def p_express_arg_1(p):
    """
    express_arg : VARIABLE
    """
    p[0] = Argument(p[1], False)


def p_express_arg_2(p):
    """
    express_arg : CONSTANT
    """
    c = p[1].strip()
    p[0] = Argument(p[1], True)
    if xsd in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype=c[c.rfind("^")+1:])
    if "@" in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype="<" + xsd + "string>", lang=c[c.rfind("@")+1:])


def p_express_arg_3(p):
    """
    express_arg : NUMBER
    """
    p[0] = Argument(p[1], True)


def p_express_arg_03(p):
    """
    express_arg : NUMBER POINT NUMBER
    """
    decimalNumber = str(p[1]) + p[2] + str(p[3])
    p[0] = Argument(decimalNumber, True)


def p_express_arg_4(p):
    """
    express_arg : REGEX LPAR express_arg COMA pattern_arg regex_flag
    """
    p[0] = Expression("REGEX", p[3], Argument(p[5], False, p[6]))


def p_regex_flags_0(p):
    """
    regex_flag : RPAR
    """
    p[0] = False


def p_regex_flags_1(p):
    """
    regex_flag : COMA pattern_arg RPAR
    """
    p[0] = p[2]


def p_pattern_arg_0(p):
    """
    pattern_arg : CONSTANT
    """
    p[0] = p[1]


def p_express_arg_5(p):
    """
    express_arg : binary_func LPAR express_arg COMA express_arg RPAR
    """
    p[0] = Expression(p[1], p[3], p[5])


def p_express_arg_6(p):
    """
    express_arg : unary_func LPAR express_arg RPAR
    """
    p[0] = Expression(p[1], p[3], None)


def p_express_arg_7(p):
    """
    express_arg : UNARYOP express_arg
    """
    p[0] = Expression(p[1], p[2], None)


def p_express_arg_8(p):
    """
    express_arg : express_arg ARITOP express_arg
    """
    p[0] = Expression(p[2], p[1], p[3])


def p_express_arg_9(p):
    """
    express_arg : LPAR express_arg RPAR
    """
    p[0] = p[2]


def p_express_arg_10(p):
    """
    express_arg : express_arg RELOP express_arg
    """
    p[0] = Expression(p[2], p[1], p[3])


def p_arit_op_0(p):
    """
    ARITOP : PLUS
    """
    p[0] = p[1]


def p_arit_op_1(p):
    """
    ARITOP : MINUS
    """
    p[0] = p[1]


def p_arit_op_2(p):
    """
    ARITOP : TIMES
    """
    p[0] = p[1]


def p_arit_op_3(p):
    """
    ARITOP : DIV
    """
    p[0] = p[1]


def p_unaryarit_op_1(p):
    """
    UNARYOP : PLUS
    """
    p[0] = p[1]


def p_unaryarit_op_2(p):
    """
    UNARYOP : MINUS
    """
    p[0] = p[1]


def p_logical_op_0(p):
    """
    LOGOP : AND
    """
    p[0] = p[1]


def p_logical_op_1(p):
    """
    LOGOP : OR
    """
    p[0] = p[1]


def p_relational_op_0(p):
    """
    RELOP : EQUALS
    """
    p[0] = p[1]


def p_relational_op_1(p):
    """
    RELOP : LESS
    """
    p[0] = p[1]


def p_relational_op_2(p):
    """
    RELOP : LESSEQ
    """
    p[0] = p[1]


def p_relational_op_3(p):
    """
    RELOP : GREATER
    """
    p[0] = p[1]


def p_relational_op_4(p):
    """
    RELOP : GREATEREQ
    """
    p[0] = p[1]


def p_relational_op_5(p):
    """
    RELOP : NEQUALS
    """
    p[0] = p[1]


def p_binary_0(p):
    """
    binary_func : REGEX
    """
    p[0] = p[1]


def p_binary_1(p):
    """
    binary_func : SAMETERM
    """
    p[0] = p[1]


def p_binary_2(p):
    """
    binary_func : LANGMATCHES
    """
    p[0] = p[1]


def p_binary_3(p):
    """
    binary_func : CONSTANT
    """
    p[0] = p[1]


def p_binary_4(p):
    """
    binary_func : CONTAINS
    """
    p[0] = p[1]


def p_unary_0(p):
    """
    unary_func : BOUND
    """
    p[0] = p[1]


def p_unary_1(p):
    """
    unary_func : ISIRI
    """
    p[0] = p[1]


def p_unary_2(p):
    """
    unary_func : ISURI
    """
    p[0] = p[1]


def p_unary_3(p):
    """
    unary_func : ISBLANK
    """
    p[0] = p[1]


def p_unary_4(p):
    """
    unary_func : ISLITERAL
    """
    p[0] = p[1]


def p_unary_5(p):
    """
    unary_func : LANG
    """
    p[0] = p[1]


def p_unary_6(p):
    """
    unary_func : DATATYPE
    """
    p[0] = p[1]


def p_unary_7(p):
    """
    unary_func : STR
    """
    p[0] = p[1]


def p_unary_8(p):
    """
    unary_func : UPPERCASE
    """
    p[0] = p[1]


def p_unary_9(p):
    """
    unary_func : DOUBLE
               | INTEGER
               | DECIMAL
               | FLOAT
               | STRING
               | BOOLEAN
               | DATETIME
               | NONPOSINT
               | NEGATIVEINT
               | LONG
               | INT
               | SHORT
               | BYTE
               | NONNEGINT
               | UNSIGNEDLONG
               | UNSIGNEDINT
               | UNSIGNEDSHORT
               | UNSIGNEDBYTE
               | POSITIVEINT
    """
    p[0] = p[1]


def p_unary_10(p):
    """
    unary_func : ID COLON ID
    """
    p[0] = p[1] + p[2] + p[3]


def p_unary_11(p):
    """
    unary_func : uri
    """
    p[0] = p[1]


def p_unary_12(p):
    """
    unary_func : UCASE
    """
    p[0] = p[1]


def p_unary_13(p):
    """
    unary_func : LCASE
    """
    p[0] = p[1]


def p_var_list(p):
    """
    var_list : var_list VARIABLE
    """
    p[0] = p[1] + [Argument(p[2], False)]


def p_single_var_list(p):
    """
    var_list : VARIABLE
    """
    p[0] = [Argument(p[1], False)]


def p_triple_0(p):
    """
    triple : subject predicate object
    """
    p[0] = Triple(p[1], p[2], p[3])


def p_predicate_rdftype(p):
    """
    predicate : ID
    """
    if p[1] == 'a':
        value = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
        p[0] = Argument(value, True, isuri=True)
    else:
        print('raising syntax error:')
        p_error(p[1])
        raise SyntaxError


def p_predicate_uri(p):
    """
    predicate : uri
    """
    p[0] = Argument(p[1], True, isuri=True)


def p_predicate_var(p):
    """
    predicate : VARIABLE
    """
    p[0] = Argument(p[1], False)


def p_subject_uri(p):
    """
    subject : uri
    """
    p[0] = Argument(p[1], True, isuri=True)


def p_subject_variable(p):
    """
    subject : VARIABLE
    """
    p[0] = Argument(p[1], False)


def p_object_uri(p):
    """
    object : uri
    """
    p[0] = Argument(p[1], True, isuri=True)


def p_object_variable(p):
    """
    object : VARIABLE
    """
    p[0] = Argument(p[1], False)


def p_object_constant_0(p):
    """
    object : CONSTANT
    """
    c = p[1].strip()
    p[0] = Argument(p[1], True)
    if xsd in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype=c[c.rfind("^")+1:])
    if "@" in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype="<" + xsd + "string>", lang=c[c.rfind("@")+1:])


def p_object_constant_1(p):
    """
    object : DOUBLECONST
    """
    c = p[1].strip()
    p[0] = Argument(p[1], True)
    if xsd in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype=c[c.rfind("^") + 1:])
    if "@" in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype="<" + xsd + "string>", lang=c[c.rfind("@") + 1:])


def p_object_constant_2(p):
    """
    object : DECIMALCONST
    """
    c = p[1].strip()
    p[0] = Argument(p[1], True)
    if xsd in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype=c[c.rfind("^") + 1:])
    if "@" in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype="<" + xsd + "string>", lang=c[c.rfind("@") + 1:])


def p_object_constant_3(p):
    """
    object : NUMBER
    """
    c = p[1].strip()
    p[0] = Argument(p[1], True)
    if xsd in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype=c[c.rfind("^") + 1:])
    if "@" in p[1]:
        p[0] = Argument(c[:c.find("^")], True, datatype="<" + xsd + "string>", lang=c[c.rfind("@") + 1:])


def p_error(p):
    print (p)
    if isinstance(p, str):
        value = p
    else:
        if p is not None:
            value = p.value
        else:
            value = p
    print(repr(xstring))
    import traceback
    traceback.print_exc()
    raise TypeError("unknown text at %r %s" % (value, xstring,))


parser = yacc.yacc(debug=0)

# Helpers
xstring = ""


def parse(string):
    global xstring
    xstring = string
    return parser.parse(string, lexer=lexer)

