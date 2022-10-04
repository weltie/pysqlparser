# -*- coding: utf-8 -*-
"""
@File: sqlparser
@Author: weltie
@Time: 2022/10/3
"""
from pyparsing import (Word, alphas, CaselessKeyword, Group, Optional, ZeroOrMore,
                       Forward, Suppress, alphanums, OneOrMore, quotedString,
                       Combine, Keyword, Literal, replaceWith, oneOf, nums,
                       removeQuotes, QuotedString, Dict)

# keyword declare
LPAREN, RPAREN = map(Suppress, "()")
EXPLAIN = CaselessKeyword('EXPLAIN').setParseAction(lambda t: {'explain': True})
SELECT = Suppress(CaselessKeyword('SELECT'))
DISTINCT = CaselessKeyword('distinct')
COUNT = CaselessKeyword('count')
WHERE = Suppress(CaselessKeyword('WHERE'))
FROM = Suppress(CaselessKeyword('FROM'))
CONDITIONS = oneOf("= != < > <= >= like", caseless=True)

AND = CaselessKeyword('and')
OR = CaselessKeyword('or')
ORDER_BY = Suppress(CaselessKeyword('ORDER BY'))
GROUP_BY = Suppress(CaselessKeyword('GROUP BY'))
DESC = CaselessKeyword('desc')
ASC = CaselessKeyword('asc')

LIMIT = Suppress(CaselessKeyword('LIMIT'))
SKIP = Suppress(CaselessKeyword('SKIP'))

# aggregate func
AGG_SUM = CaselessKeyword('sum')
AGG_AVG = CaselessKeyword('avg')
AGG_MAX = CaselessKeyword('max')
AGG_MIN = CaselessKeyword('min')
AGG_WORDS = (AGG_SUM | AGG_AVG | AGG_MIN | AGG_MAX)


# export
def parsing(query_sql):
    """
    Convert a SQL query to a spec dict for parsing.
    Support Sql Statement [select, from ,where, limit, count(*), order by, group by]
    param query_sql: string. standard sql
    return: None or a dictionary
    """
    # morphology
    word_match = Word(alphanums + "._") | quotedString
    optional_as = Optional(Suppress(CaselessKeyword('as')) + word_match)
    word_as_match = Group(word_match + optional_as)
    number = Word(nums)
    # select
    select_word = (word_as_match | Group(Keyword("*")))

    count_ = Group(COUNT + LPAREN + Keyword("*") + RPAREN)
    count_word = Group(count_ + optional_as)

    select_agg = Group(AGG_WORDS + Suppress(LPAREN) + word_match + Suppress(RPAREN))
    select_agg_word = Group(select_agg + optional_as)

    select_complex = (count_word | select_agg_word | select_word)
    select_clause = (SELECT + select_complex + ZeroOrMore(Suppress(",") + select_complex)).setParseAction(
        lambda matches: {"select": matches.asList()}
    )

    # from
    from_clause = (FROM + word_match).setParseAction(
        lambda matches: {"from": matches[0]}
    )

    # where
    condition = (word_match + CONDITIONS + word_match).setParseAction(
        lambda matches: [matches.asList()]
    )

    def condition_combine(matches=None):
        if not matches:
            return {}
        if len(matches) == 1:
            return matches
        else:
            return {"{}".format(matches[1]): [matches[0], matches[2]]}

    and_term = (OneOrMore(condition) + ZeroOrMore(AND + condition)).setParseAction(condition_combine)
    or_term = (and_term + ZeroOrMore(OR + and_term)).setParseAction(condition_combine)

    where_clause = (WHERE + or_term).setParseAction(
        lambda matches: {"where": matches.asList()}
    )

    # group by
    group_by_clause = (GROUP_BY + word_match + ZeroOrMore(Suppress(",") + word_match)).setParseAction(
        lambda matches: {"group": matches.asList()}
    )

    # order by
    order_by_word = Group(word_match + Optional(DESC | ASC))
    order_by_clause = (ORDER_BY + order_by_word + ZeroOrMore(Suppress(",") + order_by_word)).setParseAction(
        lambda matches: {"order": matches.asList()}
    )

    # limit
    limit_clause = (LIMIT + number + Optional(Suppress(",") + number)).setParseAction(
        lambda matches: {"limit": matches.asList()}
    )

    list_term = Optional(EXPLAIN) + select_clause + from_clause + \
        Optional(where_clause) + \
        Optional(group_by_clause) + \
        Optional(order_by_clause) + \
        Optional(limit_clause)

    expr = Forward()
    expr << list_term
    ret = expr.parseString(query_sql.strip())

    spec_dict = {}
    for d in ret:
        spec_dict.update(d)
    return spec_dict


if __name__ == "__main__":
    sql = """
    select a, b as c
    from db1.tbl
    where id = 1
    order by b asc
    limit 10, 10
    """
    sql_spec = parsing(sql)
    print(sql_spec)
