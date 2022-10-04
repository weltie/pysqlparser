# -*- coding: utf-8 -*-
"""
@File: sql2mongojs
@Author: weltie
@Time: 2022/10/4
"""
import sqlparser


def spec_str(spec):
    """
    :param spec: dictionary
    :return: String
    """
    if spec is None:
        return "{}"
    if isinstance(spec, list):
        out_str = "[" + ', '.join([spec_str(x) for x in spec]) + "]"
    elif isinstance(spec, dict):
        out_str = "{" + ', '.join(
            ["{}:{}".format("{}".format(spec_str(x)), spec_str(spec[x])) for x in sorted(spec)]
        ) + "}"
    elif spec and isinstance(spec, str) and not spec.isdigit():
        out_str = "\"" + spec + "\""
    else:
        out_str = spec
    return out_str


COND_KEYWORDS = {
    "=": "$eq",
    "!=": "$ne",
    ">": "$gt",
    ">=": "$gte",
    "<": "$lt",
    "<=": "$lte",
    "like": "$regex",
    "or": "$or",
    "and": "$and"
}


def combine_where(where_spec):
    if isinstance(where_spec, list):
        if isinstance(where_spec[0], str):
            key, op, val = where_spec
            op = COND_KEYWORDS[op]
            cond = "{}: {}".format(op, val)
            res = "{" + "\"{}\": ".format(key) + "{" + cond + "}" + "}"
            return res
        else:
            res = []
            for s in where_spec:
                res.append(combine_where(s))
            return "[" + ", ".join(res) + "]"
    else:
        # only one element
        for op, vals in where_spec.items():
            val_res = combine_where(vals)
            op = COND_KEYWORDS[op]
            res = "{" + "{}: {}".format(op, val_res) + "}"
            return res


def show_mongo_js_script(spec_dict):
    """
    param sql: string. standard sql
    return: string. mongo js script
    """
    try:
        # parsing from
        from_spec = spec_dict.get("from")
        if not from_spec:
            raise Exception("Error 'from' spec {}".format(spec_dict))
        if from_spec.find(".") > -1:
            db, collection = from_spec.split(".")
            js_script = "db.getSiblingDB(\"{}\").getCollection(\"{}\").".format(db, collection)
        else:
            collection = from_spec
            js_script = """db.getCollection(\"{}\").""".format(collection)
        spec_parse_results = {}
        # parsing select
        select_spec = spec_dict.get("select")
        select_results = {"$project": {}, "$addFields": {}}
        drop_id = True  # default drop _id field
        for lst_field in select_spec:
            if len(lst_field) == 2:
                real_field, as_field = lst_field
                # todo real_field is list [count, sum ,avg, ...]
                if not isinstance(real_field, str):
                    continue
                if real_field == "_id":
                    drop_id = False
                select_results["$addFields"].update({"{}".format(as_field): "${}".format(real_field)})
                select_results["$project"].update({"{}".format(as_field): 1})
            else:
                real_field = lst_field[0]
                if real_field == "*":
                    # todo : * parsing by other statement
                    continue
                if real_field == "_id":
                    drop_id = False
                select_results["$project"].update({real_field: 1})
        if drop_id:
            select_results["$project"].update({"_id": 0})

        # where parsing
        where_spec = spec_dict.get("where")
        where_results = {}
        if where_spec:
            where_spec = where_spec[0]
            where_results.update({"$match": combine_where(where_spec)})

        # limit parsing
        limit_spec = spec_dict.get("limit")
        limit_results = {}
        if limit_spec:
            if len(limit_spec) == 1:
                limit_results["$limit"] = limit_spec[0]
            else:
                limit_results["$limit"] = limit_spec[0]
                limit_results["$skip"] = limit_spec[1]

        # order by parsing
        order_spec = spec_dict.get("order")
        order_results = {}
        if order_spec:
            order_results["$sort"] = {}
            for s in order_spec:
                if len(s) == 1:
                    order_results["$sort"].update({s[0]: 1})
                else:
                    asc = 1 if s[1] == "asc" else 0
                    order_results["$sort"].update({s[0]: asc})

        spec_parse_results.update(select_results)
        spec_parse_results.update(where_results)
        spec_parse_results.update(limit_results)
        spec_parse_results.update(order_results)
        print(spec_parse_results)

        # sorted statement
        sorted_key = ["$addFields", "$match", "$sort", "$project", "$skip", "$limit"]
        _query_body = ""
        for k in sorted_key:
            _d = spec_parse_results.get(k)
            if not _d:
                continue
            if k in {"$match"}:
                _d_str = "{}:{}".format(k, _d)
            else:
                _d_str = "{}:{}".format(k, spec_str(_d))
            _d_str = "{" + _d_str + "},"
            _query_body += _d_str

        js_script += "aggregate([{}])".format(_query_body)
        return js_script
    except Exception as e:
        print(e)


if __name__ == "__main__":
    sql = """
    select a, b as c
    from db1.tbl
    where id = 1
    order by b asc
    limit 10, 10
    """
    sql_spec = sqlparser.parsing(sql)
    print(sql_spec)
    mongo_js = show_mongo_js_script(sql_spec)
    print(mongo_js)
