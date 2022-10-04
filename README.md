# pysqlparser
a simple python project to parse standard sql string to others base on pyparsing
sqlparser trans sql string to a common dictionary for other parser to parsing.

### sql syntax parse to dictionary
example:
```
=> query_sql = " select a, b as c from db1.tbl where id = 1 order by b asc limit 10, 10"
=> sqlparser.parsing(query_sql)
=> {'select': [['a'], ['b', 'c']], 'from': 'db1.tbl', 'where': [['id', '=', '1']], 'order': [['b', 'asc']], 'limit': ['10', '10']}
```

### sql to mongo js script
unsupported: explain, select *, count, group by [todo list.]
example:
```
=> query_sql = " select a, b as c from db1.tbl where id = 1 order by b asc limit 10, 10"
=> sql2mongojs.show_mongo_js_script(query_sql)
=> db.getSiblingDB("db1").getCollection("tbl").aggregate([{$addFields:{"c":"$b"}},{$match:{"id": {$eq: 1}}},{$sort:{"b":1}},{$project:{"_id":0, "a":1, "c":1}},{$skip:10},{$limit:10},])
```

### other script parser
...
