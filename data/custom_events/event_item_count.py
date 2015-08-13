"""
Event algorithm to get customer ids from the database based on the number of items they purchased
"""

from . import connect_db
import aux
import re

def operate(options):
    mode = aux.get_mode(options)

    if 'item_count' not in options:
        item_count = '1-2'
    else:
        item_count = options['item_count'].lower()

    return get_item(mode, item_count)

def get_item(mode, item_count):
    # return set(), {'mode': mode, 'item_count': item_count}      # TODO, need to test
    query = """
    SELECT fk_customer, SUM(DISTINCT(id_sales_order_item)) AS items
    FROM sales_order_item, sales_order WHERE id_sales_order = fk_sales_order
    GROUP BY id_sales_order, id_sales_order_item
    %s
    """ % (_get_where_clause(item_count))

    ### Get the results
    def execute_fn(dbname):
        db = connect_db(dbname)
        cursor = db.cursor()
        cursor.execute(query)
        return map(
            lambda k: list(k),
            list(cursor)
        )

    init_result = aux.execute_on_database(mode, execute_fn)
    keys, result = aux.convert_to_id_dict(init_result)      # returns the correct and expected format
    return keys, result, ['Item Count']


def _get_where_clause(item_count):
    base = " where items "
    if item_count == "1":
        aux = "= 1"
    elif item_count.startswith("more than"):
        m = re.findall('\d+', item_count)
        if len(m) == 0:
            aux = "= 1"
        else:
            aux = "> "+str(m[0])
    else:
        try:
            x = item_count.split('-')
            a = int(x[0])
            b = int(x[1])
            aux = ">= %i and items <= %i" % (a, b)
        except Exception, e:
            print "Got an exception during where clause generation for item_count event "+str(e)
            aux = "= 1"

    return base + aux
