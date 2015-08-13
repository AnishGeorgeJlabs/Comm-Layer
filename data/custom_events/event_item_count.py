"""
Event algorithm to get customer ids from the database based on the number of items they purchased

Tested on Thu, 13 Aug, 11:33 PM
"""

import aux
import re

def operate(mode, options):
    """ Driver for get_item
    Implements the query_event_driver specification

    :param options: Options dictionary
    :return: same as get_item
    """
    if 'item_count' not in options:
        item_count = '1-2'
    else:
        item_count = options['item_count'].lower()

    return get_item(mode, item_count)

def get_item(mode, item_count):
    """ Implements the query_event specification

    :param mode: usual mode
    :param item_count: a string representing the required range, supported values are "1", "n-m", "more than m" where
    "m" is an integer

    :return: (keys: set, result: dict > {id_customer, [data]}, headers: list)
    """
    # return set(), {'mode': mode, 'item_count': item_count}      # TODO, need to test

    query = """
    SELECT distinct(a.fk_customer), count(*) as items
    FROM sales_order a JOIN sales_order_item b
    ON a.id_sales_order = b.fk_sales_order
    GROUP BY b.fk_sales_order
    %s
    """ %(_get_having_clause(item_count))

    return aux.typical_event_routing(mode, query, ['Item Count'])

    """
    ### Get the results
    def execute_fn(dbname):
        cursor = aux.execute_query(query, dbname)
        return map(
            lambda k: list(k),
            list(cursor)
        )

    init_result = aux.execute_on_database(mode, execute_fn)
    keys, result = aux.convert_to_id_dict(init_result)      # returns the correct and expected format
    return keys, result, ['Item Count']
    """


def _get_having_clause(item_count):
    base = " HAVING items "
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
