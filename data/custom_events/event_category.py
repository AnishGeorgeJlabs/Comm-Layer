"""
Event algorithm to get the category match from database

Tested on Tue, 11 Aug, 08:28 PM
"""
from . import connect_db
import json
import aux

headers = ['Order Number', 'Grand Total', 'Category']
def operate(options):
    """ Driver for the main get_category method
    Implements the query_event_driver specification

    Checks the options dictionary to see whether all options needed by get_category are present. Tries to recover missing
    options and if it still wont work, then returns the empty result as expected form get_category short circuiting the
    query process
    :param options: The actual options object we got from the api
    :return: Same as get_category
    """

    # Recoverable options
    mode = aux.get_mode(options)

    if 'cat_list' not in options or len(options['cat_list']) == 0:
        return None, None, None
    else:
        return get_category(mode, options['cat_list'])


def get_category(mode, cat_list):
    """ Follows the query_event specifications

    Get combination list with id_customer where each
    of these id_customer has bought items from the given category list
    :param mode: Similar to mode in event_customer, any one of 'all', 'uae', 'ksa'
    :param cat_list: a list of categories to match
    :return: A tuple (
            keys: A set of id_customer,
            result: A dictionary { id_customer, [additional] },
            headers: A list of headers
        )
    """

    r1 = aux.execute_on_database(mode, _step1)   # id_cutomer | sku | data ...
    r2 = _step2(cat_list)       # { sku : category }

    result = {}                     # { id: [order_number, grand_total, category]
    keys = set()
    for tpl in r1:
        sku = tpl[1]
        cid = tpl[0]
        if sku in r2 and cid not in result:
            data = list(tpl[2:])
            data.append(r2[sku])        # Add the category as well
            result[cid] = data
            keys.add(cid)

    return keys, result, headers

def _step1(dbname):
    """
    :return: A List of Tuples, where each tuple 0 index is the id_customer and 1 is sku
    """

    query = """
    SELECT a.fk_customer, b.sku, a.order_nr, a.grand_total
    FROM sales_order a JOIN sales_order_item b
    ON a.id_sales_order = b.fk_sales_order
    """

    db = connect_db(dbname)
    cursor = db.cursor()
    cursor.execute(query)
    '''
    for tpl in result:
        tpl[1] = tpl[1].split('-')[1]       # Remove -config from sku
    '''

    def mapper(tpl):
        res = list(tpl)
        res[1] = res[1].split('-')[0]
        return res

    result = map(
        mapper,
        list(cursor)
    )
    return result

def _step2(cat_list):
    """
    :return: A dict of { sku: category }
    """
    where_clause = 'where cat.name in (%s)' % json.dumps(cat_list).strip('[]')

    query = """
    SELECT conf.sku, cat.name FROM catalog_config conf
    JOIN catalog_config_has_catalog_category cc ON cc.fk_catalog_config = conf.id_catalog_config
    JOIN catalog_category cat on cc.fk_catalog_category = cat.id_catalog_category
    %(where_clause)s
    """ % locals()

    db = connect_db('bob_live_ae')          # This one only in ae
    cursor = db.cursor()
    cursor.execute(query)

    result = {}
    for tpl in list(cursor):
        result[tpl[0]] = tpl[1]             # Will ovewrite some skus

    return result
