"""
Event algorithm to get the category match from database
"""
from . import connect_db

def get_category(mode, cat_list):
    """ Follows the query_event specifications

    Get combination list with id_customer where each
    of these id_customer has bought items from the given category list
    :param mode: Similar to mode in event_customer, any one of 'all', 'uae', 'ksa'
    :param cat_list: a list of categories to match
    :return: A tuple (
            keys: A set of id_customer,
            result: A dictionary { id_customer, [additional] }
        )
    """

    r1 = _step1(mode)           # id_cutomer | sku | data ...
    r2 = _step2(cat_list)       # { sku : category }

    result = {}
    keys = set()
    for tpl in r1:
        sku = tpl[1]
        cid = tpl[0]
        if sku in r2 and cid not in result:
            data = list(tpl[2:])
            data.append(r2[sku])        # Add the category as well
            result[cid] = data
            keys.add(cid)

    return keys, result

def _step1(mode):
    if mode == 'uae':
        return _step1_partial('bob_live_ae')
    elif mode == 'ksa':
        return _step1_partial('bob_live_sa')
    else:
        result = _step1_partial('bob_live_sa')
        result += _step1_partial('bob_live_ae')
        return result

def _step1_partial(dbname):
    """
    :return: A List of Tuples, where each tuple 0 index is the id_customer and 1 is sku
    """

    query = """
    SELECT a.fk_customer, b.sku, a.id_sales_order, a.order_nr, a.grand_total
    FROM sales_order a JOIN sales_order_item b
    ON a.id_sales_order = b.fk_sales_order
    """

    db = connect_db(dbname)
    cursor = db.cursor()
    cursor.execute(query)
    result = list(cursor)
    for tpl in result:
        tpl[1] = tpl[1].split('-')[1]       # Remove -config from sku
    return result

def _step2(cat_list):
    """
    :return: A dict of { sku: category }
    """
    where_clause = 'where cat.name in (%s)' % str(cat_list).strip('[]')

    query = """
    SELECT conf.sku, cat.name
    FROM catalog_config conf JOIN catalog_config_has_catalog_category cc
    ON cc.fk_catalog_config = conf.id_catalog_config
    $(where_clause)
    """ % locals()

    db = connect_db('bob_live_ae')          # This one only in ae
    cursor = db.cursor()
    cursor.execute(query)

    result = {}
    for tpl in list(cursor):
        result[tpl[0]] = tpl[1]             # Will ovewrite some skus

    return result
