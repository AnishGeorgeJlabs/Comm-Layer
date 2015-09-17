"""
Get customer based on month of origination

"""

import aux


def operate(mode, options):
    """ Driver for get_origination
    Implements the query_event_driver specification

    :param mode:
    :param options:
    :return:
    """

    if 'purchase_month' not in options:
        return None, None, None
    else:
        months = options['origination_month']

    if not isinstance(months, list):
        months = [months]

    return get_origination(months)


def get_origination(months):
    query = """
    SELECT id_customer, created_at
    FROM customer
    WHERE EXTRACT(YEAR_MONTH FROM created_at) in ('%s')""" % "','".join(months)

    cursor = aux.execute_query(query, 'jerry_live')
    keys, result = aux.convert_to_id_dict(cursor)
    return keys, result, ['Origination Date']
