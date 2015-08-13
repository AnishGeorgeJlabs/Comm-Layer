"""
Get customer based on month of purchase
"""

import aux
import json

def operate(mode, options):
    """ Driver for get_month
    Implements the query_event_driver specification

    :param mode:
    :param options:
    :return:
    """

    if 'purchase_month' not in options:
        return None, None, None
    else:
        months = options['purchase_month']

    if not isinstance(months, list):
        months = [months]

    return get_month(mode, months)

def get_month(mode, months):
    query = """
    SELECT fk_customer, created_at
    FROM sales_order
    WHERE EXTRACT(MONTH FROM created_at) in (%s)""" % json.dumps(months).strip("[]")

    return aux.typical_event_routing(mode, query, ['Purchased at'])
