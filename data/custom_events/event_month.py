"""
Get customer based on month of purchase

Tested on Fri, 14 Aug, 01:40 AM
"""

import aux
import json

def operate(mode, options):
    """ Driver for get_month
    Implements the query_event_driver specification

    updated on 17th september 2015 to support year against month
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
    WHERE EXTRACT(YEAR_MONTH FROM created_at) in ('%s')""" % "','".join(months)

    return aux.typical_event_routing(mode, query, ['Purchased at'])
