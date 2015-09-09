"""
Event algorithms to get payment method from db

Tested on Fri, 14 Aug, 03:58 AM
"""
import aux
from . import connect_db

def operate(mode, options):
    """ Driver for get_payment
    Implements the query_event_driver specification
    Note: payment_method has to be a single string value
    """
    pay = options.get('payment_method', [])
    if not isinstance(pay, list):
        pay = [pay]
    if len(pay) == 0:
        return None, None, None
    else:
        return get_payment(mode, pay)


def get_payment(mode, payment_method):
    query = """
    SELECT distinct(fk_customer), payment_method
    FROM sales_order
    WHERE payment_method IN ('%s')""" % "','".join(payment_method)

    return aux.typical_event_routing(mode, query, ['Payment Method'])

