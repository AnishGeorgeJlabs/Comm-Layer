"""
Event algorithms to get payment method from db
"""
import aux
from . import connect_db

def operate(mode, options):
    """ Driver for get_payment
    Implements the query_event_driver specification
    Note: payment_method has to be a single string value
    """
    if 'payment_method' in options:
        return get_payment(mode, options['payment_method'])
    else:
        return None, None, None


def get_payment(mode, payment_method):
    query = """
    SELECT distinct(fk_customer), payment_method
    FROM sales_order
    WHERE payment_method = %s""" % payment_method

    return aux.typical_event_routing(mode, query, ['Payment Method'])

