"""
Algorithms to get repeat buying frequency and all
"""
import aux
import re

def operate(mode, options):
    """ Driver for get_repeat
    Implements the query_event_driver specification

    :param mode:
    :param options:
    :return:
    """

    if 'repeat_buyer' not in options:
        return None, None, None
    else:
        repeat = options['repeat_buyer']

def get_repeat(mode, repeat):
    """ Implements the query_event specification

    :param mode:  usual mode
    :param repeat: A string which contains the limits to check for the repeat frequency
    :return:
    """
    query = """
    SELECT fk_customer, count(*) as total
    FROM sales_order
    GROUP BY fk_customer
    HAVING %s""" % _get_where_clause(repeat)

    return aux.typical_event_routing(mode, query, ['Repeat Frequency'])

def _get_where_clause(repeat)
    l = re.findall('\d+', repeat)
    if len(l) == 1 and repeat.lower().startswith("more than"):
        clause = "total > "+str(l[0])
    elif len(l) == 1:
        clause = "total = "+str(l[0])
    elif len(l) == 2:
        clause = "total >= %i and total <= %i" %(int(l[0]), int(l[1]))
    else:
        clause = "total = 1"

    return clause
