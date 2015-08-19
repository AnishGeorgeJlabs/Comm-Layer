"""
Algorithm for filtration based on last touch clicked

Tested on Mon, 17 Aug, 03:07 PM
"""
import aux

def operate(mode, options):
    """ Driver for the get_channel function
    implements event_query_driver specification
    """

    if 'channel' not in options:
        return None, None, None
    channel = options['channel']
    if not isinstance(channel, list):
        channel = [channel]

    return get_platform(mode, '|'.join(channel))

def get_platform(mode, regex):
    """ Implements the query_event specification

    :param regex: The regular expression used by sql to match on lasttouch_click
    """

    query = """
    SELECT fk_customer, lasttouch_click
    FROM sales_order
    WHERE lasttouch_click REGEXP '%s'""" % regex

    return aux.typical_event_routing(mode, query, ['Last Touch Click'])
