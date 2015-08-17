"""
Event algorithm to get the customer set from database

Tested on Tue, 11 Aug, 08:27 PM

Restested for new header specs and refactor on Thu, 13 Aug, 11:33 PM
"""

import aux

def operate(mode, options):
    """ Driver for the main get_customer method
    Implements the query_event_driver specification

    Extracts the mode option from the options dict and recovers it if not present, then drives the get_customer method
    :param options: The actual options object we got from the api
    :return: Same as get_customer
    """

    if 'language' in options:       # has to be a string
        return get_customer(mode, options['language'])
    else:
        return get_customer(mode, None)

def get_customer(mode, language):
    """ Follows the query_event specifications

    Get customers from the database
    :param mode: Any one of
        1. 'all': no discrimination on region, default
        2. 'uae': UAE customers
        3. 'ksa': KSA customers
        4. 'both': Not implemented
        4. 'others': Not implemented
    :param language: An optional language
    :return: A tuple (
            keys: A set of id_customer,
            result: A dictionary { id_customer, [phone, language] },
            headers: A list of headers
        )
    """

    if mode == 'uae':
        where_clause = 'where cust.fk_country = 3'
    elif mode == 'ksa':
        where_clause = 'where cust.fk_country = 193'
    else:
        where_clause = ''

    query = """
    SELECT distinct cust.id_customer, phone.number, if(cust.fk_language=1, 'English', 'Arabic') as language, if(cust.fk_country=3, 'UAE', 'SA') as country
    FROM customer cust INNER JOIN customer_phone phone
    ON phone.fk_customer = cust.id_customer
    %s
    """ % where_clause

    if language is not None:
        fquery = """
        SELECT * FROM (%s) AS T WHERE language = '%s'""" % (query, language)
    else:
        fquery = query
    cursor = aux.execute_query(fquery, 'jerry_live')

    result = {}
    keys = set()
    keys, result = aux.convert_to_id_dict(cursor)
    return keys, result, ['Phone', 'language', 'Country']
