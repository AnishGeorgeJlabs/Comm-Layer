"""
Event algorithm to get the customer set from database
"""
from . import connect_db

def get_customer(mode):
    """ Follows the query_event specifications

    Get customers from the database
    :param mode: Any one of
        1. 'all': no discrimination on region, default
        2. 'uae': UAE customers
        3. 'ksa': KSA customers
        4. 'both': Not implemented
        4. 'others': Not implemented
    :return: A tuple (
            keys: A set of id_customer,
            result: A dictionary { id_customer, [phone, language] }
        )
    """

    if mode == 'uae':
        where_clause = 'where cust.fk_country = 3'
    elif mode == 'ksa':
        where_clause = 'where cust.fk_country = 193'
    else:
        where_clause = ''

    query = """
    SELECT distinct cust.id_customer, phone.number, if(cust.fk_language=1, 'English', 'Arabic') as language
    FROM customer cust INNER JOIN customer_phone phone
    ON phone.fk_customer = cust.id_customer
    %(where_clause)s
    """ % locals()

    db = connect_db("jerry_live")
    cursor = db.cursor()
    cursor.execute(query)

    result = {}
    keys = set()
    for tpl in list(cursor):
        result[tpl[0]] = list(tpl[1:])

    return keys, result
