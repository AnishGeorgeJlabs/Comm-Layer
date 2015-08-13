"""
Auxiliary helper methods
"""
from . import connect_db

def get_mode(options):
    """
    Validate the options to get correct mode and if not present, get fallback mode
    """
    if 'mode' not in options:
        return 'all'
    elif options['mode'] not in ['all', 'uae', 'ksa']:
        return 'all'
    else:
        return options['mode']

def execute_on_database(mode, function):
    """
    Execute the given function in one or both of 'bob_live_ae' and 'bob_live_sa' depending
    on the mode
    :param mode: any of ['all', 'uae', 'ksa']
    :param function: MUST accept the name of database as parameter and MUST return a list. If the function is called on
        both databases, then the results will be concatenated and given
    :return:
    """
    if mode == 'uae':
        return function('bob_live_ae')
    elif mode == 'ksa':
        return function('bob_live_sa')
    else:
        result = function('bob_live_sa')
        result += function('bob_live_ae')
        return result

def convert_to_id_dict(cursor):
    """
    Converts a given list(list) to a dict where the keys are the first element of every original value and the values
    are lists containing the remaining elements of each of those orignal values
    e.g. converts [['a', 'anish', 'george'], ['b', 'jibin', 'george']]
         to       {'a': ['anish', 'george'], 'b': ['jibin', 'george']}
    The function natively supports using db cursors and makes the necessary list conversions wherever needed
    """
    result = {}
    keys = set()
    for tpl in list(cursor):
        result[tpl[0]] = list(tpl[1:])
        keys.add(tpl[0])

    return keys, result

def execute_query(query, dbname):
    """
    Execute the given query on the given database
    :param query: SQL query to execute
    :param dbname: Database to use
    :return: pymysql cursor with the query executed
    """
    db = connect_db(dbname)
    cursor = db.cursor()
    cursor.execute(query)
    return cursor

def typical_event_routing(mode, query, headers):
    """
    A typical format for an event function, so refactored it here
    :param mode: same
    :param query: compiled query, just the one one to be executed
    :param headers: final headers
    :return: Final result for the event process
    """
    def execute_fn(dbname):
        cursor = execute_query(query, dbname)
        return map(
            lambda k: list(k),
            list(cursor)
        )

    init_result = execute_on_database(mode, execute_fn)
    keys, result = convert_to_id_dict(init_result)      # returns the correct and expected format
    return keys, result, headers
