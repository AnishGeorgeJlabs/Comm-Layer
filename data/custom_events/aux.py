"""
Auxiliary helper methods
"""
from . import connect_db
import requests
import re

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

def extract_limits(input, item):
    """
    Helper method to extract the min, max limits from a string and convert that to an expression which can be
    appended to an sql WHERE or HAVING clause
    :param input: The string containing limits
    :param item: the column name to be checked with the limits
    :return: A string which can be appended to a WHERE or HAVING clause and confirms to the limits present in the input
    """
    input = input.lower()

    l = sorted(map(
        lambda k: int(k),
        re.findall('\d+', input)
    ))

    if len(l) == 0:
        return " %s = 1 " % item
    elif len(l) == 1:
        if any(a in input for a in ['more', 'greater', '>', 'higher']):
            op = '>'
        elif any(a in input for a in ['less', '<', 'lower']):
            op = '<'
        else:
            op = '='
        return " %s %s %i " % (item, op, l[0])
    else:
        return " %s >= %i and %s <= %i " % (item, l[0], item, l[1])

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


def get_block_list():
    """ Get the blocked numbers from jlabs api """
    r = requests.get('http://45.55.72.208/wadi/block_list?type=phone')
    if r.status_code != 200:
        return []
    data = r.json()
    if not data.get('success', False):
        return []
    else:
        return data.get('data', [])

def get_block_set():
    """ Get the blocked list as a set of strings- phone,language """
    return set(','.join(a[0:2]) for a in get_block_list())

