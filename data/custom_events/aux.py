"""
Auxilary helper methods
"""

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
    result = {}
    keys = set()
    for tpl in list(cursor):
        result[tpl[0]] = list(tpl[1:])
        keys.add(tpl[0])

    return keys, result
