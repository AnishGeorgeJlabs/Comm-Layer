"""
Algorithms to handle platform of purchase
"""

import aux
import json

def operate(mode, options):
    """ Driver for get_platform
    Implements the quer_event_driver specifications
    """

    if 'platform' not in options:
        return None, None, None

    platform = options['platform']
    if not isinstance(platform, list):
        platform = [platform]
    return get_platform(mode, platform)

def get_platform(mode, platform):
    query = "SELECT fk_customer, misc FROM sales_order"

    def execute_fn(dbname):
        cursor = aux.execute_query(query, dbname)
        return map(
            lambda k: list(k),
            list(cursor)
        )

    init_result = aux.execute_on_database(mode, execute_fn)

    parsed_list = map(
        lambda l: [l[0], _get_plat_from_misc(l[1])],
        init_result
    )

    fin_result = filter(lambda k: k[1] in platform, parsed_list)
    keys, result = aux.convert_to_id_dict(fin_result)
    return keys, result, ['Platform']


def _get_plat_from_misc(misc):
    d = json.loads(misc)
    if 'platform' in d:
        return d['platform']
    else:
        return ''
