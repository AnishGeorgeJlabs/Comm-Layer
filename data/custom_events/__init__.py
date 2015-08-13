"""
Custom Event based model for super complicated filtering

# Specifications:

## query_event specification
A Function which has:
    1. Parameters: Any kind, no restriction
    2. Return type: (set, dict)
The first element returned MUST by a set of 'id_customer' entries form the database
The second element returned MUST by a dict of 'id_customer' entries mapped to a list of extra data i.e. {str, list<str>}

## query_event_driver specification
A Function (preferably named 'operate') which has
    1. Parameters: (dict)
    2. Return type: (set, dict)
The parameter dict is the options object we receive from external API. The function MUST execute the paired query_event
at some point using the options dict and return the results of that call directly. The function may short circuit the
query_event and return a base case result if it deems it necessary.
"""
__author__ = 'basso'
from sql_data import connect_db
