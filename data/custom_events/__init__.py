"""
Custom Event based model for super complicated filtering

# Specifications:
## query_event specification
A Function which has:
    1. Parameters: Any kind, no restriction
    2. Return type: (set, dict, list)
The first element returned MUST be a set of 'id_customer' entries form the database
The second element returned MUST be a dict of 'id_customer' entries mapped to a list of extra data i.e. {str, list<str>}
The third element returned MUST be a list of headers. The length of this MUST equal to that of each value in the results
    second dict
The Function MAY return (None, None, None) in which case, its effect will be skiped from the pipeline, as good as if
the function was never called and its operation never in the pipeline

## query_event_driver specification
A Function (preferably named 'operate') which has
    1. Parameters: (dict)
    2. Return type: (set, dict, list)
The parameter dict is the options object we receive from external API. The function MUST execute the paired query_event
at some point using the options dict and return the results of that call directly. The function may short circuit the
query_event and return a base case result if it deems it necessary.



# Special option - mode
The mode is a special option needed by most of the operations in the pipeline,
currently, it can have any of the following values
    1. uae
    2. ksa
    3. all


# Operations (current implementation)
## customer
    Get customers, must be the final operation in the pipeline

## category
    Filter results based on the category of purchase
    Required options:
        * cat_list: A non-empty list of categories to be checked

## item_count
    Filter results based on the count of items
    Required options:
        * item_count: A string which denotes the item count to checked. Currently supported
            value kinds are : '1', 'm-n', 'more than m' where m and n are integers

## payment_method
    Filter results for a perticular kind of payment method
    Required options:
        * payment_method: A string denoting the exact payment method. Database suppors any of
            1. innovate
            2. postpayment
            3. nopayment
"""
__author__ = 'basso'
from sql_data import connect_db
