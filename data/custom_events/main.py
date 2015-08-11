"""
Root algorithm for event queries
"""
from event_category import get_category
from event_customer import get_customer


OPERATIONS = {
    'customer': {
        'description': 'Get customers, must be the final operation in pipeline',
        'options required': ['mode']
    },
    'category': {
        'description': 'Filter results based on the category of purchase',
        'options required': ['mode', 'cat_list']
    }
}

OPTIONS = {
    'mode': {
        'description': 'Database mode',
        'valid values': ['all', 'uae', 'ksa'],
        'required by': ['customer', 'category']
    },
    'cat_list': {
        'description': 'List of categories to check',
        'required by': 'category'
    }
}

def _execute_event(operation, options):
    print "Executing operation ", operation
    try:
        if operation == 'category':
            return get_category(options['mode'], options['cat_list'])
        elif operation == 'customer':
            return get_customer(options['mode'])
    except Exception, e:
        print "Got an exception at execute_query: "+str(e)
        return set(), {}

def execute_pipeline(pipeline, options):
    """ Executes the given pipeline and returns the combined result

    :param pipeline: The pipeline is a an ordered list of operations which
        are very specific. See OPERATIONS to find valid operations for the pipeline.
        Note: the 'customer' is a special case operation and should always be the last operation
              in the pipeline for the system to work correctly
    :param options: An object of options for the operations in the pipeline. Each operation may require
        certain options to be present. See OPTIONS for the list of valid operations

    :return: A list where each item is a list describing the record. If done correctly, the first two elements
        of each of these items will be the Phone and language in order. Each of these phones and numbers must
        be sent the sms
    """
    print "debug: got pipeline ", pipeline
    print "       and options ", options
    cid_set = set()
    extra_data = {}

    for operation in pipeline:
        s, res = _execute_event(operation, options)
        print "Got resulting set length ", len(s)
        if len(cid_set) == 0:
            cid_set = s
            extra_data = res
        else:
            cid_set = cid_set.intersection(s)
            temp = extra_data.copy()
            extra_data = {}
            if len(cid_set) == 0:       # shortcut
                break
            for k in cid_set:
                extra_data[k] = res[k] + temp[k]        # Both are arrays

    return extra_data.values()
