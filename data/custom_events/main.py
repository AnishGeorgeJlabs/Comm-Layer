"""
Root algorithm for event queries
"""
from event_category import get_category
from event_customer import get_customer


def execute_event(operation, options):
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

    print "debug: got pipeline ", pipeline
    print "       and options ", options
    cid_set = set()
    extra_data = {}

    for operation in pipeline:
        s, res = execute_event(operation, options)
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
