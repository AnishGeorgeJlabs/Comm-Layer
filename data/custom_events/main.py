"""
Root algorithm for event queries
"""
import event_category
import event_customer
import event_item_count
import event_payment_method
import event_repeat_buyer
import event_item_status
import event_month
import aux


drivers = {
    'category': event_category.operate,
    'customer': event_customer.operate,
    'item_count': event_item_count.operate,
    'payment_method': event_payment_method.operate,
    'repeat_buyer': event_repeat_buyer.operate,
    'item_status': event_item_status.operate,
    'purchase_month': event_month.operate
}

def _execute_event(operation, mode, options):
    print "Executing operation ", operation
    try:
        if operation in drivers:
            return drivers[operation](mode, options)
        else:
            print "Unimplemented operation: "+operation
            return None, None, None
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
    main_headers = []
    mode = aux.get_mode(options)

    for operation in pipeline:
        s, res, headers = _execute_event(operation, mode, options)
        if s is None:                           # In case of unimplemented operation, we move on to next
            continue
        print "Got resulting set length ", len(s)
        main_headers = headers + main_headers
        if len(cid_set) == 0:
            cid_set = s
            extra_data = res
            if len(cid_set) == 0:       # shortcut
                break
        else:
            cid_set = cid_set.intersection(s)
            temp = extra_data.copy()
            extra_data = {}
            if len(cid_set) == 0:       # shortcut
                break
            for k in cid_set:
                extra_data[k] = res[k] + temp[k]        # Both are arrays

    return extra_data.values(), main_headers
