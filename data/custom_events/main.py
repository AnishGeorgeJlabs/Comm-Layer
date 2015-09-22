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
import event_platform
import event_channel
import event_origination_month
import aux
from . import createLogger

cLogger = createLogger("custom_events")

drivers = {
    'category': event_category.operate,
    'customer': event_customer.operate,
    'item_count': event_item_count.operate,
    'payment_method': event_payment_method.operate,
    'repeat_buyer': event_repeat_buyer.operate,
    'item_status': event_item_status.operate,
    'purchase_month': event_month.operate,
    'origination_month': event_origination_month.operate,
    'platform': event_platform.operate,
    'channel': event_channel.operate
}


def _execute_event(operation, mode, options):
    print "Executing operation ", operation
    try:
        if operation in drivers:
            return drivers[operation](mode, options)
        else:
            print "Unimplemented operation: " + operation
            return None, None, None
    except Exception, e:
        print "Got an exception at execute_query: " + str(e)
        return set(), {}


def execute_pipeline(pipeline, options):
    """ Executes the given pipeline and returns the combined result

    :param pipeline: The pipeline is a dictionary of two operation lists:
        additional: This is an ordered list of operations, whose results will be added together in a union operation
        required: Results of the operations in this list will be intersected so that the final result satisfies each of
                  the operation. Also, the result of the 'additional' list of operations will be intersected with.
      Please note, 'customer' operation is a special case operation and should be the last operation in the 'required' list
        so that the system may work correctly

    :param options: An object of options for the operations in the pipeline. Each operation may require
        certain options to be present. See OPTIONS for the list of valid operations

    :return: A list where each item is a list describing the record. If done correctly, the first two elements
        of each of these items will be the Phone and language in order. Each of these phones and numbers must
        be sent the sms
    """
    try:
        print "debug: got pipeline ", pipeline
        print "       and options ", options

        if isinstance(pipeline, list):          # Supporting the old api for now
            cid_set, data, headers = _execute_sub_pipe(
                pipeline=pipeline,
                cid_set=set(),
                extra_data=dict(),
                main_headers=[],
                options=options,
                op_mode='and'
            )
        else:                                   # New API, pipeline is a dict
            cid_set, data, headers = _execute_sub_pipe(
                pipeline=pipeline.get('additional', pipeline.get('or', [])),
                cid_set=set(),
                extra_data=dict(),
                main_headers=[],
                options=options,
                op_mode='or'
            )
            cid_set, data, headers = _execute_sub_pipe(
                pipeline=pipeline.get('required', pipeline.get('and', [])),
                cid_set=cid_set,
                extra_data=data,
                main_headers=headers,
                options=options,
                op_mode='and'
            )
            cid_set, data, headers = _execute_sub_pipe(
                pipeline=pipeline.get('excluded', pipeline.get('not', [])),
                cid_set=cid_set,
                extra_data=data,
                main_headers=headers,
                options=options,
                op_mode='not'
            )

        block_set = aux.get_block_set()
        records = map(
            lambda k: [str(k[0]).strip('+ ').replace('-', '')] + k[1:],
            data.values()
        )

        res = [v for v in records if ','.join(v[0:2]) not in block_set]
        return res, headers
    except Exception:
        cLogger.exception("execute pipeline crashed with pipeline %s and options %s", str(pipeline), str(options))
        return [], ['Phone', 'Language']


def _execute_sub_pipe(pipeline, cid_set, extra_data, main_headers, options, op_mode="and"):
    """ Executes a single sub-pipeline in the OR fashion or AND fashion

    :param pipeline: An ordered list of operations
    :param cid_set: A set of the customer ids, from previous sub-pipe or an empty one
    :param extra_data: A dictionary of customer data, from previous sub-pipe or empty
    :param main_headers: The current list of headers. from previous sub-pipe or empty
    :param options: The main options object
    :param op_mode: Either of 'and' or 'or'
    :return: Same as a query_event
    :rtype: (set, dict, list)
    """

    for operation in pipeline:
        s, res, headers = _execute_event(operation, aux.get_mode(options), options)
        if s is None:  # In case of unimplemented operation, we move on to next
            continue

        if len(cid_set) == 0:
            cid_set = s
            extra_data = res
            if len(cid_set) == 0:  # shortcut
                break
        else:

            if op_mode == "and":
                cid_set = cid_set.intersection(s)
            elif op_mode == "or":
                cid_set = cid_set.union(s)
            else:
                cid_set = cid_set.difference(s)

            temp = extra_data.copy()
            extra_data = {}
            if len(cid_set) == 0:  # shortcut
                break

            c_empty = ['' for _ in range(len(main_headers))]  # Empty array for those who are not part of current set
            f_empty = ['' for _ in range(len(headers))]  # Emtpy array for those who are not part of coming set

            for k in cid_set:
                extra_data[k] = (res.get(k, f_empty) if op_mode != 'not' else []) + temp.get(k, c_empty)  # Both are arrays

        print "Got resulting set length ", len(s)
        main_headers = (headers if op_mode != 'not' else []) + main_headers

    return cid_set, extra_data, main_headers
