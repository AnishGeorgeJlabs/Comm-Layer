import pymongo
import os

dbclient = pymongo.MongoClient("45.55.232.5:27017")
dbclient.wadi.authenticate('wadiAdmin', 'secureWadiOp', mechanism='MONGODB-CR')
db = dbclient.wadi

local_dir = os.path.dirname(__file__)


def process_csv(records):
    """
    :param records: The csv parsed tuples phone | english | arabic | country | name | email
    :return: The list of records to be inserted
    """
    true_cases = ['y', 'yes', 'true']

    result = []
    for record in records:
        try:
            data = {'phone': record[0].strip().replace('+', '').replace('-', ''), 'language': []}
            if record[1].lower() in true_cases:
                data['language'].append('English')
            if record[2].lower() in true_cases or len(data['language']) == 0:
                data['language'].append('Arabic')

            if len(record) >= 4 and record[3].lower() in ['uae', 'ae']:
                data['country'] = 'UAE'
            else:
                data['country'] = 'KSA'

            if len(record) >= 5 and record[4] != '':
                data['name'] = record[4]
            if len(record) >= 6 and record[5] != '':
                data['email'] = record[5]

            result.append(data)
        except Exception, e:
            print "Got excepted: "+str(e)
            continue

    return result

def transform_to_bulk_write(records):
    return [
        pymongo.UpdateOne({'phone': d['phone']}, {"$set": d}, upsert=True)
        for d in records
    ]

if __name__ == '__main__':
    import glob
    import csv
    import itertools

    lst = glob.glob(os.path.join(local_dir, "*.csv"))
    print lst
    for fl in lst:
        if not fl.startswith('completed_'):
            with open(fl, 'r') as cfile:
                reader = itertools.islice(csv.reader(cfile), 1, None)
                # reader = csv.reader(cfile)
                bl_write = transform_to_bulk_write(process_csv(reader))
                db.external_data.bulk_write(bl_write)
            os.rename(fl, 'completed_'+fl)
