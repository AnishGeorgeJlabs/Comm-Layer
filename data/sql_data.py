import pymysql

#db = pymysql.connect(user='maowadi', password='FjvQd3fvqxNhszcU', database='jerry_live', host="db02")
def connect_db(dbname):
    return pymysql.connect(
        user='maowadi',
        password='FjvQd3fvqxNhszcU',
        database=dbname,
        host='db02'
    )

db = connect_db('jerry_live')
