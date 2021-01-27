import pymongo
import json
import threading
import os
import time
from bson import ObjectId
import datetime
import optparse


usage = "python %prog -s/--source <source url> -d/--dest <dest url>"
parser = optparse.OptionParser(usage)  # 写入上面定义的帮助信息
parser.add_option('-s', '--source', dest='source', type='string',
                  help='源地址')
parser.add_option('-d', '--dest', dest='dest',
                  type='string', help='目标地址')

options, args = parser.parse_args()

source = options.source

conn = pymongo.MongoClient(source)
db = conn.production


dest = options.dest
conn2 = pymongo.MongoClient(dest)
db2 = conn2.production


def insert(next):
    try:
        print(f'写入数据{next}')
        coll = next['ns']['coll']
        db2[coll].insert_one(next['fullDocument'])
        print(f'写入数据{next}成功！')
    except Exception as e:
        print(e)


def update(next):
    try:
        print(f'修改数据{next}')
        coll = next['ns']['coll']
        db2[coll].update_one(next['documentKey'], {
            '$set': next['updateDescription']['updatedFields']})
        print(f'修改数据{next}成功！')
    except Exception as e:
        print(e)


def replace(next):
    try:
        print(f'替换数据{next}')
        coll = next['ns']['coll']
        db2[coll].replace_one(
            next['documentKey'], next['fullDocument'])
        print(f'替换数据{next}成功！')
    except Exception as e:
        print(e)


def delete(next):
    try:
        print(f'删除数据{next}')
        coll = next['ns']['coll']
        db2[coll].delete_one(next['documentKey'])
        print(f'删除数据{next}成功！')
    except Exception as e:
        print(e)


def watchMongo(c):
    watchCursor = db[c].watch()
    print(f'collection: {c} 已启动监听...')
    while True:
        next = watchCursor.next()
        print(next)
        if next['operationType'] == 'replace' or next['operationType'] == 'insert' or next['operationType'] == 'update':
            try:
                if next['fullDocument'].get('updatedAt', None) != None:
                    if type(next['fullDocument']['updatedAt']) == type(datetime.datetime(2020, 1, 1)):
                        next['fullDocument']['updatedAt'] = int(
                            next['fullDocument']['updatedAt'].timestamp()) * 1000
                if next['fullDocument'].get('createdAt', None) != None:
                    if type(next['fullDocument']['createdAt']) == type(datetime.datetime(2020, 1, 1)):
                        next['fullDocument']['createdAt'] = int(
                            next['fullDocument']['createdAt'].timestamp()) * 1000
            except Exception as e:
                print(e)

        dump = {
            'name': '备份数据!',
            'data': next
        }
        db2.backup.insert_one(dump)


def run():
    for c in db.collection_names():
        t = threading.Thread(target=watchMongo, args=(c,))
        t.start()


def dump():
    import os
    dpath = "dump"
    if not os.path.exists(dpath):
        os.mkdir(dpath)

    print('正在将数据备份至本地...')
    os.system(f'mongodump --uri="{source}" --out={dpath} --forceTableScan && mongorestore --uri="{dest}" --dir={dpath}')


def updateColections():
    while True:
        toDest()
        time.sleep(3)

def toDest():
    for next in db2.backup.find(no_cursor_timeout=True):
        if next['data']['operationType'] == 'insert':
            insert(next['data'])
        elif next['data']['operationType'] == 'update':
            update(next['data'])
        elif next['data']['operationType'] == 'replace':
            replace(next['data'])
        elif next['data']['operationType'] == 'delete':
            delete(next['data'])

        db2.backup.delete_one({'_id': ObjectId(next['_id'])})


if __name__ == "__main__":
    run()
    dump()
    updateColections()
    print(source, dest)
