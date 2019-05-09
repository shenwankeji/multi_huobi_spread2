from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure

########################################################################
class DBEngine(object):
    """数据库引擎"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        # MongoDB数据库相关
        self.dbClient = None  # MongoDB客户端对象

    # ----------------------------------------------------------------------
    def dbConnect(self):
        """连接MongoDB数据库"""
        if not self.dbClient:
            # 读取MongoDB的设置
            try:
                # 设置MongoDB操作的超时时间为0.5秒
                self.dbClient = MongoClient('localhost', 27017, connectTimeoutMS=500)

                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()

                print('MongoDB is connected.')

            except ConnectionFailure:
                print('Failed to connect to MongoDB.')

    #----------------------------------------------------------------------
    def dbInsert(self, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        if self.dbClient:
            d = d.__dict__
            db = self.dbClient[dbName]
            collection = db[collectionName]
            #键值重复，需要删除
            if '_id' in d:
                del d['_id']
            collection.insert_one(d)
        else:
            print('Data insert failed，please connect MongoDB first.')