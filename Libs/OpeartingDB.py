# import psycopg2
# import pymssql
import pymysql
# import json
import copy
import math
import yaml



class OpeartingDB:
    def __init__(self, config_path='Config/database.yaml', db_config=False):
        if not db_config:
            db_config_file = open(config_path)
            db_config = yaml.load(db_config_file)
            db_config_file.close()

        self._config = db_config
        self.CONNECTPOOL = {}
        self.CURSORPOOL = {}

    def connectionMSSql(self, database):
        db_config = self._config[database]
        connect = pymssql.connect(database=db_config['database'], user=db_config['user'], password=db_config['password'], host=db_config['host'], port=db_config['port'],charset="utf8")
        cursor = connect.cursor(as_dict=True)
        return connect, cursor

    def connection(self, database):
        db_config = self._config[database]
        connect = pymysql.connect(database=db_config['database'], user=db_config['user'], password=db_config['password'],
                                  host=db_config['host'], port=db_config['port'], cursorclass=pymysql.cursors.DictCursor, charset="utf8")
        cursor = connect.cursor()

        self.CONNECTPOOL[database] = connect
        self.CURSORPOOL[database] = cursor

        return connect, cursor

    def commitAll(self):
        for k, v in self.CONNECTPOOL.items():
            v.commit()

    def closeAll(self):
        for k, v in self.CURSORPOOL.items():
            v.close()

        for k, v in self.CONNECTPOOL.items():
            v.close()

        self.CURSORPOOL = {}
        self.CONNECTPOOL = {}

    def close(self, database):
        self.CURSORPOOL[database].close()
        self.CONNECTPOOL[database].close()
        del self.CURSORPOOL[database]
        del self.CONNECTPOOL[database]

    def insert(self, data, database, table):
        if(len(data) < 1):
            return 0

        keys = data[0].keys()
        s = ['%s' for i in range(len(keys))]
        sql = "insert into %s (%s) values (%s);" % (
            table, ','.join(keys), ','.join(s))
        tuple_list = self.covertTuple(data, keys)
        self.CURSORPOOL[database].executemany(sql, tuple_list)
    
    def queryUpdateSetStr(self, data):
        keys = [v for v in data]
        setstr = ",".join([ "%s" % (v) + "=%s" for v in keys ])
        values = self.covertTuple([data], keys)[0]
        return {'str': setstr, 'keys': keys, 'values': values}

    def updateById(self, data, database, table, idFiled):
        if(len(data) < 1):
            return 0
        
        keys = [v for v in data[0]]
        setstr = ",".join([ "%s" % (v) + "=%s" for v in keys ])

        sql = ("update %s set %s " % (table, setstr)) + (" where %s=" % (idFiled) + "%s;")
        keys.append(idFiled)

        tuple_list = self.covertTuple(data, keys)
        self.CURSORPOOL[database].executemany(sql, tuple_list)
    
    def insertUpdateDataById(self, data, database, table, idFiled):
        ids = [ v[idFiled] for v in data ]
        sql = "select %s from %s where %s in (%s);" % (idFiled, table, idFiled, self.getWhereInString(ids))
        self.CURSORPOOL[database].execute(sql)
        ids = self.CURSORPOOL[database].fetchall()
        ids = [ v[idFiled] for v in ids ]
        update_data = []
        insert_data = []
        for v in data:
            if v[idFiled] in ids:
                update_data.append(v)
            else:
                insert_data.append(v)
        
        self.updateById(update_data, database, table, idFiled)
        self.insert(insert_data, database, table)


    def insertBigData(self, data, database, table):
        if(len(data) < 1): return 0

        keys = data[0].keys()
        s = ['%s' for i in range(len(keys))]
        sql = "insert into %s (%s) values (%s);" % (table, ','.join(keys), ','.join(s))

        per_count = 10
        sur_count = len(data) % per_count
        count = math.ceil(len(data) / per_count)

        tuple_list = self.covertTuple(data, keys)

        for ci in range(count):
            star_num = ci * per_count
            end_num = per_count + star_num
            if ci == count - 1:
                end_num = sur_count + star_num

        self.CURSORPOOL[database].executemany(sql, tuple_list[star_num:end_num])

    def addQuotes(self, x):
        return "'%s'" % x

    def getWhereInString(self, array):
        return ','.join(list(map(self.addQuotes, array)))

    def covertTuple(self,  d, k):
        tuple_list = []
        for v in d:
            tuple_tmp = []
            for kv in k:
                val = self.isKey(kv, v, kv)
                tuple_tmp.append(val)

            tuple_list.append(tuple(tuple_tmp))

        return tuple_list

    def addPropOneToOne(self, d1, d2, k1, p, k2=False):
        k2 = k2 if k2 else k1
        d1 = copy.deepcopy(d1)
        d2 = copy.deepcopy(d2)
        d2_dic = self.convertDic(d2, k2)
        for v in d1:
            key = self.covertKey(v, k1)
            if key in d2_dic.keys():
                self.addProp(v, d2_dic[key], p)
            else:
                self.addProp(v, {}, p)

        return d1

    def addProp(self, d1, d2, p):
        for v in p:
            int_val = '' if len(v) <= 2 else v[2]
            d1[v[0]] = self.isKey(v[1], d2, int_val)

    def convertDic(self, d, ks):
        d = copy.deepcopy(d)
        dic = {}
        for v in d:
            k_arr = []
            key = self.covertKey(v, ks)
            dic[key] = v

        return dic

    def covertKey(self, d, k):
        k_arr = []
        for v in k:
            k_arr.append(d[v])

        return ','.join(k_arr)

    def isKey(self, k, data, v=''):
        return v if k not in data.keys() else data[k]



if __name__ == "__main__":
    opeartingDB = OpeartingDB()
    opeartingDB.connection('db_warehouse_glass')
    opeartingDB.CURSORPOOL['db_warehouse_glass'].execute('select * from tb_glass limit 10;')
    print(opeartingDB.CURSORPOOL['db_warehouse_glass'].fetchall())
    opeartingDB.closeAll()
