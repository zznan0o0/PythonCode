from Libs.OpeartingDB import OpeartingDB
from Libs.KLogger import KLogger

DATABASESCONFIGPATH = 'Config/database.json'
LOGPATH = 'Log/%Y/%m.log'
opeartingDB = OpeartingDB(DATABASESCONFIGPATH)
kLogger = KLogger(LOGPATH)

con, cur = opeartingDB.connection('db_warehouse_glass')

def main():
  sql = 'select * from tb_glass limit 1'
  opeartingDB.CURSORPOOL['db_warehouse_glass'].execute(sql)
  res = cur.fetchall()
  print(res)
  kLogger.write('aaaa')



if __name__ == '__main__':
  main()
  opeartingDB.closeAll()