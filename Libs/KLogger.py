from KFS import KFS
import time
import datetime

class KLogger:
  def __init__(self, logpath):
    self.__kFS = KFS()
    self.__logpath = logpath
  
  def __handleLogpath(self):
    path = time.strftime(self.__logpath, time.localtime())
    path_arr = path.split('/')
    self.__logname = path_arr[-1]
    path_arr[-1] = ''
    self.__path = '/'.join(path_arr)
    self.__kFS.mknod(self.__path + self.__logname)
  
  def write(self, content):
    now = datetime.datetime.now()
    now = 'at %s' % (now)
    contents = [now, content]
    self.__handleLogpath()
    self.__kFS.writelines(self.__path + self.__logname, contents)


    

if __name__ == '__main__':
  kLogger = KLogger('log/%Y/%m/log.txt')
  kLogger.write('aaa')