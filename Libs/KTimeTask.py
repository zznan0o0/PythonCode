import datetime
import time
import threading


class KTimeTask:
    def __init__(self):
        self.__dayfn = {}
        self.__timefn = []

    def time(self, time_format, time_str, fn):
        self.__timefn.append({
            'time_format': time_format,
            'time_str': time_str,
            'fn': fn
        })

    def runTime(self, dtime):
        for v in self.__timefn:
            dtime_str = dtime.strftime(v['time_format'])
            if dtime_str == v['time_str']:
                v['fn']()

    def day(self, time_str, fn):
        if time_str not in self.__dayfn.keys():
            self.__dayfn[time_str] = []
        self.__dayfn[time_str].append(fn)

    def runDay(self, dtime):
        dtime_str = dtime.strftime("%H:%M:%S")
        for k, v in self.__dayfn.items():
            if k == dtime_str:
                for fn_v in v:
                    fn_v()

    def run(self, dtime):
        self.runDay(dtime)
        self.runTime(dtime)

    def start(self):
        while True:
            dtime = datetime.datetime.now()
            t = threading.Thread(target=self.run, args=(dtime, ))
            t.start()
            time.sleep(1)


if __name__ == '__main__':
    def aaa():
        print(111)

    kTimeTask = KTimeTask()
    kTimeTask.time('%S', '05', aaa)
    kTimeTask.start()

    # kTimeTask.day('10:09:01', aaa)
    # kTimeTask.start()
