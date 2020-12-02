import sys
import getopt
sys.path.append("..")

help_str = """
eg: mysqlha -i /etc/mysqlha/mysqlha.yaml -g /etc/mysqlha/mysqlhastate.yaml -l /var/log/mysqlha

-h, --help      display the help
-i, --init      set the profile directory (default current directory ./Config/database.yaml)
-g, --generate  set the health file directory (default ./mysqlhastate.yaml)
-l, --log       set the log file directory (default ./log)
-r, --reset     reset the mysql master-slave state by state yaml or init yaml 
"""


class HandleOpt:
    @staticmethod
    def getOPT():
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hi:g:l:r", ["help", "init=", "generate=", "log=", "reset"])
        except getopt.GetoptError as e:
            print(e)
            sys.exit(2)
        
        sopt = {
            'i': '',
            'g': '',
            'l': '',
            'r': False,
        }
        
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print(help_str)
                sys.exit()
            
            elif opt in ("-i", "--init"):
                sopt['i'] = arg
            
            elif opt in ("-g", "--generate"):
                sopt['g'] = arg

            elif opt in ("-l", "--log"):
                sopt['l'] = arg

            elif opt in ("-r", "--reset"):
                sopt['r'] = True
        
        return sopt

if __name__ == "__main__":
    print(HandleOpt.getOPT())
    

