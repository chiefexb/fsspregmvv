#!/usr/bin/python
#coding: utf8
import regmvv
from config import Config
import sys
def main():
 if len(sys.argv)<1:
  print ("getfromint: нехватает параметров\nИспользование: getfromint ФАЙЛ_КОНФИГУРАЦИИ")
  sys.exit(2)
 print sys.argv[1]
 try:
  f=file(sys.argv[1])
 except Exception,e:
  print e
  sys.exit(2)
 cfg = Config(f) 
 cfg.add_namespace(regmvv)
 print cfg.hostname
 #print const[cfg.reqxml[0]['node']] 
 print cfg.reqxml[0]['node']
 f.close()
if __name__ == "__main__":
    main()
