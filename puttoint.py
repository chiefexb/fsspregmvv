#!/usr/bin/python
#coding: utf8
import regmvv
from config import Config
from dbfpy import dbf
import sys
def main():
 print len (sys.argv)
 if len(sys.argv)<=1:
  print ("getfromint: нехватает параметров\nИспользование: getfromint ФАЙЛ_КОНФИГУРАЦИИ")
  sys.exit(2)
 print sys.argv[1]
 try:
  f=file(sys.argv[1])
 except Exception,e:
  print e
  sys.exit(2)
 cfg = Config(f) 
#Соединяемся с базой ОСП
 
 try:
  con = fdb.connect (host=cfg.hostname, database=cfg.database, user=cfg.username, password=cfg.password,charset=cfg.database_connection_codepage)
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 cur = con.cursor() 
 db.close() 
 
 f.close()
 con.close()
if __name__ == "__main__":
    main()
