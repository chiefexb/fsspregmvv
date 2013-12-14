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
 #cfg.add_namespace(regmvv)
 print len(cfg.file_scheme)
 db = dbf.Dbf("/home/chief/work/root/1.dbf", new=True)
 reg=[]
 for i in range(0,len(cfg.file_scheme)):
  field=[]
  if len(cfg.file_scheme[i])==3:
   field.append(cfg.file_scheme[i]['field_name'])
   field.append(cfg.file_scheme[i]['field_type'])
   field.append(cfg.file_scheme[i]['field_size'])
  db.addField(field)
  reg.append(field)
 print db
 print reg[1][0]
 db.close() 
 
 #print const[cfg.reqxml[0]['node']] 
 #print cfg.reqxml[0]['node']
 f.close()
if __name__ == "__main__":
    main()
