#!/usr/bin/python
#coding: utf8
#from regmvv import *
from lxml import etree
#from dbfpy import dbf
from datetime import *
import timeit
import time
import fdb
import sys
from os import *
import logging
def main():
#Обработка параметров
 #print len (sys.argv)
#Открытие файла конфигурации
 try:
  f=file('./config.xml')
 except Exception,e:
  print e
  sys.exit(2)
 xml = etree.parse(f)
 #cfg.add_namespace(regmvv)
 xmlroot=xml.getroot()
#Ищем параметры системы
#Ищем параметры базы
 main_database=xmlroot.find('main_database')
 main_dbname=main_database.find('dbname').text
 main_user=main_database.find('user').text
 main_password=main_database.find('password').text
 main_host=main_database.find('hostname').text
#Соединяемся с базой ОСП
 try:
  con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 cur=con.cursor()
 sql="select * from osp_versions"
 cur.execute(sql)
 r=cur.fetchall()
 print u'Версия:',r[0][1],u' дата обновления ',r[0][2],' дата актуальности ', r[0][0]
# print r
if __name__ == "__main__":
    main()
