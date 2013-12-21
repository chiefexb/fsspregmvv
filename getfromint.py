#!/usr/bin/python
#coding: utf8
from regmvv import *
from lxml import etree
from dbfpy import dbf
import fdb
import sys
def main():
#Обработка параметров
 print len (sys.argv)
 if len(sys.argv)<=1:
  print ("getfromint: нехватает параметров\nИспользование: getfromint ФАЙЛ_КОНФИГУРАЦИИ")
  sys.exit(2)
 print sys.argv[1]
#Открытие файла конфигурации
 try:
  f=file(sys.argv[1])
 except Exception,e:
  print e
  sys.exit(2)
#Парсим xml конфигурации
 cfg = etree.parse(f) 
 #cfg.add_namespace(regmvv)
 cfgroot=cfg.getroot()
#Ищем параметры системы
 systemcodepage=cfgroot.find('codepage').text
#Ищем параметры базы
 dbparams=cfgroot.find('database_params')
 username=dbparams.find('username').text
 password=dbparams.find('password').text
 hostname=dbparams.find('hostname').text
 concodepage=dbparams.find('connection_codepage').text
 codepage=dbparams.find('codepage').text
 database=dbparams.find('database').text
 print username,password,hostname,concodepage,codepage
#Ищем параметры МВВ
 mvv=cfgroot.find('mvv')
 agent_code=mvv.find('agent_code').text
 dept_code=mvv.find('dept_code').text
 agreement_code=mvv.find('agreement_code').text
 #f.close()
#Определяем тип и путь файла
 filepar=cfgroot.find('file')
 fliecodepage=filepar.find('codepage').text
 output_path=filepar.find('output_path').text
 filetype=filepar.find('type').text
 filenum=filepar.find('numeric').text
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filiescheme=filepar.find('scheme')
 #создание root
 print filiescheme.getchildren()[0].tag
 root=etree.Element(filiescheme.getchildren()[0].tag)
 #Определение заголовка

#Соединяемся с базой ОСП
 try:
  con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 cur = con.cursor() 
#Предварительная обработка 
#Определяем список необработанных пакетов
 packets=getnotprocessed(cur,systemcodepage,'CP1251',mvv_agent_code=agent_code,mvv_agreement_code=agreement_code,mvv_dept_code=dept_code)
 print len(packets)
 print str(type(agent_code)),str(type('Росреестр'))
 p=len(packets)
 #p=1
 #divname=getdivname(cur)
 #p=3
 for pp in range(0,p):
  print packets[pp][0]
  r=getrecords(cur,packets[pp][0])
  print "LEN="+str(len(r))
 f.close()
 con.close()
if __name__ == "__main__":
    main()
