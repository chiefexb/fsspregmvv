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
 filecodepage=filepar.find('codepage').text
 output_path=filepar.find('output_path').text
 intput_path=filepar.find('input_path').text
 intput_arc_path=filepar.find('input_path_arc').text
 filetype=filepar.find('type').text
 filenum=filepar.find('numeric').text
#Соединяемся с базой ОСП
 #try:
 # con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
 #except  Exception, e:
 # print("Ошибка при открытии базы данных:\n"+str(e))
 # sys.exit(2)
 #cur = con.cursor() 
 fn='./for_UFSSP_20131219_184058.txt'
 fp=file(fn)
 fpl=fp.readlines()
 print len(fpl)
 for st in fpl:
  #Пропуск #
  st2=st.decode('CP1251')
  if st2[0]<>'#':
   st3=st2.split(';')
   print st3[1][0]+ st3[1][1]

#Получить список файлов в папке input
 f.close()
# con.close()
if __name__ == "__main__":
    main()
