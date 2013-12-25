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
 try:
  con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 cur = con.cursor() 
 fn='./for_UFSSP_20131219_184058.txt'
 fp=file(fn)
 fpl=fp.readlines()
 print len(fpl)
 rr2=[]
 r2=[]
 r3=[]
 for st in fpl:
  #Пропуск #
  stt=[]
  stt2=[]
  st2=st.decode('CP1251')
  if st2[0]<>'#':
   st3=st2.split(';')
   #num= st3[1][0:2]+' '+st3[1][2:4]+' '+st3[1][4:11]
   r2.append([st3[1]])
   for i in range(0,11):
    rr2.append(st3[i])
   #r3.append({st3[1]:rr2}) 
   stt2.append(rr2)
# sql="SELECT DOC_IP.ID,DOC_IP_DOC.ID_DOCNO FROM  DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID WHERE DOCUMENT.DOCSTATUSID IN (9)"
 sql=" SELECT doc_ip.ID,IP_RISEDATE,DOC_NUMBER,IP_EXEC_PRIST_NAME,ID_DBTR_NAME,ID_DBTR_INN,ID_DEBTSUM,IP_REST_DEBTSUM,ID_DOCNO      FROM  DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID WHERE DOCUMENT.DOCSTATUSID IN (9) and DOC_IP_DOC.ID_DEBTCLS IN (37)" 
#and upper(DOC_IP_DOC.ID_DOCNO) containing '"+num+"'"
 cur.execute(sql.encode('CP1251'))
 rr=cur.fetchall()
 #print rr[0][1]
  # print st3[0]
  #  if len(rr)<>0:
  #   print rr[0]
  #  rr2=append[rr]
 print len(rr)
 for i in range(len(rr)):
  #if str(type(rr[i]))<>'NoneType':
  try:
   rs= rr[i][8].replace(' ' ,'')
  except:
   print str(type(rr[i][8]))
  if rs in r2:
    print rs,rr[i][5]
# print rr[i][0],rr[i][1]
#  print rr[i][0],rr[i][1]
#tr3=map(rr2-rr
# print rr[0][1]
# print st3[1]
#Получить список файлов в папке input
 #ff=file('./1.txt','w')
 #ff.writelines(ee)
 f.close()
 #ff.close()
 con.close()
if __name__ == "__main__":
    main()
