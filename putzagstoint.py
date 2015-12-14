#!/usr/bin/python
#coding: utf8
import logging
from os import * 
from regmvv import *
from lxml import etree
import fdb
import sys
import timeit
import time

def inform(st):
 logging.info(st)
 print st
 return
def informwarn(st):
 logging.warning(st)
 print st
 return
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        #print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".format(time.time() - self._startTime)
        st=u"Время выполнения:"+str(time.time() - self._startTime) # {:.3f} sec".format(time.time() - self._startTime)
        print st
        logging.info(st)
def main():
 logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s',level = logging.DEBUG, filename = './zags.log')
 clm=";"
#Обработка параметров
#Открытие файла конфигурации
 try:
  f=file('./zags.xml')
 except Exception,e:
  print e
  sys.exit(2)
#Парсим xml конфигурации
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

#Ищем параметры МВВ
 mvv=xmlroot.find('mvv')
 agent_code=mvv.find('agent_code').text
 dept_code=mvv.find('dept_code').text
 agreement_code=mvv.find('agreement_code').text
#Определяем тип и путь файла
 nd=xmlroot.find('input_path')
 input_path=nd.text
 nd=xmlroot.find('input_arc_path')
 input_arc_path=nd.text
 nd=xmlroot.find('output_path')
 output_path=nd.text

#Соединяемся с базой ОСП
 try:
  con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 sql1="INSERT INTO EXT_INPUT_HEADER (ID, PACK_NUMBER, PROCEED, AGENT_CODE, AGENT_DEPT_CODE, AGENT_AGREEMENT_CODE, EXTERNAL_KEY, METAOBJECTNAME, DATE_IMPORT, SOURCE_BARCODE) VALUES (?, ?, ?, ?, ?, ?,?, ?, ?, ?)"
 sql2="INSERT INTO EXT_DEBT_FIX (ID, ID_NUM, PD_NUM, IP_NUM, ID_DATE, PD_DATE, PAY_SUM, DEBTOR_ADR, DEBTOR_INN, DEBTOR_KPP, DEBTOR_OGRN, IS_PAYING_OFF, ID_EXTERNAL_KEY, DEBTOR_BIRTH_YEAR, DEBTOR_BIRTH_DATE, DEBTOR_NAME, CHANGEDBT_REASON_CODE, PENALTY_DATE, PAYMENT_PURPOSE) VALUES (?, ?,?, ?, ?, ?, ?, ?,?, ?,?, ?, ?, ?,?, ?, ?,?, ?)"
 sql3=""
 cur = con.cursor() 
 fld=['packet_id','id', 'ip_id','osp.packet_id','doc_number','id_dbtr_fullname','nametypeaz','namezags','numaz','numsv','mestolsub1','datesm','mestosm','prichsm']
 for ff in listdir(input_path):
  inform(u"Начинаем вставлять данные об оплате из файла:"+ff.decode('UTF-8'))
  with Profiler() as p:
   #f1=file(input_path+ff)
   #fl=f1.readlines()
   #st='ФИО СПИ;Номер ИП;Должник;Номер ИД;Сумма платежа;Дата платежа'
   #packid=getgenerator(cur,"DX_PACK")
   xmlfile=file(input_path+ff)
   xml=etree.parse(xmlfile)
   xmlroot=xml.getroot()
   ans=xmlroot.findall('answer')
   print ans[0].getchildren()
   aa={}
   #'ALTER SEQUENCE SEQ_EXT_INPUT_HEADER RESTART WITH 91121012388036'
   #select max(id) from ext_input_header 
   #TEST Генератора
   sq="SELECT GEN_ID(SEQ_EXT_INPUT_HEADER, 0) FROM RDB$DATABASE"
   cur.execute(sq)
   gg=cur.fetchone()[0]
   sq2='select max(id) from ext_input_header'
   cur.execute(sq2)
   maxid=cur.fetchone()[0]
   print gg,maxid
   if gg<maxid:
    st=u'Какая-то редиска не пользуется генератором SEQ_EXT_INPUT_HEADER. Исправляю!!'
    informwarn(st)
    sq3='ALTER SEQUENCE SEQ_EXT_INPUT_HEADER RESTART WITH '+str(maxid)
    cur.execute(sq3)
    con.commit()
    
   print gg
   for a in ans:
    for i in range(0,len(fld)):
     aa[fld[i]]=a.find(fld[i])
    sql1='INSERT INTO EXT_INPUT_HEADER (ID, PACK_NUMBER, PROCEED, AGENT_CODE, AGENT_DEPT_CODE, AGENT_AGREEMENT_CODE, EXTERNAL_KEY, METAOBJECTNAME, DATE_IMPORT, SOURCE_BARCODE) VALUES (id, packet_id, 0, agent_code, dept_code, agreement_code , extkey, 'EXT_REPORT', NULL, NULL);'
   #for j in range(0,len(fl)):
   # id=getgenerator(cur,"SEQ_DOCUMENT") #"SEQ_EXT_INPUT_HEADER")
   # d=datetime.datetime.now()
    #hsh=hashlib.md5()
    #hsh.update(str(id))
    #extkey=hsh.hexdigest()
    #cur.execute ("SELECT  doc_ip_doc.id , document.doc_number, doc_ip_doc.id_dbtr_name, DOC_IP.IP_EXEC_PRIST_NAME FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID    where document.doc_number="+quoted(ip_num))
    #rr=cur.fetchall()
    #st=''
    #if len (rr) <>0:
     #'ФИО СПИ;Номер ИП;Должник;Номер ИД;Сумма платежа;Дата платежа'
     #extkey=str(rr[0][0])
     #prim=(fls[8]+clm+fls[9]+clm+ff).decode('UTF-8')
     #sql1params=(id, packid, 0, agent_code, dept_code, agreement_code, extkey, "EXT_DEBT_FIX", pd_date,None)
     #sql2params=(id,id_num, None, ip_num, id_date, pd_date, pay_sum, None, None, None,None, 1, extkey, None, None, fio, None,None, prim);
     #cur.execute(sql1,sql1params)
     #cur.execute(sql2,sql2params)
    #else:
     #informwarn(u"Файл:"+ff.decode('UTF-8')+u". Не найдено ИП:"+ip_num)
  #inform(u"Меряем время коммита:")
  #with Profiler() as p:
  # con.commit()
  #f1.close()
  #rename(input_path+ff, input_arc_path+ff)
 f.close()
 con.close()

if __name__ == "__main__":
    main()
