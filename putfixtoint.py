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
 logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s',level = logging.DEBUG, filename = './putfixtoint.log')
 clm=";"
#Обработка параметров
#Открытие файла конфигурации
 try:
  f=file('./config.xml')
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
 cur = con.cursor() 
 for ff in listdir(input_path):
  inform(u"Начинаем вставлять данные об оплате из файла:"+ff.decode('UTF-8'))
  with Profiler() as p:
   #ff='9112_2014_07_27_11_fix.txt'
   f1=file(input_path+ff)
   #ff2=ff.replace('.txt','')+'.csv'
   #f2=file(output_path+ff2,'w')
   fl=f1.readlines()
   st='ФИО СПИ;Номер ИП;Должник;Номер ИД;Сумма платежа;Дата платежа'
   #f2.write(st+'\n')
   packid=getgenerator(cur,"DX_PACK")
   for j in range(0,len(fl)):
    id=getgenerator(cur,"SEQ_DOCUMENT") #"SEQ_EXT_INPUT_HEADER")
    d=datetime.datetime.now()
    #hsh=hashlib.md5()
    #hsh.update(str(id))
    #extkey=hsh.hexdigest()
    fls= fl[j].split(";")
    id_num=fls[0]
    id_date=datetime.datetime.strptime(fls[1],'%d.%m.%Y') 
    fio=(fls[2]+' '+fls[3]+' '+fls[4]).decode('UTF-8')
    pd_date=datetime.datetime.strptime(fls[6],'%d.%m.%Y')
    pay_sum=float(fls[5])
    ip_num=fls[14].decode('UTF-8')
    cur.execute ("SELECT  doc_ip_doc.id , document.doc_number, doc_ip_doc.id_dbtr_name, DOC_IP.IP_EXEC_PRIST_NAME FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID    where document.doc_number="+quoted(ip_num))
    rr=cur.fetchall()
    st=''
    if len (rr) <>0:
     #'ФИО СПИ;Номер ИП;Должник;Номер ИД;Сумма платежа;Дата платежа'
     pristav=(rr[0][3])
     st=st+pristav+clm+ip_num+clm+fio+id_num+clm+str(pay_sum)+clm+pd_date.strftime('%d.%m.%Y') +'\n'
     #print str(type(st))
     #f2.write(st.encode('UTF-8'))
     extkey=str(rr[0][0])
     prim=(fls[8]+clm+fls[9]+clm+ff).decode('UTF-8')
     #print str(type(prim))
     #print "DD", prim,len(prim)
     sql1params=(id, packid, 0, agent_code, dept_code, agreement_code, extkey, "EXT_DEBT_FIX", pd_date,None)
     sql2params=(id,id_num, None, ip_num, id_date, pd_date, pay_sum, None, None, None,None, 1, extkey, None, None, fio, None,None, prim);
     cur.execute(sql1,sql1params)
     cur.execute(sql2,sql2params)
    else:
     informwarn(u"Файл:"+ff.decode('UTF-8')+u". Не найдено ИП:"+ip_num)
  inform(u"Меряем время коммита:")
  with Profiler() as p:
   con.commit()
  f1.close()
  #f2.close()
  rename(input_path+ff, input_arc_path+ff)
 f.close()
 con.close()

if __name__ == "__main__":
    main()
