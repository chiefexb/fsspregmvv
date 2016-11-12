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
from cStringIO import StringIO
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
 sql1="INSERT INTO DOCUMENT (DOCSTATUSID,DOCUMENTCLASSID, DOC_DATE, CREATE_DATE, ID, METAOBJECTNAME) VALUES (?,?,?,?,?,?) "
 sql2="INSERT INTO I_IP_OTHER (ID, INDOC_TYPE, INDOC_TYPE_NAME, I_IP_OTHER_CONTENT, NUM_POSTFIX) VALUES (?,?, ?, ?, ?)"
 sql3="INSERT INTO  I (ID,OFF_SPECIAL_CONTROL,CONTR_IS_INITIATOR,contr_name, agent_code,agent_dept_code,agreement_code,INITIATOR_EMAIL_COPY) VALUES (?,?,?,?,?,?,?,?)"
 sql4="INSERT INTO I_IP (ID,IP_ID) VALUES (?,?)"
 cur = con.cursor() 
 fld=['packet_id','id', 'ip_id','packet_id','doc_number','id_dbtr_fullname','nametypeaz','namezags','numaz','dateaz','numsv','mestolsub1','datesm','mestosm','prichsm']
 for ff in listdir(input_path):
  inform(u"Начинаем вставлять данные об оплате из файла:"+ff.decode('UTF-8'))
  with Profiler() as p:
   xmlfile=file(input_path+ff)
   xml=etree.parse(xmlfile)
   xmlroot=xml.getroot()
   ans=xmlroot.findall('answer')
   print ans[0].getchildren()
   aa={}
   for a in ans:
    for i in range(0,len(fld)):
     if str (type (a.find(fld[i]).text) )=="<type 'NoneType'>":
      aa[fld[i]]='б/н'
     else:
      aa[fld[i]]=(a.find(fld[i]).text).strip(' ')
    id=getgenerator(cur,"SEQ_DOCUMENT")       #"EXT_INFORMATION") #"SEQ_DOCUMENT") #"SEQ_EXT_INPUT_HEADER")
    d=datetime.datetime.now()
    sqlparam1= (105,272,d,d,id,"I_IP_OTHER")         #(DOCSTATUSID, DOC_DATE, CREATE_DATE, ID, METAOBJECTNAME)
    tt=[u'Сообщаем Вам что по имеющимя данным ЗАГСа <<', aa['namezags'],  u'>>, должник ',aa['id_dbtr_fullname'] ,u' является умершим. Номер свидетельства "', aa['numsv'], u'", дата свитетельства "',aa['dateaz'] , u'", место смерти ', (aa['mestosm']), u', дата смерти ', aa['datesm'] , '.']
    text=''
    for t in tt:
     #print t
     text=text+convtotype(['','C'],t,'UTF-8','UTF-8' )
    #print text
    sqlparam2= (id,None,None,text,None)             #  (ID, INDOC_TYPE, INDOC_TYPE_NAME, I_IP_OTHER_CONTENT, NUM_POSTFIX) VALUES (91121011252366, NULL, NULL, NULL, NULL);
    print 'NAMEZAGS', aa['namezags']
    sqlparam3= (id,0,0,aa['namezags'], agent_code,dept_code,agreement_code,False)              #(ID,OFF_SPECIAL_CONTROL,CONTR_IS_INITIATOR)
    sqlparam4= (id,int(aa['ip_id'])) #(ID,IP_ID) 
    #print sqlparam1,len(sqlparam1)
    #print sqlparam2,len(sqlparam2)
    print sqlparam3,len(sqlparam3)
    #print '1',sqlparam2
    cur.execute(sql1,sqlparam1)
    #print 2,sqlparam2

    cur.execute(sql2,sqlparam2)
    #print 3,sqlparam2
    #print sql3
    #st=u''
    #for sp in sqlparam3:
    #    st=st+unicode(sp)+u','
    #print '~!'+sql3+u'('+st
    cur.execute(sql3,sqlparam3)
    cur.execute(sql4,sqlparam4)
    con.commit()	
  xmlfile.close()
  rename(input_path+ff, input_arc_path+ff)
 f.close()
 con.close()

if __name__ == "__main__":
    main()
