#!/usr/bin/python
#coding: utf8
from regmvv import *
from lxml import etree
from dbfpy import dbf
from datetime import *
import timeit
import time
import fdb
import sys
from os import *
import logging
def quoted(a):
 st="'"+a+"'"
 return st

class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".forma$
        st="Elapsed time:"+str(time.time() - self._startTime) # {:.3f} sec".for$
        logging.info(st)
def getgenerator(cur,gen):
 sq="SELECT GEN_ID("+gen+", 1) FROM RDB$DATABASE"
 try:
  cur.execute(sq)
 except:
  print "err"
 cur.execute(sq)
 r=cur.fetchall()
 try:
  g=r[0][0]
 except:
  g=-1
 return g
def main():
 logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s',  level = logging.DEBUG, filename = './ppa.log')
 try:
  f=file('./ppaconfig.xml')
 except Exception,e:
  print e
  sys.exit(2)
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
 filepar=cfgroot.find('file')
 input_path=filepar.find('input_path').text
 input_arc_path=filepar.find('input_path_arc').text
 st=u'Начало процесса загрузки, файлов для обработки:'+str( len( listdir(input_path) ))
 logging.info( st )
 #connect
 try:
  con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 cur = con.cursor() 
 sq1="INSERT INTO EXT_INPUT_HEADER (ID, PACK_NUMBER, PROCEED, AGENT_CODE, AGENT_DEPT_CODE, AGENT_AGREEMENT_CODE, EXTERNAL_KEY, METAOBJECTNAME, DATE_IMPORT, SOURCE_BARCODE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
 sq2="INSERT INTO EXT_PPA (ID, PAYER_INN, PAYDOC_DATE, PAYDOC_AMOUNT, PAYDOC_NUMBER, PAYER_ACCOUT, PAYER_CORACC, PAYER_NAME, PAYDOC_GROUND, PAYER_BANK_NAME, PAYER_OGRN, PAYER_KPP, PAYER_BIC, PAYER_TYPEID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
 sq3="INSERT INTO EXT_PPA_LINES (ID, LINE_AMOUNT, IP_NUMBER, DEBTOR_NAME, LINE_EXTERNAL_KEY, DEBTOR_INN, DEBTOR_BIRTHDATE, DEBTOR_OGRN, DEBTOR_ADDRESS, DEBTOR_KPP, EXTERNAL_KEY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
 contrtype=1
#1010201000000
 if  len(listdir(input_path))==0:
  inform(u'Нет файлов для загрузки')
 else:
   for ff in listdir(input_path):
    xmlfile=file(input_path+ff) #'rr4.xml')
    print ff
    #ext_input_header
    xml=etree.parse(xmlfile)
    xmlroot=xml.getroot()  
    #initgen  ALTER SEQUENCE SEQ_EXT_INPUT_HEADER RESTART WITH 91021000000002
    cur.execute("select max(id),gen_id(seq_ext_input_header,0) from ext_input_header")
    r=cur.fetchone()
    print type(r[0]),type(r[1]),r[0],r[1]
    if r[1]<r[0]:
     print 'Max' 
     cur.execute ( "ALTER SEQUENCE SEQ_EXT_INPUT_HEADER RESTART WITH "+str(r[0]))
     con.commit()
    id=getgenerator(cur,'SEQ_EXT_INPUT_HEADER')
    packetid=getgenerator(cur,'DX_PACK')
    agent_code=u'СБРФ'
    agent_dept_code=u'СБРФСТАВР'
    agent_agreement_code =u'СБРФИНТ'
    dt=datetime.now()
  
    dataarea=xmlroot.find('DataArea')
    header=dataarea.find('Header')
    report=header.find('Report')
    #id=1
    #extkey=header.find('DocGUID').text
    hsh=hashlib.md5()
    hsh.update( str(id))
    extkey =hsh.hexdigest()
    print extkey

    pr1=(id, packetid, 0, agent_code, agent_dept_code, agent_agreement_code, extkey, 'EXT_PPA', dt, '')
    #ext_ppa
    #print extkey,(len(extkey))
    payyer_inn=report.find('INN_PLAT_PP').text
    print payyer_inn ,len (payyer_inn)
    if len (payyer_inn)==10:
     payyer_inn='0123456789'
    paydoc_dates= report.find('DATE_PP').text
    paydoc_date=datetime.strptime(paydoc_dates, "%Y-%m-%d")
    paydoc_amount= float( report.find('SUM_PP').text)/100
    paydoc_number= report.find('NOM_PP').text
    payyer_bik=report.find('ID_KO').text
    regpp=report.findall('RegPP')
    cur.execute ('select bik_namep,bik_ksnp from nsi_bik   where nsi_bik.bik_newnum='+quoted(payyer_bik))
    print 'select bik_namep,bik_ksnp from nsi_bik   where nsi_bik.bik_newnum='+quoted(payyer_bik)
    r2=cur.fetchone()
    payyer_accout=regpp[0].find('NOM_LS_PLAT').text
    if len(r2)<>0:
     payyer_name= r2[0]
     payyer_cor=r2[1]
    else:
     payyer_name=u'ОАО "СБЕРБАНК РОССИИ"'
     payyer_cor=payyer_accout
     print "ERR",payyer_cor
    paydoc_ground= report.find('NOM_PP').text
    payyer_bank_name=payyer_name
    payyer_kpp=report.find('KPP_PLAT_PP').text
    #1010201000000
    pr2=(id, payyer_inn, paydoc_date, paydoc_amount, paydoc_number, payyer_accout, payyer_cor, payyer_name, paydoc_ground, payyer_bank_name, '1027700132195', payyer_kpp, payyer_bik, contrtype)
    #print paydoc_date,payyer_accout
    cur.execute(sq1,pr1)
    cur.execute (sq2,pr2)
    con.commit()
    for r in regpp:
     id=getgenerator(cur,'EXT_PPA_LINES')
     line_amount=float( r.find('SUM_REESTR_PP').text)/100
     pr=(r.find('PURPOSE').text).split(';')
     ip_number=(pr[1].lstrip(u'НОМЕР ИП:')).replace (u'\\',u'/') 
     print ip_number
     debtor_name=( r.find('FIO_PLAT').text)
     hsh=hashlib.md5()
     hsh.update(str(id))
     line_externalkey =hsh.hexdigest()
     cur.execute ('select document.doc_number,doc_ip_doc.id_dbtr_born,doc_ip_doc.id_dbtr_inn, doc_ip_doc.id_dbtr_adr from doc_ip_doc join  document on document.id =doc_ip_doc.id   where document.doc_number starting with '+quoted(ip_number))
     r2=cur.fetchone()
     dateborn=None
     address=None
     debtor_inn=None 
     if r2:
      ip_number=r2[0]
      dateborn=r2[1]
      address=r2[3]
      if r2[2]<>None:
       debtor_inn='0123456789'
       #r2[2]
     if (debtor_inn==None):
      if  r.find('INN_PLAT').text=='0':
       debtor_inn='0123456789'
        #011234567890
      else:
       debtor_inn=r.find('INN_PLAT').text
          #ID, LINE_AMOUNT, IP_NUMBER, DEBTOR_NAME, LINE_EXTERNAL_KEY, DEBTOR_INN, DEBTOR_BIRTHDATE,  DEBTOR_OGRN, DEBTOR_ADDRESS, DEBTOR_KPP, EXTERNAL_KEY
     pr3=(id, line_amount, ip_number, debtor_name, line_externalkey, debtor_inn, dateborn, None, address, None, extkey)   
     print extkey,'ERR',pr3
     cur.execute(sq3,pr3)
    con.commit()
    xmlfile.close()
    rename(input_path+ff, input_arc_path+ff)     
 con.close()
if __name__ == "__main__":
    main()
