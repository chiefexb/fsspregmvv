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
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()
         
    def __exit__(self, type, value, traceback):
        print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".format(time.time() - self._startTime)
        st="Elapsed time:"+str(time.time() - self._startTime) # {:.3f} sec".format(time.time() - self._startTime)
        logging.info(st)
def main():
#Обработка параметров
 #print len (sys.argv)
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
 try:
  port=dbparams.find('port').text
 except:
  print 'default port 3050'
  port='3050'

 #print username,password,hostname,concodepage,codepage
#Ищем параметры МВВ
 mvv=cfgroot.find('mvv')
 agent_code=mvv.find('agent_code').text
 dept_code=mvv.find('dept_code').text
 agreement_code=mvv.find('agreement_code').text
 #f.close()
 logpar=cfgroot.find('logging')
 log_path=logpar.find('log_path').text
 log_file=logpar.find('log_file').text
#Определяем тип и путь файла
 filepar=cfgroot.find('file')
 filecodepage=filepar.find('codepage').text
 output_path=filepar.find('output_path').text
 input_path=filepar.find('input_path').text
 input_arc_path=filepar.find('input_path_arc').text
 input_err_path=filepar.find('input_path_err').text
 filetype=filepar.find('type').text
 filenum=filepar.find('numeric').text
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filescheme=filepar.findall('scheme')
 #создание root
 if str( type (filepar.find('bank') ) )<>"<type 'NoneType'>":
  filebank=filepar.find('bank')
  bankname=filebank.find('name').text
  bankbik=filebank.find('bik').text
 else:
  bankname=''
  bankbik=''
 if str( type (filepar.find('result') ) )<>"<type 'NoneType'>":
  print 'RES'
  fileresult=filepar.find('result')
  positiveresult=fileresult.find('positiveresult').text ##Проверить ПФР
  negativeresult=fileresult.find('negativeresult').text
  resultattrib='AnswerType'
 else:
  fileresult=''
 try:
  ans_scheme=filescheme[1].getchildren()[0]
 except:
  sys.exit(2)
 #Ищем поля ответа
 logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s', level = logging.DEBUG, filename = log_path+log_file)
 #logging.info( u'This is an info message' )
 if filetype=='xml':
  replydatefrom=-1
  #print ans_scheme.tag,ans_scheme.keys()
  #Проверяем явлется ли root контейнером ответов
  #print  ans_scheme.tag, ans_scheme.attrib.keys()
  if 'answers' in ans_scheme.keys():
   answer=ans_scheme
   answers=ans_scheme.tag #ans_scheme.tag
   print 'ANSWERS',answers,'Answers.tag ', answer.tag#answer.tag,answer.getchildren()
   #Ищем er_req_id
   
  else:
   for ch in ans_scheme.getchildren():
    #print ch.tag
    if ch.text=='reply_date':
     replydatetag=ch.tag
     replydatefrom=1
    if 'answers' in ch.keys():
     #print ch.tag
     answer=ch.getchildren()[0]
     answers=ch.tag
     break
  #Ищем поля сведений
   print 'ANSWERS',answers,'Answers.tag ',answer.tag#,answer.getchildren()
  ansnodes=[]
  print answer.getchildren()
  req_id_in=0
  for ch in answer.getchildren(): #ограничение на кол-во ответов debug
   #ans2=[]
   
   #print 'answer',ch.keys
   # if 'req_id_in' in ch.keys():
   if 'req_id_in' in ch.keys():
    for chh in ch:
     if chh.text=='er_req_id':
      req_id_in=1
      reqidtag=chh.tag
      print chh.tag
     if chh.text=='reply_date':
      replydatetag=chh.tag
   if 'answer' in ch.keys():
    #Заполняем ключи для данных
    ans2={}
    if ch.attrib.values()[0]=='01': #Там где есть параметры это не приемлимо
     for chh in ch:
      for af in ansfields['01']:
       if chh.text==af:
        ans2[af]=chh.tag
    if ch.attrib.values()[0]=='11':
     chh=ch.getchildren()[0]
     #print "!!BUG",chh.tag
     ans2['right']=chh.tag #!!!
     for chh2 in chh:
      for af in ansfields['11']:
       #print "MM",chh2.text,chh2.tag
       if chh2.text==af:
        ans2[af]=chh2.tag #!bug
      if 'childrens' in chh2.attrib.keys():
       print "ADDR"
       for chh3 in chh2:
        for af in ansfields['11']:
         if chh3.text==af:
          #print af,ans2	
          ans2[af]=chh2.tag+':'+chh3.tag
    if ch.attrib.values()[0]=='09':
     for chh in ch:
      for af in ansfields['09']:
       if chh.text==af:
        ans2[af]=chh.tag
    ansnodes.append([ch.tag,ch.attrib.values()[0],ans2])
  #print 'NODES',ansnodes
  print ansnodes
  #Ищем в значениях тег request_id
  for ch in answer.getchildren():
   print 'er_req_id',ch.tag,ch.text
   if ch.text=='er_req_id': 
    reqidtag=ch.tag
   if ch.text=='reply_date': 
    replydatetag=ch.tag 
    replydatefrom=2
    #print ch.tag
  print reqidtag
  #Соединяемся с базой ОСП
  try:
   con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage,port=port)
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor() 
 #Получить список файлов в папке input
  print input_path
  st=u'Начало процесса загрузки, файлов для обработки:'+str( len( listdir(input_path) ))
  logging.info( st )
  for ff in listdir(input_path):
   xmlfile=file(input_path+ff) #'rr4.xml')
   xml=etree.parse(xmlfile)
   xmlroot=xml.getroot()
   #print xmlroot.tag
   #for ch in xmlroot.getchildren(): ###Зачем
   # if ch.text=='reply_date':
   #  replydatetag=ch.tag
   #  replydatefrom=2
   #Ищем контейнер ответов
   #print 'ANSWER',answers,xmlroot.tag
   if xmlroot.tag==answers:
    xmlanswers=xmlroot
   else:
    xmlanswers=xmlroot.find(answers)
   print 'xmlanswers',xmlanswers.tag
  #Начинаем разбор ответов
   cn=0
   packid=getgenerator(cur,"DX_PACK")
   sqlbuff=[]
   sqltemp=''
   print xmlanswers
   with Profiler() as p:
    for a in xmlanswers.getchildren():#[11:20]: #!Ограничение
     #Проверить запрос с этим id был или нет загружен
     print "REQ",reqidtag,a.tag
     print a.tag
     request_id=a.find(reqidtag).text
     #print "Req_id",request_id,str(type(request_id))
     ipid=getipid(cur,'UTF-8','CP1251',request_id)
     #request_dt=a.find(replydatetag).text #reply_date      #    "06.12.2013" #???
     #print str(xmlroot.getchildren())
     if replydatefrom==1:
      replydate=xmlroot.find(replydatetag).text
     elif replydatefrom==2:
      replydate=a.find(replydatetag).text
     else:
      replydate=datetime.now().strftime('%d.%m.%y')  
   
     if len(getanswertype(ansnodes,a))==0 and ipid<>-1:
      #request_id=a.find(reqidtag).text
      #request_dt="06.12.2013"
      #print timeit.Timer("""
      #with Profiler() as p:
      sqltemp= setnegative(cur,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,packid) 
      for sqt in sqltemp:
       sqlbuff.append(sqt)
      #""").repeat(1)
     elif ipid<>0:
      ans=getanswertype(ansnodes,a)
      #print "ANS",ans
      #print timeit.Timer("""
      sqltemp=setpositive(cur,con,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,ans,a,packid)
      for sqt in sqltemp:
       sqlbuff.append(sqt)
 
     #""").repeat(1)
     #print ans
     #print ans[0].values()
     #for i in range(len( ans)):
     # ans[i]
    #print len (xmlanswers),cn
    #print "first:"+xmlanswers.getchildren()[0].find(reqidtag).text,xmlanswers.getchildren()[0][3].text
    #print ipid,id
   #print len(sqlbuff)
   #con.commit()
   st=u'Выгружаем буфер sql запросов:'+str(len(sqlbuff))
   logging.info( st )
   with Profiler() as p:
    for sqt in sqlbuff:
     try:
      cur.execute(sqt)
     except:
      print sqt
    con.commit()
   xmlfile.close()
   if ipid <>-1:
    rename(input_path+ff, input_arc_path+ff)
    pass
   else:
    st=u'Загрузка файла '+ff+u' невозможна, запрос с id='+str(request_id)+u'отстутствует в базе'
    logging.error( st ) #logging.error
    rename(input_path+ff, input_err_path+ff)
  f.close()
  con.close()
 if filetype=='xmlatrib':
  print ans_scheme.keys()
  answer=ans_scheme.getchildren()[0]  
  print answer.keys()
  
  #Ищем поля сведений
  ansnodes=[]
  for ch in answer.getchildren():
   if 'answers' in ans_scheme.keys():
    answer=ans_scheme.getchildren()[0]
    answers=ans_scheme.tag
   else:
    for ch in ans_scheme.getchildren():
    #if ch.text=='reply_date':
    # replydatetag=ch.tag
     if 'answers' in ch.keys():
      #print ch.tag
      answer=ch.getchildren()[0].attrib.keys()
      answers=ch.tag
      break
  print answers,answer.getchildren()[1].tag
  ans2={}
  answermatrix={}
  ch=answer.getchildren()[0]
  answermatrixatr=ch.attrib['answermatrix']
  for ch in answer:
   print 'X',ch.tag
   for chh in ch.attrib.keys():
    if  'answermatrix' ==chh:
     print ch.attrib['answermatrix'] 
     am=ch.attrib[ch.attrib['answermatrix']]
     at=ch.attrib['answer'] 
     answermatrix[am]=at
    print answermatrix
    
  print resultattrib #,str(answer.attrib[resultattrib]==positiveresult)
  #к параметров
  #разбор контейнеров
  xmlfile=file(intput_path+ ff)# 'PFR_20140507_12_09002_008_000_00010.xml') #'rr4.xml')
  xml=etree.parse(xmlfile)
  xmlroot=xml.getroot()
  print xmlroot.tag
  #Ищем контейнер ответов
  if(xmlroot.tag==answers):
   xmlanswers=xmlroot
  else:
   xmlanswers=xmlroot.find(answers)
  print answers
  print len(  xmlanswers.getchildren())
  for a in  xmlanswers.getchildren():
   print a.attrib[resultattrib]
   print answermatrix,a.attrib.keys()
   if (a.attrib[resultattrib]==positiveresult):
    aa=a.getchildren()[0]
    print answermatrix[ aa.attrib[answermatrixatr]]
    aaa=aa.getchildren()[0]
    print aaa.tag,aaa.attrib.keys()
 if filetype=='pfr':
  if  len(listdir(input_path))==0:
   inform(u'Нет файлов для загрузки')
  try:
   con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage,port=port)
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  sqlbuff=[]
  inform(u'Начало процесса загрузки, файлов для обработки:'+str( len( listdir(input_path) )) )
  for ff in listdir(input_path):
   fff=ff.split('_')
   try:
    d=date(int (fff[1][0:4]),int (fff[1][4:6]), fff[1][6:8] ) 
   except:
    d=date.today()
   #Если формат файла верный тогда верно и ниже
   replydate=d.strftime('%d.%m.%Y')
   xmlfile=file(input_path+ff )#'PFR_20140507_12_09002_008_000_00010.xml') #'rr4.xml')
   #Логирование и учет
   inform(u'Загружаем: '+ff.decode('UTF-8'))
   with Profiler() as p:
    sqlbuff=[]
    xml=etree.parse(xmlfile)
    xmlroot=xml.getroot()
    #Ищем контейнер ответов
    answers='ExtAnswer'
    print xmlroot.tag
    if(xmlroot.tag==answers):#Безполезное условие
     xmlanswers=xmlroot
    else:
     xmlanswers=xmlroot.findall(answers)
    cn=0
    sqltemp=''
    inform(u"Проверка файл соответствует ли  структуре")
    if len(xmlanswers)<>0:
     #Если присутвует контейнеров ответов, проверяем ipid
     try:
      request_id=xmlanswers[0].attrib['IPKey']
     except:
      ipid=-1
     else:
      ipid=getipid(cur,'UTF-8','CP1251',request_id)
     if ipid==-1:
      informerr(u"Ошибка в структуре файла, файл будет помещен в папку для ошибочных")
      rename(input_path+ff, input_err_path+ff)
     else:#Основной блок программы если стопудово файл подходит
      packid=getgenerator(cur,"DX_PACK")
      for a in  xmlanswers:
       # #Проверить запрос с этим id был или нет загружен
       request_id=a.attrib['IPKey']
       #request_dt=a.find(replydatetag).text #reply_date      #    "06.12.2013" #???
       # #request_dt можно найти из запроса
       ipid=getipid(cur,'UTF-8','CP1251',request_id)
       if a.attrib['AnswerType']=='1' and ipid<>-1:
        #Разбор сведений
        for aa in a:
         ipid=getipid(cur,'UTF-8','CP1251',request_id)
         if aa.attrib['KindData']=='93':
          id=getgenerator(cur, "SEQ_DOCUMENT") #"SEQ_EXT_INPUT_HEADER") #Надо_SEQ_EXT
          #cur.execute('select max(id) from ext_input_header ')
          #er=cur.fetchall()
          #max=er[0][0]
          #print max,id
          #if id<max:
          # cur.execute('ALTER SEQUENCE SEQ_EXT_INPUT_HEADER RESTART WITH '+str(max))
          # con.commit()
          # informwarn(u'Сбит генератор SEQ_EXT_INPUT_HEADER. Исправляю значение.'+str(max)+','+str(id))
          # id=getgenerator(cur,"SEQ_EXT_INPUT_HEADER")
          hsh=hashlib.md5()
          hsh.update(str(id))
          extkey=hsh.hexdigest()
          sqltemp=setresponse(cur,con,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,'01',id,packid,extkey,"Сведения о пенсии.")
          for sqt in sqltemp:
           sqlbuff.append(sqt)
          #Вставка сведений о статусе
          aaa=aa.getchildren()[0]
          #print aaa.tag, aaa.attrib['State']
          debstate= aaa.attrib['State']
          cur.execute(("select * from ext_request where req_id="+request_id).decode('CP1251'))
          er=cur.fetchall()
          idnum=convtotype([' ','C'], getidnum(cur,'UTF-8','CP1251',ipid),'UTF-8','UTF-8')
          ent_name=convtotype([' ','C'],er[0][const["er_debtor_name"]],'UTF-8','UTF-8')
          ent_bdt=convtotype([' ','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8')
          ent_by=ent_bdt.split('.')[2]
          ent_inn=convtotype([' ','C'],er[0][const["er_debtor_inn"]],'UTF-8','UTF-8')
          req_num=convtotype([' ','C'],er[0][const["er_req_number"]],'UTF-8','UTF-8')
          ipnum=convtotype([' ','C'],er[0][const["er_ip_num"]],'UTF-8','UTF-8')
          id=getgenerator(cur,"EXT_INFORMATION")
          cur.execute('select max(id) from EXT_INFORMATION ')
          er=cur.fetchall()
          max=er[0][0]
          #print max,id
          if id<max:
           cur.execute('ALTER SEQUENCE EXT_INFORMATION RESTART WITH '+str(max))
           con.commit()
           informwarn(u'Сбит генератор EXT_INFORMATION. Исправляю значение.'+str(max)+','+str(id))
           id=getgenerator(cur,"EXT_INFORMATION")
   
          hsh.update(str(id))
          svextkey=hsh.hexdigest()
          sq3="INSERT INTO EXT_INFORMATION (ID, ACT_DATE, KIND_DATA_TYPE, ENTITY_NAME, EXTERNAL_KEY, ENTITY_BIRTHDATE, ENTITY_BIRTHYEAR, PROCEED, DOCUMENT_KEY, ENTITY_INN) VALUES ("+str(id)+cln+quoted(replydate)+cln+quoted('08')+cln+quoted(ent_name)+cln+quoted(svextkey)+cln+quoted(ent_bdt)+cln+quoted(ent_by)+cln+quoted('0')+cln+quoted(extkey)+cln+quoted(ent_inn)+")"       #print sqltemp
          sq4="INSERT INTO EXT_DEBTOR_STATE_DATA (ID, STATE) VALUES ("+str(id)+cln+quoted(debstate)+");"
          sqlbuff.append(sq3)
          sqlbuff.append(sq4) 
         if aa.attrib['KindData']=='81':
          aaa=aa.getchildren()[0]
          tt=len (aaa.attrib.keys())<>0
          if tt:
           tt='NaimOrg' in aaa.attrib.keys()
          if tt:
           tt='AdresJ' in	aaa.attrib.keys()
          if tt:
    	   tt='AdresF' in  aaa.attrib.keys()
          else:
           #print ff,request_id,aaa.attrib.keys()
           informwarn(u'Неполные сведения о работе, данные проигнорированы:'+ff.decode('UTF-8')+' '+request_id)
          if tt:
           id=getgenerator(cur,"SEQ_DOCUMENT") #"SEQ_EXT_INPUT_HEADER")
           #cur.execute('select max(id) from ext_input_header ')
           #er=cur.fetchall()
           #max=er[0][0]
           #if id<max:
           # cur.execute('ALTER SEQUENCE SEQ_EXT_INPUT_HEADER RESTART WITH '+str(max))
           # con.commit()
           # informwarn(u'Сбит генератор SEQ_EXT_INPUT_HEADER. Исправляю значение.'+str(max)+','+str(id))
           # id=getgenerator(cur,"SEQ_EXT_INPUT_HEADER") 
           hsh=hashlib.md5()
           hsh.update(str(id))
           extkey=hsh.hexdigest()
           sqltemp=setresponse(cur,con,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,'01',id,packid,extkey,"Есть сведения о работе.")
           for sqt in sqltemp:
            sqlbuff.append(sqt)
          #Вставка сведений о статусе
           naimorg=aaa.attrib['NaimOrg']
           adresj=aaa.attrib['AdresJ']
           adresf=aaa.attrib['AdresF']
           cur.execute(("select * from ext_request where req_id="+request_id).decode('CP1251'))
           er=cur.fetchall()
           idnum=convtotype([' ','C'], getidnum(cur,'UTF-8','CP1251',ipid),'UTF-8','UTF-8')
           ent_name=convtotype([' ','C'],er[0][const["er_debtor_name"]],'UTF-8','UTF-8')
           ent_bdt=convtotype([' ','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8')
           ent_by=ent_bdt.split('.')[2]
           ent_inn=convtotype([' ','C'],er[0][const["er_debtor_inn"]],'UTF-8','UTF-8')
           req_num=convtotype([' ','C'],er[0][const["er_req_number"]],'UTF-8','UTF-8')
           ipnum=convtotype([' ','C'],er[0][const["er_ip_num"]],'UTF-8','UTF-8')
           id=getgenerator(cur,"EXT_INFORMATION")
           cur.execute('select max(id) from EXT_INFORMATION ')
           er=cur.fetchall()
           max=er[0][0]
           #print max,id
           if id<max:
            cur.execute('ALTER SEQUENCE EXT_INFORMATION RESTART WITH '+str(max))
            con.commit()
            informwarn(u'Сбит генератор EXT_INFORMATION. Исправляю значение.'+str(max)+','+str(id))
            id=getgenerator(cur,"EXT_INFORMATION")
           hsh.update(str(id))
           svextkey=hsh.hexdigest()
           sq3="INSERT INTO EXT_INFORMATION (ID, ACT_DATE, KIND_DATA_TYPE, ENTITY_NAME, EXTERNAL_KEY, ENTITY_BIRTHDATE, ENTITY_BIRTHYEAR, PROCEED, DOCUMENT_KEY, ENTITY_INN) VALUES ("+str(id)+cln+quoted(replydate)+cln+quoted('56')+cln+quoted(ent_name)+cln+quoted(svextkey)+cln+quoted(ent_bdt)+cln+quoted(ent_by)+cln+quoted('0')+cln+quoted(extkey)+cln+quoted(ent_inn)+")"       #print sqltemp
           #      #Сведения о работодтеле
           sq4="INSERT INTO EXT_SVED_RAB_DATA (ID, ADRES_F, ADRES_J, NAIMORG, INN, KPP) VALUES ("+str(id)+cln+quoted(adresf)+cln+quoted(adresj)+cln+quoted(naimorg)+", NULL, NULL);"
           sqlbuff.append(sq3)
           sqlbuff.append(sq4) 
          #else:
          # st=u'В файле '+ff+u' в запросе с id='+str(request_id)+u'неверные сведения.'
          # logging.warning( st ) #logging.error
          # sqltemp= setnegative(cur,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,packid)
          # for sqt in sqltemp:
          #  sqlbuff.append(sqt)
       elif ipid<>-1:
        #id=getgenerator(cur,"SEQ_EXT_INPUT_HEADER")
        #cur.execute('select max(id) from ext_input_header ')
        #er=cur.fetchall()
        #max=er[0][0]
        #if id<max:
        # cur.execute('ALTER SEQUENCE SEQ_EXT_INPUT_HEADER RESTART WITH '+str(max))
        # con.commit()
        # #informwarn(u'Сбит генератор SEQ_DOCUMENT. Исправляю значение.'+str(max)+','+str(id))
        #id=getgenerator(cur,"SEQ_EXT_INPUT_HEADER")
        sqltemp= setnegative(cur,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,packid)
        for sqt in sqltemp:
         sqlbuff.append(sqt)
      xmlfile.close()
      rename(input_path+ff, input_arc_path+ff)
      #Конец основного блока, чтобы не путаться
    else: #Если отстутствуе блок ответа:
     xmlfile.close()
     informerr (u"В файле "+ff+u"Отстутсвует блок ответа, файл будет помещен в папку для ошибочных.")
     rename(input_path+ff, input_err_path+ff)
   
   st=u'Выгружаем буфер sql запросов:'+str(len(sqlbuff))
   logging.info( st )
   with Profiler() as p:
    for sqt in sqlbuff:
     try:
      cur.execute(sqt)
     except Exception,e:
      informerr (u'Ошибка скрипта: '+str(e)+u' '+sqt)
    con.commit()
  f.close()
  con.close() 
 if filetype=='dbf': #Тип dbf определяет только банки для других целей будут dbf2 и т д
#Первое мы должны сохранить схему файла
   ans_scheme=filescheme[1]
   flds={}   #Поля участвующие
   answer={} #Формат ответа
   reqstart=''
   for ch in ans_scheme.getchildren():
    print ch.tag,  len(str (ch.text))
    if ch.text=='reply_date':
     flds[ch.text]=ch.tag
    if ch.text=='request_id':
     if 'start' in ch.attrib.keys():
      reqstart=ch.attrib['start'] #Начало от id
      #print reqstart
     flds[ch.text]=ch.tag
    if ch.text=='result':
     flds[ch.text]=ch.tag
    if 'result' in ch.attrib.keys():#Атрибут result определяет тип ответа 
     #Проверяем есть ли разделитель
     if 'separator' in ch.attrib.keys(): #Должна быть ветки если нет
      ans=ch.text.split(ch.attrib['separator'])
     answer[ch.attrib['result']]=ans
     answer['separator']=ch.attrib['separator']
     flds['resulttext']=ch.tag
     if 'blob_field' in ch.attrib.keys():#Проверка есть ли блоб поле
      flds['blob_field']=ch.attrib['blob_field']
 #request_id
   #print flds
   #print answer
   #print positiveresult
   #Соединяемся с базой ОСП
   try:
    con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage,port=port)
   except  Exception, e:
    print("Ошибка при открытии базы данных:\n"+str(e))
    sys.exit(2)
   cur = con.cursor()
   if  len(listdir(input_path))==0:
    inform(u'Нет файлов для загрузки')
   else:
    inform(u'Начало процесса загрузки, файлов для обработки:'+str( len( listdir(input_path) )) )
    for ff in listdir(input_path):
     #ff='orshb_9112_18.09.2013_33.dbf' #'orshb0912_01_02_14_1.dbf'
     #packid=getgenerator(cur,"DX_PACK")
     try:
      db=dbf.Dbf(input_path+ff)
     except:
      ipid=-1
      print "FILE " +ff
      request_id=u'Ошибка открытия файла'
     else:
      #Проверяем ipid
      print ff,flds['request_id'],reqstart,db[0]
      try:
       request_id=(reqstart+str (db[0][flds['request_id']]))
      except:
       ipid=-1
       
       request_id=u'Ошибка открытия файла'
      else:
       ipid=getipid(cur,'UTF-8','CP1251',request_id)
     if ipid<>-1:
      inform(u'Начало процесса загрузки, файла '+ff.decode('UTF-8')+u'. Файл содержит ' +str(len(db))+u' Записей.')
      with Profiler() as p:
       packid=getgenerator(cur,"DX_PACK")
       #print "Запроса с id=",request_id," не найдено!!!"
       #print db
       sqlbuff=[]
       for j in range (0,len(db)):
        #sqlbuff=[]
        sqltemp=''
        aa={} #Ответ без расшифровки
        #На этом этапе нужно добавить загрузку блоб поля
        #print 'KEYS',flds.keys()
        for kk in flds.keys():
         #print 'KK=',kk
         if str(type (db[j][flds[kk]])) =="<type 'str'>":
          aa[kk]=db[j][flds[kk]].decode(filecodepage) #Тут должна быть кодирока файла из настроек
         elif kk=='request_id':
          aa[kk]=(reqstart+str (db[j][flds[kk]])) #восстанавливаем укороченный request_id
         elif  str(type (db[j][flds[kk]])) == "<type 'datetime.date'>":
          aa[kk]=datetime.strftime(db[j][flds[kk]],'%d.%m.%Y')
         else:
          aa[kk]= str(db[j][flds[kk]])
          #print "ELSE", kk,aa
         if db[j][flds['resulttext']]=='' and db[j][flds['blob_field']]<>'':
          try:
           bl= file(input_path+db[j][flds['blob_field']])
          except:
           aa['result']='-1'
          else:
           bll=bl.readlines()
           t=bll[0]
           t=t.replace(chr(13),'')
           t=t.replace(chr(10),'')
           aa['resulttext']=t
           #print t
          #else:
          # aa[kk]= str(db[j][flds[kk]])
          # print "ELSE", kk,aa
    
        #for a in aa.keys():
        #нужна функция вход aa и answer
        #print aa['resulttext']
        #print aa.keys(),'REZ',aa['resulttext']
        #print 'result is:',aa['result'],aa['result']==positiveresult,str(type(positiveresult)),positiveresult,str(type(aa['result']))
        #print aa['reply_date']
        #Если поле надо парсить
        #print 'Result',aa['result']
        replydate=aa['reply_date']
        request_id=aa['request_id']
        ipid=getipid(cur,'UTF-8','CP1251',request_id)
        if aa['result']==positiveresult  and ipid<>-1:
         id=getgenerator(cur,"SEQ_DOCUMENT")
         ipid=getipid (cur,systemcodepage,codepage,request_id)
         #print "IPID",ipid,request_id
         #packid=getgenerator(cur,"DX_PACK")
         hsh=hashlib.md5()
         hsh.update(str(id))
         extkey=hsh.hexdigest()
         #print extkey,ipid
         sqltemp=setresponse(cur,con,systemcodepage,codepage,agent_code,agreement_code,dept_code,request_id,replydate,'01',id,packid,extkey,"Есть сведения")
         for sqt in sqltemp:
          #print sqt
          sqlbuff.append(sqt)
          #sqltemp.append(sqt)
         rs=aa['resulttext'].split(answer['separator'])
         sc=answer['09'] #Работает только банки
         accdata=[]
         ac={}
         #print 'sc=',len(sc),'rs=',len(rs),len(rs)/len(sc)
         for k in range(0,len(rs)/len(sc)):
          ac={}
          for i in range (0,len(sc)):
           #print 'i=',i,'k=',k,len(sc)*(k)+i
           if sc[i]<>'ignore':
            ac[sc[i]]=rs[len(sc)*(k)+i]
          #print ac
          accdata.append(ac)
        #end if 
         for acd in accdata:
          #print acd
          cur.execute(("select * from ext_request where req_id="+request_id).decode('CP1251'))
          er=cur.fetchall()
          #print "Поиск", len(er)
          #print len(er)
          #datastr="Есть сведения"
          idnum=convtotype([' ','C'], getidnum(cur,systemcodepage,codepage,ipid),'UTF-8','UTF-8')
          ent_name=convtotype([' ','C'],er[0][const["er_debtor_name"]],'UTF-8','UTF-8')
          #print str(type((ent_name)))
          ent_bdt=(convtotype([' ','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8'))
          #print "ERR", len(ent_bdt),ent_bdt
          if len(ent_bdt)<>0:
           ent_by=quoted(ent_bdt.split('.')[2])
           ent_bdt=quoted(ent_bdt)
          else:
           ent_by='Null'
           ent_bdt='Null'
          ent_inn=convtotype([' ','C'],er[0][const["er_debtor_inn"]],'UTF-8','UTF-8')
          req_num=convtotype([' ','C'],er[0][const["er_req_number"]],'UTF-8','UTF-8')
          ipnum=convtotype([' ','C'],er[0][const["er_ip_num"]],'UTF-8','UTF-8')
          id=getgenerator(cur,"EXT_INFORMATION")
          hsh.update(str(id))
          svextkey=hsh.hexdigest()
          sq3="INSERT INTO EXT_INFORMATION (ID, ACT_DATE, KIND_DATA_TYPE, ENTITY_NAME, EXTERNAL_KEY, ENTITY_BIRTHDATE, ENTITY_BIRTHYEAR, PROCEED, DOCUMENT_KEY, ENTITY_INN) VALUES ("+str(id)+cln+quoted(replydate)+cln+quoted('09')+cln+quoted(ent_name)+cln+quoted(svextkey)+cln+(ent_bdt)+cln+(ent_by)+cln+quoted('0')+cln+quoted(extkey)+cln+quoted(ent_inn)+")"
          datastr='Счета есть'#'Есть сведения'
          currcode=currency_type[str(acd['curr'])]
          sq4="INSERT INTO EXT_AVAILABILITY_ACC_DATA (ID, BIC_BANK, CURRENCY_CODE, ACC, BANK_NAME, SUMMA, DEPT_CODE, SUMMA_INFO) VALUES ("+str(id)+cln+quoted(bankbik)+cln+ quoted(currcode)+cln+ quoted(acd['acc'])+cln+quoted(bankname)+cln+(acd['summa'])+", NULL, NULL)"
          #print sq3
          #print sq4
          sqlbuff.append(sq3)
          sqlbuff.append(sq4)
         #Обработка счетов
        elif aa['result']==negativeresult and ipid<>-1:
         #print ipid,request_id
         sqltemp= setnegative(cur,systemcodepage,codepage,agent_code,agreement_code,dept_code,request_id,replydate,packid)
         for sq in sqltemp:
          #print sq
          sqlbuff.append(sq)
        else:
         if ipid==-1:
          informerr(u'В теле ответа присутвует ошибочный ответ, файл:'+ff+u' request_id:'+request_id+u' Номер записи: '+str(j) )
         else:
          informerr(u'Ошибка типа ХЗ. Возможно из-за того что результат равен '+ aa['result']+ff+u' request_id:'+request_id+u'Номер записи: '+str(j) )
      inform(u'Загружаем запросы:'+str(len(sqlbuff)) )
      with Profiler() as p:
       for sq in sqlbuff:
        cur.execute(sq)
      inform(u'Меряем время коммита' )
      with Profiler() as p:
       con.commit()
      rename(input_path+ff, input_arc_path+ff)
     else:#Else Если ipid=-1
      informerr(u"Запроса с id="+(request_id)+u" не найдено!!! Файл "+ff.decode('UTF-8')+  u" перемещен в ошибочные")
      rename(input_path+ff, input_err_path+ff)
   con.close()
   f.close() 
if __name__ == "__main__":
    main()
