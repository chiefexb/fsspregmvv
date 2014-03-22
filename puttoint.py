#!/usr/bin/python
#coding: utf8
from regmvv import *
from lxml import etree
from dbfpy import dbf
import fdb
import sys
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
 #print username,password,hostname,concodepage,codepage
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
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filescheme=filepar.findall('scheme')
 #создание root
 try:
  ans_scheme=filescheme[1].getchildren()[0]
 except:
  sys.exit(2)
 #Ищем поля ответа
 #print ans_scheme.tag,ans_scheme.keys()
 #Проверяем явлется ли root контейнером ответов
 if 'answers' in ans_scheme.keys():
  ans=anscheme
 else:
  for ch in ans_scheme.getchildren():
   if ch.text=='reply_date':
    replydatetag=ch.tag
   if 'answers' in ch.keys():
    #print ch.tag
    answer=ch.getchildren()[0]
    answers=ch.tag
    break
 #Ищем поля сведений
 ansnodes=[]
 for ch in answer.getchildren():
  #ans2=[]
  if 'answer' in ch.keys():
   #Заполняем ключи для данных
   ans2={}
   if ch.attrib.values()[0]=='01':
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
   ansnodes.append([ch.tag,ch.attrib.values()[0],ans2])

 #print ansnodes
 #Ищем в значениях тег request_id
 for ch in answer.getchildren():
  print ch.tag,ch.text
  if ch.text=='request_id': 
   reqidtag=ch.tag
  #if ch.text=='reply_date': 
  # replydatetag=ch.tag 
   #print ch.tag
#Соединяемся с базой ОСП
 try:
  con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 cur = con.cursor() 
#Получить список файлов в папке input
 xmlfile=file(intput_path+'rr4.xml')
 xml=etree.parse(xmlfile)
 xmlroot=xml.getroot()
 #print xmlroot.tag
 for ch in xmlroot.getchildren():
  if ch.text=='reply_date':
   replydatetag=ch.tag
 #Ищем контейнер ответов
 xmlanswers=xmlroot.find(answers)
 #Начинаем разбор ответов
 cn=0
 packid=getgenerator(cur,"DX_PACK")
 for a in xmlanswers.getchildren():
 #Проверить запрос с этим id был или нет загружен
  request_id=a.find(reqidtag).text
  print "Req_id",request_id,str(type(request_id))
  ipid=getipid(cur,'UTF-8','CP1251',request_id)
  #request_dt=a.find(replydatetag).text #reply_date      #    "06.12.2013" #???
  replydate=xmlroot.find(replydatetag).text
  if len(getanswertype(ansnodes,a))==0:
   #request_id=a.find(reqidtag).text
   #request_dt="06.12.2013"
   setnegative(cur,con,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,packid) 
  else:
   ans=getanswertype(ansnodes,a)
   #print "ANS",ans
   setpositive(cur,con,'UTF-8','CP1251',agent_code,agreement_code,dept_code,request_id,replydate,ans,a)
   #print ans
   #print ans[0].values()
   #for i in range(len( ans)):
   # ans[i]
  #print len (xmlanswers),cn
  #print "first:"+xmlanswers.getchildren()[0].find(reqidtag).text,xmlanswers.getchildren()[0][3].text
  #print ipid,id
  xmlfile.close()
  f.close()
 con.close()
if __name__ == "__main__":
    main()
