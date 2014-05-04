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
 #print sys.argv[1]
#Открытие файла конфигурации
 try:
  f=file(sys.argv[1])
 except Exception,e:
  #print e
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
 pre=mvv.find('preprocessing')
 try:
  con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
 except  Exception, e:
  #print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 cur = con.cursor()
 #print str(type(pre)),str(type(pre))<>"<type 'NoneType'>"
 if str(type(pre))<>"<type 'NoneType'>":
  ch=pre.findall('sql')
  #print ch[0].tag,ch[0].text
  for chh in ch:
   #print "CH",chh.text,chh.tag
   sq=chh.text.encode('UTF-8')
   #print sq,str(type(sq))
   preprocessing(cur,con,'UTF-8','CP1251',sq)
 ##f.close()
 con.close()
#Определяем тип и путь файла
 filepar=cfgroot.find('file')
 #print filepar
 filecodepage=filepar.find('codepage').text
 output_path=filepar.find('output_path').text
 filetype=filepar.find('type').text
 filenum=filepar.find('numeric').text
 fileprefix=filepar.find('prefix').text
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filiescheme=filepar.findall('scheme')[0]
 #создание root
 #print filiescheme.getchildren()[0].tag
 root2=filiescheme.getchildren()[0]
 #print "X", filetype
 cfgroot.find('file')
 if filetype=='xml':
  #Определение заголовка
  ch=root2.getchildren()	
  reqq=[]
  int2str=[]
  for i in range(len(ch)):
   req=[]
   if ch[i].attrib<>{}:
    if 'records' in ch[i].attrib.keys(): 
     #print ch[i].attrib, ch[i].tag
     zapros=ch[i]
    break
   req.append(ch[i].tag)
   req.append('C')
   reqq.append(req)
   int2str.append(ch[i].text)
  #print reqq,int2str[0]
  #print zapros.tag
  ch=zapros.getchildren()[0]
  reqq2=[]
  int2str2=[]
  for i in range(len(ch)):
   req2=[]
   req2.append(ch[i].tag)
   #print ch[i].tag
   req2.append('C')
   reqq2.append(req2)
   int2str2.append(ch[i].text)
  #print reqq2,int2str2
 #Соединяемся с базой ОСП
  try:
   con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
  except  Exception, e:
   #print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  root=etree.Element(root2.tag)
  packets=getnotprocessed(cur,systemcodepage,'CP1251',mvv_agent_code=agent_code,mvv_agreement_code=agreement_code,mvv_dept_code=dept_code)
  p=len(packets)
  for pp in range(0,p):
   root=etree.Element(root2.tag)
   r=getrecords(cur,packets[pp][0])
   #print "PP",pp,packets[pp][0],"LEN R",len(r)
   rr=r[0]
   xmladdrecord(root.tag,root,reqq,int2str,rr,systemcodepage,codepage,filecodepage)
   #xml= etree.tostring(root, pretty_print=True, encoding=filecodepage, xml_declaration=True)
   #print xml
   zp=etree.SubElement(root,zapros.tag)
   zpp=zapros.getchildren()[0]
   for rr in r:
   #rr=r[0]
    #print "ZP",zp.tag,'INT',int2str2,zpp.tag
    xmladdrecord(zpp.tag,zp,reqq2,int2str2,rr,systemcodepage,codepage,filecodepage)
   xml= etree.tostring(root, pretty_print=True, encoding=filecodepage, xml_declaration=True)
   #print xml
   num= getnumfrompacknumber(cur,'UTF-8',codepage,agent_code,agreement_code,dept_code,rr[const['er_pack_date']],rr[const['er_pack_id']])
   filename=fileprefix+str(rr[const['er_osp_number']])+'_'+str(rr[const['er_pack_date']].strftime('%d_%m_%y'))+'_'+str(num)+'.xml'
   #print filename,num
   f2=open(output_path+filename,'w')
   f2.write(xml)
   f2.close()
  #setprocessed(cur,con,'UTF-8',codepage,packets[pp][0])

 elif filetype=='xmlatrib':
  #print 'XML',root2.attrib.keys(),root2.attrib.values()
  ch=root2.getchildren()
  reqq=[]
  int2str=[]
  #print root2.tag
  #Создание заголовка xml
#Соединяемся с базой ОСП
  try:
   con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
  except  Exception, e:
   #print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  root=etree.Element(root2.tag)
  rr=[]
  delta=datetime.timedelta(days=7)
  zapros=root2.getchildren()[0]
  print zapros.attrib.keys(),zapros.attrib.values()
#Предварительная обработка 
#Определяем список необработанных пакетов
  packets=getnotprocessed(cur,systemcodepage,'CP1251',mvv_agent_code=agent_code,mvv_agreement_code=agreement_code,mvv_dept_code=dept_code)
  #print len(packets)
  #print str(type(agent_code)),str(type('Росреестр'))
  p=len(packets)
 #p=1
 #divname=getdivname(cur)
 #p=3
 #r=getrecords(cur,packets[0][0]) #!!!
 #rr=r[0]
 #root=setattribs(cur,'UTF-8','UTF-8',root,root2,rr,delta,1) 
 
  for pp in range(0,p):
   root=etree.Element(root2.tag)
   r=getrecords(cur,packets[pp][0])
   print "PP",pp,packets[pp][0],"LEN R",len(r)
   rr=r[0]
   root=setattribs(cur,'UTF-8','UTF-8',root,root2,rr,delta,1)
   rr=[]
   for ri in range(len(r)):
    rr=r[ri]
   #print "LEN R",len(r)
    zp=etree.SubElement(root,zapros.tag)
    delta=datetime.timedelta(days=7)
    zp=setattribs(cur,'UTF-8','UTF-8',zp,zapros,rr,delta,ri+1)
    for ch in zapros.getchildren():
     sbch=etree.SubElement(zp,ch.tag)
     sbch=setattribs(cur,'UTF-8','UTF-8',sbch,ch,rr,delta,ri+1)
   xml= etree.tostring(root, pretty_print=True, encoding=filecodepage, xml_declaration=True)
   r=[]
  #print xml
#  xmladdrecord(root.tag,root,reqq,int2str,rr,systemcodepage,codepage,filecodepage)
#  root2=etree.SubElement(root,zapros.tag)
#  
#  xml= etree.tostring(root, pretty_print=True, encoding=filecodepage, xml_declaration=True)
   num= getnumfrompacknumber(cur,'UTF-8',codepage,agent_code,agreement_code,dept_code,rr[const['er_pack_date']],rr[const['er_pack_id']])
   filename=fileprefix+str(rr[const['er_osp_number']])+'_'+str(rr[const['er_pack_date']].strftime('%d_%m_%y'))+'_'+str(num)+'.xml'
#  print filename,num
   f2=open(output_path+filename,'w')
   f2.write(xml)
   f2.close()
   setprocessed(cur,con,'UTF-8',codepage,packets[pp][0])
 elif filetype=='dbf':
  print 'FS', filiescheme.tag
  print 'root', root2.tag 
  ch=filiescheme.getchildren()
 #Соединяемся с базой ОСП
  try:
   con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
  except  Exception, e:
   #print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  #divname=getdivname(cur)
  reqdbfscheme=[]
  int2dbfscheme=[]
  for chh in ch:
   #Анализ аттрибутов
   spp=[]
   spp2=[]
   spp.append(chh.tag.replace(' ',''))
   spp.append(chh.attrib['field_type'])
   spp.append(int(chh.attrib['field_size']))
   if 'field_dec' in chh.attrib.keys():
    spp.append(int(chh.attrib['field_dec']))
   #Анализ текта
   tt=chh.text
   tt=tt.replace(' ','')
   print tt,  (',' in tt)
   if ',' in tt:
    print 'YEAH'
    spp2.append(tt.split(','))
    print spp2
   else:
    spp2.append(tt)
   #print chh.tag
   reqdbfscheme.append(spp) 
   int2dbfscheme.append(spp2)
  print reqdbfscheme
  print int2dbfscheme[10][0]
  print str(type(int2dbfscheme[2][0]))
  db = dbf.Dbf("/home/chief/dbfile.dbf", new=True)
  db.addField(*reqdbfscheme)
  packets=getnotprocessed(cur,systemcodepage,'CP1251',mvv_agent_code=agent_code,mvv_agreement_code=agreement_code,mvv_dept_code=dept_code)
  p=len(packets)
  pp=packets[0][0]
  print pp
  r=getrecords(cur,pp)
  rec = db.newRecord()
  rr=r[0]
  print reqdbfscheme,int2dbfscheme  
  dbfaddrecord(rec,reqdbfscheme,int2dbfscheme,rr,'UTF-8',codepage,filecodepage)
#reqdbfscheme
#int2dbfscheme
#def getdivname (cur):
  db.close()

#sch
#  print "LEN="+str(len(r))
#  print xml
# f.close()
 con.close()
 
   
if __name__ == "__main__":
    main()
