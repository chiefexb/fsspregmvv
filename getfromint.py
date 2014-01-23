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
 filetype=filepar.find('type').text
 filenum=filepar.find('numeric').text
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filiescheme=filepar.findall('scheme')[0]
 #создание root
 print filiescheme.getchildren()[0].tag
 root2=filiescheme.getchildren()[0]
 print "X", filetype
 if filetype=='xml':
  #Определение заголовка
  ch=root2.getchildren()	
  reqq=[]
  int2str=[]
  for i in range(len(ch)):
   req=[]
   if ch[i].attrib<>{}:
    if 'records' in ch[i].attrib.keys(): 
     print ch[i].attrib, ch[i].tag
     zapros=ch[i]
    break
   req.append(ch[i].tag)
   req.append('C')
   reqq.append(req)
   int2str.append(ch[i].text)
  print reqq,int2str[0]
  print zapros.tag
  ch=zapros.getchildren()
  reqq2=[]
  int2str2=[]
  for i in range(len(ch)):
   req2=[]
   req2.append(ch[i].tag)
   req2.append('C')
   reqq2.append(req2)
   int2str2.append(ch[i].text)
 elif filetype=='xmlatrib':
  print 'XML',root2.attrib.keys(),root2.attrib.values()
  ch=root2.getchildren()
  reqq=[]
  int2str=[]
  print root2.tag
  #print root.tag
  #Создание заголовка xml
#Соединяемся с базой ОСП
  try:
   con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  cur.execute('select osp.territory,osp.department  from osp')
  rr=cur.fetchall()
  numto=convtotype(['tp','C'],rr[0][0],'UTF-8','UTF-8')
  print 'NUM',numto
  numdepartment=numto+'0'+convtotype(['tp','C'],rr[0][1],'UTF-8','UTF-8')
  root=etree.Element(root2.tag)
  for kk in root2.attrib.keys():
   if root2.attrib[kk]=='tonum':
    root.attrib[kk]=(numto)
   elif root2.attrib[kk]=='departmentnum':
    root.attrib[kk]=(numdepartment)
   elif kk== 'records':
    pass
   else:
    root.attrib[kk]=root2.attrib[kk]
  #departmentnum
   print kk,root2.attrib[kk]
  zapros=root2.getchildren()[0]
  print zapros.attrib.keys(),zapros.attrib.values()
#  zp=etree.SubElement(root,zapros.tag)
#  for kk in zapros.attrib.keys()
#  xml= etree.tostring(root, pretty_print=True, encoding=filecodepage, xml_declaration=True)
#  print xml   



  
 
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
 p=1
 #divname=getdivname(cur)
 #p=3
 for pp in range(0,p):
#  print packets[pp][0]
#  root=etree.Element(root2.tag)
  r=getrecords(cur,packets[pp][0])
  rr=r[0]
  zp=etree.SubElement(root,zapros.tag)
  for kk in zapros.attrib.keys():
   if zapros.attrib[kk] in const:
    print kk,str(type(rr[const[zapros.attrib[kk]]]))
    zp.attrib[kk]=convtotype(['tp','C'],rr[const[zapros.attrib[kk]]],'UTF-8','UTF-8')   
   elif zapros.attrib[kk]=='num':
    zp.attrib[kk]=convtotype(['tp','C'],pp+1,'UTF-8','UTF-8')
   elif zapros.attrib[kk]=='ansdate':
    zp.attrib[kk]=convtotype(['tp','C'],rr['er_req_date']+7,'UTF-8','UTF-8')
   else:
    zp.attrib[kk]=zapros.attrib[kk]
#rr[const[zapros.attrib[kk]]]
  xml= etree.tostring(root, pretty_print=True, encoding=filecodepage, xml_declaration=True)
  print xml
#  xmladdrecord(root.tag,root,reqq,int2str,rr,systemcodepage,codepage,filecodepage)
#  root2=etree.SubElement(root,zapros.tag)
#  
#  xml= etree.tostring(root, pretty_print=True, encoding=filecodepage, xml_declaration=True)
#  num= getnumfrompacknumber(cur,'UTF-8',codepage,agent_code,agreement_code,dept_code,rr[const['er_pack_date']],rr[const['er_pack_id']])
#  filename='rr_'+str(rr[const['er_osp_number']])+'_'+str(rr[const['er_pack_date']].strftime('%d_%m_%y'))+'_'+str(num)+'.xml'
#  print filename,num
#  f2=open(output_path+filename,'w')
#  f2.write(xml)
#  f2.close()

#  print "LEN="+str(len(r))
#  print xml
# f.close()
 con.close()
if __name__ == "__main__":
    main()
