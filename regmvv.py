#!/usr/bin/python
#coding: utf8
const={'er_ext_request_id':0,'er_debtor_inn':1,'er_debtor_kpp':2,'er_req_date':3,'er_pack_date':4,'er_debtor_birthday':5,'er_debtor_ogrn':6,'er_ip_sum':7,'er_processed':8,'er_ip_num':9,'er_req_number':10,'er_mvv_agent_code':11,'er_debtor_document':12,'er_mvv_agreement_code':13,'er_mvv_agent_dept_code':14,'er_pack_number':15,'er_req_id':16,'er_pack_id':17,'er_h_spi':18, 'er_fio_spi':19,'er_osp_number':20,'er_debtor_name':21,'er_debtor_address':22,'er_debtor_birthplace':23,'er_entity_type':24,'er_spi_id':25,'er_ip_id':26,'er_ip_risedate':27,
'eih_id':0,'eih_pack_number':1,'eih_proceed':2,'eih_agent_code':3,'eih_agent_dept_code':4,'eih_agreement_code':5,'eih_external_key':6,'eih_metaobjectname':7,'eih_date_import':8,'eih_source_barcode':9}
ansfields ={'01':['ser_doc','num_doc','date_doc','issue_organ'],'11':['kadastr_n','inv_n_nedv','s_nedv','adres_nedv']}
numstr=('0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','X','Y','Z')
cln=', '
getdivnamesql="select osp.div_fullname_title from osp"
getsbnumsql="select counter from sbcount where sbcount.req_date="
import datetime
import hashlib
#import xml.etree.ElementTree as etree
from lxml import etree
def getdivname (cur):
 cur.execute(getdivnamesql)
 osp=cur.fetchall()
 #divname=osp[0][0]
 return osp[0][0]
def preprocessing(cur,con,systcp,dbcp,sql):
 sql2=('update ext_request set ext_request.processed=2  where  '+str(sql)).decode(systcp).encode(dbcp)
 try:
  cur.execute(sql2)
 except Exception, e:
  print sql2, str(e)
 con.commit()
 return

def getnotprocessed(cur,systcp,dbcp,mvv_agent_code,mvv_agreement_code,mvv_dept_code):
 print str(type(mvv_agent_code))
# if str(type(mvv_agent_code))=="<type 'unicode'>":
#  mvv_agent_code=mvv_agent_code.encode(dbcp)
# else:
#  mvv_agent_code=mvv_agent_code.decode(systcp).encode(dbcp)
#
# if str(type(mvv_agreement_code))=="<type 'unicode'>":
#  mvv_agreement_code=mvv_agreement_code.encode(dbcp)
# else:
#  mvv_agreement_code=mvv_agreement_code.decode(systcp).encode(dbcp)
#
# if str(type(mvv_agent_code))=="<type 'unicode'>":
#  mvv_dept_code=mvv_dept_code.encode(dbcp)
# else:
#  mvv_dept_code=mvv_dept_code.decode(systcp).encode(dbcp)

 sql2="select ext_request.pack_id  from ext_request where mvv_agent_code='" + mvv_agent_code +  "' and mvv_agreement_code='"+ mvv_agreement_code +"' and mvv_agent_dept_code='"+mvv_dept_code+"'  and ext_request.processed = 0 group by pack_id"
 print "SQL=",sql2
 try:
  cur.execute(sql2)
 except Exception ,e:
  print sql2,e
 pack=cur.fetchall() #множества пакетов
 return pack
def getnumfrompacknumber(cur,systcp,dbcp,mvv_agent_code,mvv_agreement_code,mvv_dept_code,packdate,packnum):
 sql2="select ext_request.pack_id  from ext_request where mvv_agent_code='" + mvv_agent_code +  "' and mvv_agreement_code='"+ mvv_agreement_code +"'  and ext_request.pack_date='"+str(packdate.strftime('%d.%m.%y'))+"'  group by pack_id" 
 cur.execute(sql2)
 num=-1
 pp=cur.fetchall()
 #print 'sql2=',sql2,'len=',len(pp),packnum,'num=',num
 if len (pp)==1:
  if pp[0][0]==packnum:
   num=1
 elif len(pp)>1:
  for ii in range (0,len(pp)):
   print packnum,pp[ii][0]
   if pp[ii][0]==packnum:
    print packnum,pp[ii][0],'ii=',ii
    num=ii+1
 #print 'ii=',ii,'num=',num
 return num
def getrecords(cur,packet):
 sql="select * from ext_request where ext_request.pack_id="+str(packet) +" and ext_request.processed = 0"
 cur.execute(sql)
 pack=cur.fetchall()
 return pack
def convtotype(rowdbf,dbvalue,dbcp,dbfcp):
 #Проверяем длину типа N 0
 if rowdbf[1]=='N':
  if rowdbf[3]==0:   #целое проверка длины
   if len(str(dbvalue))>rowdbf[2]:
    val=int(str(dbvalue)[len(str(dbvalue))-rowdbf[2]:len(str(dbvalue))])
  elif str(type(dbvalue))=="<type 'NoneType'>":
   val=0 
  else:
   val=dbvalue
#int( str(dbvalue)[len(str(dbvalue)-rowdbf[2]:len(str(dbvalue)))])
 elif rowdbf[1]=='C':
  if  str(type(dbvalue))=="<type 'datetime.date'>":
   val=str(dbvalue.strftime("%d.%m.%Y"))
  elif str(type(dbvalue))=="<type 'int'>":
   val=str(dbvalue)
  else:
   try:
    val =(dbvalue).encode(dbfcp)
   except:
    #print type(dbvalue)
    val='' 
 elif rowdbf[1]=='D':
  val=(dbvalue).strftime('%Y%m%d')
 return val
#def getfizur()
def dbfaddrecord(rec,dbfscheme,dbscheme,dbvalues,dbsystcp,dbcp,dbfcp):
 fizur=(dbvalues[const['er_entity_type']] in (95,2))
 if fizur:
  fizurnum=1
 else:
  fizurnum=2
 ii=range(0,12)
 #range(0,3)
 #range(0,len(dbfscheme))
 j=0
 for i in ii:
  if str(type(dbscheme[i]))=="<type 'tuple'>":
   j=fizurnum
  else:
   j=0 
  if j==0:
   print str(type(dbscheme[i])),dbfscheme[i][0],dbfscheme[i][1]
#,str(type(dbvalues[dbscheme[i]]))
   if str(type(dbscheme[i]))=="<type 'unicode'>":
    rec[dbfscheme[i][0]]=dbscheme[i].encode(dbfcp)
   elif str(type(dbscheme[i]))=="<type 'str'>" and dbscheme[i]=='fizur':
    rec[dbfscheme[i][0]]=fizurnum
   else:
    rec[dbfscheme[i][0]]=convtotype(dbfscheme[i],dbvalues[dbscheme[i]],dbcp,dbfcp)
  else:
   print str(type(dbscheme[i][j-1])),dbfscheme[i][1]
   if str(type(dbscheme[i][j-1]))=="<type 'unicode'>": 
    rec[dbfscheme[i][0]]=dbscheme[i][j-1].encode(dbfcp)
   elif str(type(dbscheme[i][j-1]))=="<type 'str'>" and dbscheme[i][j-1]=='fizur': 
    rec[dbfscheme[i][0]]=fizurnum
   else:
    rec[dbfscheme[i][0]]=convtotype(dbfscheme[i],dbvalues[dbscheme[i][j-1]],dbcp,dbfcp)
 rec.store()
 return 
def getsbfilename (packdate,num,filial,client):
# rDDMFFFF.NXX
# FFFF=8585 X=61
 m=int((packdate).strftime('%m'))
 d=(packdate).strftime('%d')
 filename='r'+d+numstr[m]+filial+'.'+numstr[num]+client 
 return filename
def getsbnum (con,cur,packdate):
 dd=packdate
 dstr=str((dd).strftime('%d.%m.%y'))
 cur.execute(getsbnumsql+"'"+(dstr)+"'")
 cnt=cur.fetchall()
 if len (cnt)<>0:
  if int(cnt[0][0])>=34:
   print "C>=34"
   dd=datetime.date.today()
   dstr=str(dd.strftime('%d.%m.%y'))
   cur.execute(getsbnumsql+"'"+(dstr)+"'")
   cnt=cur.fetchall()

 if len (cnt)==0:
  print "cnt =0"
  cur.execute("INSERT INTO SBCOUNT (COUNTER, REQ_DATE)  VALUES (0, '"+dstr+"')")
  cur.execute(getsbnumsql+"'"+str(dstr)+"'")
  #cur.execute("COMMIT WORK")
  con.commit();
  cur.execute(getsbnumsql+"'"+(dstr)+"'")
  cnt=cur.fetchall()
 if int(cnt[0][0])<34:
  num=int(cnt[0][0])+1
  cur.execute("UPDATE SBCOUNT set COUNTER="+str(num) +"  WHERE REQ_DATE='"+dstr+"'")
  print "UPDATE SBCOUNT set COUNTER="+str(num) +" WHERE REQ_DATE='"+dstr+"'",num
  con.commit() 
 else:
  num=-1
 cur.execute(getsbnumsql+"'"+str(dstr)+"'")
 cnt=cur.fetchall()
 if num>0:
  num=int (cnt[0][0])
 return num,dd
def setprocessed(cur,con,systcp,dbcp,packet):
 sql2='update ext_request set ext_request.processed=1  where  ext_request.pack_id='+str(packet)
 try:
  cur.execute(sql2.decode(systcp).encode(dbcp))
 except Exception, e:
  print sql2, str(e) 
 con.commit()
 return 
def xmladdrecordold(elname,root,xmlscheme,dbscheme,dbvalues,dbsystcp,dbcp,dbfcp):
 if root.tag==elname:
  zapros=root
 else:
  zapros=etree.SubElement(root,elname)
 #el=etree.SubElement(zapros,xmlscheme[0][0])
 #i=0
 #j=1
 #for i in range(0,3):
 # el=etree.SubElement(zapros,xmlscheme[i][0])
 # dbv=convtotype(xmlscheme[i],dbvalues[dbscheme[i]],dbcp,dbfcp)
 # dbvalue=dbvalues[dbscheme[i]]
 # print  len(str(dbvalue)),str(type(dbvalue))
 # el.text=dbv
 fizur=(dbvalues[const['er_entity_type']] in (95,2))
 if fizur:
  fizurnum=1
 else:
  fizurnum=2
 #ii=range(0,6)
 #range(0,3)
 ii=range(0,len(xmlscheme))
 j=0
 for i in ii:
  if str(type(dbscheme[i]))=="<type 'tuple'>":
   j=fizurnum
  else:
   j=0
  if j==0:
   #print str(type(dbscheme[i])),xmlscheme[i][0],xmlscheme[i][1]
#,str(type(dbvalues[dbscheme[i]]))
   if str(type(dbscheme[i]))=="<type 'unicode'>":
    el=etree.SubElement(zapros,xmlscheme[i][0])
    #el.text=str(dbscheme[i].encode(dbfcp))
    #print el.text
   elif str(type(dbscheme[i]))=="<type 'str'>" :
    if  dbscheme[i]=='fizur':
     el=etree.SubElement(zapros,xmlscheme[i][0])
     el.text=str(fizurnum)
    else:
     el=etree.SubElement(zapros,xmlscheme[i][0])
     el.text=dbscheme[i].decode('UTF-8')
   else:
     el=etree.SubElement(zapros,xmlscheme[i][0])
     el.text=convtotype(xmlscheme[i],dbvalues[dbscheme[i]],dbcp,dbfcp).decode('UTF-8')
     #print convtotype(xmlscheme[i],dbvalues[dbscheme[i]],dbcp,dbfcp)
  else:
   #print str(type(dbscheme[i][j-1])),xmlscheme[i][1]
   if str(type(dbscheme[i][j-1]))=="<type 'unicode'>":
     print "UN"
     el=etree.SubElement(zapros,xmlscheme[i][0])
     el.text=dbscheme[i][j-1].decode(dbsystcp).encode(dbfcp)
   elif str(type(dbscheme[i][j-1]))=="<type 'str'>" and dbscheme[i][j-1]=='fizur':
     el=etree.SubElement(zapros,xmlscheme[i][0])
     el.text=str(fizurnum)
   else:
     el=etree.SubElement(zapros,xmlscheme[i][0])
     el.text=convtotype(xmlscheme[i],dbvalues[dbscheme[i][j-1]],dbcp,dbfcp).decode('UTF-8')
 return root
def strtoconst(str):
 return
def xmladdrecord(elname,root,xmlscheme,dbscheme,dbvalues,dbsystcp,dbcp,dbfcp):
 if root.tag==elname:
  zapros=root
 else:
  zapros=etree.SubElement(root,elname)
 fizur=(dbvalues[const['er_entity_type']] in (95,2))
 if fizur:
  fizurnum=1
 else:
  fizurnum=2
 for i in range(0,len(xmlscheme)):
  if str(type(dbscheme[i]))=="<type 'tuple'>":
   j=fizurnum
  else:
   j=0
  if j==0:
   print dbscheme[i] in const.keys()
   if dbscheme[i] in const.keys():
    el=etree.SubElement(zapros,xmlscheme[i][0])
    el.text=convtotype(xmlscheme[i],dbvalues[const[dbscheme[i]]],dbcp,dbfcp).decode(dbsystcp)
   else:
    st=dbscheme[i].split(';')
    print st[1]
    st2=''
    for k in range(len(st)): 
     if st[k] in const.keys():
      st2=st2+convtotype(['tp','C'],dbvalues[const[st[i]]],dbcp,dbfcp).decode(dbsystcp)
     else:
      st2=st2+st[k]
    el=etree.SubElement(zapros,xmlscheme[i][0])
    el.text=st2#.decode(dbsystcp)
def getanswertype(ansfields,ansnodes):
 #Если данных нет возвращаем []
 #Если есть данные возвращаем список полей которые есть
 ans=[]
 for i in range(len(ansfields)):
  nd=ansnodes.find(ansfields[i][0])
  if str(type(nd)) =="<type 'lxml.etree._Element'>":
   ans.append([nd.tag,ansfields[i][1],ansfields[i][2]])
   #print ans 
 return ans
  #in consts.keys() 
def getipid (cur,systcp,dbcp,req_id):
 sq="select ext_request.ip_id from ext_request where ext_request.req_id='"+req_id+"'"
 try:
  cur.execute(sq)
 except:
  print "err"
 cur.execute(sq) 
 r=cur.fetchall()
 try:
  ipid=r[0][0]
 except:
   ipid=-1
 return ipid
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
def quoted(a):
 st="'"+a+"'"
 return st
def getidnum(cur,dbsystcp,dbcp,ipid):
 sq="select doc_ip.id_no from doc_ip  where doc_ip.id="+str(ipid)
 cur.execute(sq)
 r=cur.fetchall()
 return r[0][0]

def setnegative(cur,con,dbsystcp,dbcp,mvv_agent_code,mvv_agreement_code,mvv_dept_code,req_id,dt):
 meta="EXT_RESPONSE"
 id=getgenerator(cur,"SEQ_DOCUMENT")
 ipid=getipid (cur,dbsystcp,dbcp,req_id)
 packid=getgenerator(cur,"SEQ_DOCUMENT")
 #cur.execute(sq.encode(dbcp))
 #packid=getgenerator(cur,"SEQ_DOCUMENT") 
 sq="INSERT INTO EXT_INPUT_HEADER (ID, PACK_NUMBER, PROCEED, AGENT_CODE, AGENT_DEPT_CODE, AGENT_AGREEMENT_CODE, EXTERNAL_KEY, METAOBJECTNAME, DATE_IMPORT, SOURCE_BARCODE) VALUES ("+str(id)+cln+str(packid)+cln+"0"+cln+ quoted(mvv_agent_code)+cln+ quoted(mvv_dept_code)+cln+quoted(mvv_agreement_code)+cln+str(ipid)+cln+quoted(meta)+cln+quoted(dt)+cln+" NULL)" 
# cur.execute(sq.encode(dbcp))
 #print str(sq)
 cur.execute(("select * from ext_request where req_id="+req_id).decode('CP1251'))
 er=cur.fetchall();
 datastr="Нет сведений"
 idnum=convtotype(['','C'], getidnum(cur,dbsystcp,dbcp,ipid),'UTF-8','UTF-8')
 ent_name=convtotype(['','C'],er[0][const["er_debtor_name"]],'UTF-8','UTF-8')
 #print str(type((ent_name)))
 ent_bdt=convtotype(['','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8')
 ent_by=ent_bdt.split('.')[2]
 ent_inn=convtotype(['','C'],er[0][const["er_debtor_inn"]],'UTF-8','UTF-8')
 req_num=convtotype(['','C'],er[0][const["er_req_number"]],'UTF-8','UTF-8')
 ipnum=convtotype(['','C'],er[0][const["er_ip_num"]],'UTF-8','UTF-8')
 #convtotype(['','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8')
 #print str(type(ent_name))
 
 sq2="INSERT INTO EXT_RESPONSE (ID, RESPONSE_DATE, ENTITY_NAME, ENTITY_BIRTHYEAR, ENTITY_BIRTHDATE, ENTITY_INN, ID_NUM, IP_NUM, REQUEST_NUM, REQUEST_ID, DATA_STR) VALUES ("+str(id)+cln+quoted(dt)+cln+quoted(ent_name)+cln+quoted(ent_by)+cln+quoted(ent_bdt)+cln+quoted(ent_inn)+cln+quoted(idnum)+cln+ quoted(ipnum)+cln+quoted(req_num)+cln+(req_id)+cln+quoted(datastr)+")"
 #sqq=convtotype([' ','C'],sq,'UTF-8','CP1251') 
 #print "SQL1=",sqq
 #sqq2=convtotype([' ','C'],sq2,'UTF-8','CP1251')
# print "SQL1=",(sq2.encode('CP1251'))
# print "SQL2=",(sq.decode('UTF-8').encode('CP1251'))
 cur.execute(sq)
#.decode('UTF-8').encode(dbcp))
 cur.execute(sq2.decode('UTF-8').encode('CP1251'))
#.decode('UTF-8').encode(dbcp))
 con.commit() 
 return
def setpositive(cur,con,dbsystcp,dbcp,mvv_agent_code,mvv_agreement_code,mvv_dept_code,req_id,dt,ans,a):
 print "LEN ANS:",len(ans),"I AM IN"
 for aa in range(len(ans)):
  print ans[aa][1],aa
  if ans[aa][1]=='01':
   id=getgenerator(cur,"SEQ_DOCUMENT")
   ipid=getipid (cur,dbsystcp,dbcp,req_id)
   packid=getgenerator(cur,"DX_PACK")
   hsh=hashlib.md5()
   hsh.update(str(id))
   extkey=hsh.hexdigest()
   print extkey
   cur.execute(("select * from ext_request where req_id="+req_id).decode('CP1251'))
   er=cur.fetchall();
   #datastr="Есть сведения"
   idnum=convtotype([' ','C'], getidnum(cur,dbsystcp,dbcp,ipid),'UTF-8','UTF-8')
   ent_name=convtotype([' ','C'],er[0][const["er_debtor_name"]],'UTF-8','UTF-8')
   #print str(type((ent_name)))
   ent_bdt=convtotype([' ','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8')
   ent_by=ent_bdt.split('.')[2]
   ent_inn=convtotype([' ','C'],er[0][const["er_debtor_inn"]],'UTF-8','UTF-8')
   req_num=convtotype([' ','C'],er[0][const["er_req_number"]],'UTF-8','UTF-8')
   ipnum=convtotype([' ','C'],er[0][const["er_ip_num"]],'UTF-8','UTF-8')
   setresponse(cur,con,dbsystcp,dbcp,mvv_agent_code,mvv_agreement_code,mvv_dept_code,req_id,dt,ans[aa][1],id,packid,extkey,"Есть сведения") 
   id=getgenerator(cur,"EXT_INFORMATION")
   hsh.update(str(id))
   svextkey=hsh.hexdigest()
   sq3="INSERT INTO EXT_INFORMATION (ID, ACT_DATE, KIND_DATA_TYPE, ENTITY_NAME, EXTERNAL_KEY, ENTITY_BIRTHDATE, ENTITY_BIRTHYEAR, PROCEED, DOCUMENT_KEY, ENTITY_INN) VALUES ("+str(id)+cln+quoted(dt)+cln+quoted(ans[aa][1])+cln+quoted(ent_name)+cln+quoted(svextkey)+cln+quoted(ent_bdt)+cln+quoted(ent_by)+cln+quoted('0')+cln+quoted(extkey)+cln+quoted(ent_inn)+")"
   print "SQ3=",sq3,ans[aa][2].keys()
   doc=a.find(ans[aa][0])
   docs={}
   for dd in ans[aa][2].keys():
    docs[dd]=getxmlvalue(dd,ans[aa],doc)
   print "Паспорт номер:",docs['ser_doc']," ",docs['num_doc']," ",docs['date_doc'],docs['issue_organ']
   sq4="INSERT INTO EXT_IDENTIFICATION_DATA (ID, NUM_DOC, DATE_DOC, CODE_DEP, SER_DOC, FIO_DOC, STR_ADDR, ISSUED_DOC) VALUES ("+str(id)+cln+quoted(docs['num_doc'])+cln+quoted(docs['date_doc'])+cln+"NULL"+cln+quoted(docs['ser_doc'])+cln+quoted(ent_name)+cln+"NULL,NULL)"
   print "SQ4=",sq4
   #cur.execute(sq3.decode('UTF-8').encode('CP1251'))
   #cur.execute(sq4.decode('UTF-8').encode('CP1251'))
   #con.commit()

  if ans[aa][1]=='12':
   rights=a.find(ans[aa][0])
   right=rights.findall(ans[aa][2]['right'])
   for rr in right:
    id=getgenerator(cur,"SEQ_DOCUMENT")
    #sq3="INSERT INTO EXT_INFORMATION (ID, ACT_DATE, KIND_DATA_TYPE, ENTITY_NAME, EXTERNAL_KEY,ENTITY_BIRTHDATE, ENTITY_BIRTHYEAR, PROCEED, DOCUMENT_KEY, ENTITY_INN) VALUES  ("+str(id)+cln+quoted(dt)+cln+quoted(ans[aa][1])+cln+quoted(ent_name)+cln+str(ipid)+cln+quoted(ent_bdt)+cln+quoted(ent_by)+cln+quoted('0')+cln+str(rid)+cln+quoted(ent_inn)+")"
    #print sq3
    rightv={}
    for dd in ans[aa][2].keys():
     rightv[dd]=getxmlvalue(dd,ans[aa],rr)
    rightv['floor']=rightv['nfloor'].split('/')[0]
    #print rightv['kadastr_n'],rightv['inv_n_nedv'],rightv['s_nedv'],rightv['nfloor'],rightv['adres_nedv']
    sq4="INSERT INTO EXT_SVED_NEDV_DATA (ID, KADASTR_N, ADRES_NEDV, S_NEDV, FLOOR, LITER_N, INV_N_NEDV, NFLOOR) VALUES ("+str(id)+cln+quoted(rightv['kadastr_n'])+cln+quoted(rightv['adres_nedv'])+cln+rightv['s_nedv']+cln+quoted(rightv['floor'])+cln+"NULL"+cln+quoted(rightv['inv_n_nedv'])+cln+quoted(rightv['nfloor'])+")"
    print sq4
    #cur.execute(sq3.decode('UTF-8').encode('CP1251'))
    #cur.execute(sq4.encode('CP1251'))
    #con.commit()

 #Заполняем датумы
 #cur.execute(sq)
#.decode('UTF-8').encode(dbcp))
 #cur.execute(sq2.decode('UTF-8').encode('CP1251'))
#.decode('UTF-8').encode(dbcp))
 #con.commit() 
 return
def getxmlvalue(name,ans,a):
 #Проверка есть ли длинный путь
 nd=ans[2][name]
 #print len(nd.split(':')),
 ndd=nd.split(":")
 #print ndd,a.tag
 nn=a
 #nn.tag
 for n in ndd:
  nn=nn.find(n)
 #print nn.tag,nn.text
  try:
   val=nn.text
  except:
   val='Null'
 return val
def setresponse(cur,con,dbsystcp,dbcp,mvv_agent_code,mvv_agreement_code,mvv_dept_code,req_id,dt,anst,id,packid,extkey,datastr): 
 meta="EXT_RESPONSE"
 #id=getgenerator(cur,"SEQ_DOCUMENT")
 ipid=getipid (cur,dbsystcp,dbcp,req_id)
 #packid=getgenerator(cur,"DX_PACK")
 #hsh=hashlib.md5()
 #hsh.update(str(id))
 #extkey=hsh.hexdigest()
 sq="INSERT INTO EXT_INPUT_HEADER (ID, PACK_NUMBER, PROCEED, AGENT_CODE, AGENT_DEPT_CODE, AGENT_AGREEMENT_CODE, EXTERNAL_KEY, METAOBJECTNAME, DATE_IMPORT, SOURCE_BARCODE) VALUES ("+str(id)+cln+str(packid)+cln+"0"+cln+ quoted(mvv_agent_code)+cln+ quoted(mvv_dept_code)+cln+quoted(mvv_agreement_code)+cln+extkey+cln+quoted(meta)+cln+quoted(dt)+cln+" NULL)" 
 #print str(sq)
 cur.execute(("select * from ext_request where req_id="+req_id).decode('CP1251'))
 er=cur.fetchall();
 #datastr="Есть сведения"
 idnum=convtotype([' ','C'], getidnum(cur,dbsystcp,dbcp,ipid),'UTF-8','UTF-8')
 ent_name=convtotype([' ','C'],er[0][const["er_debtor_name"]],'UTF-8','UTF-8')
 #print str(type((ent_name)))
 ent_bdt=convtotype([' ','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8')
 ent_by=ent_bdt.split('.')[2]
 ent_inn=convtotype([' ','C'],er[0][const["er_debtor_inn"]],'UTF-8','UTF-8')
 req_num=convtotype([' ','C'],er[0][const["er_req_number"]],'UTF-8','UTF-8')
 ipnum=convtotype([' ','C'],er[0][const["er_ip_num"]],'UTF-8','UTF-8')
 #convtotype(['','C'],er[0][const["er_debtor_birthday"]],'UTF-8','UTF-8')
 #print str(type(ent_name))
 rid=id
 sq2="INSERT INTO EXT_RESPONSE (ID, RESPONSE_DATE, ENTITY_NAME, ENTITY_BIRTHYEAR, ENTITY_BIRTHDATE, ENTITY_INN, ID_NUM, IP_NUM, REQUEST_NUM, REQUEST_ID, DATA_STR,ANSWER_TYPE) VALUES ("+str(id)+cln+quoted(dt)+cln+quoted(ent_name)+cln+quoted(ent_by)+cln+quoted(ent_bdt)+cln+quoted(ent_inn)+cln+quoted(idnum)+cln+ quoted(ipnum)+cln+quoted(req_num)+cln+(req_id)+cln+quoted(datastr)+cln+quoted(anst)+")"
 print "SQL1=",sq
 print "SQL2=",sq2
# cur.execute(sq)
# cur.execute(sq2.decode('UTF-8').encode('CP1251'))
# con.commit()

 return
#def main():
#if __name__ == "__main__":
#    main()
