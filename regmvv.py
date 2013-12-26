#!/usr/bin/python
#coding: utf8
const={'er_ext_request_id':0,'er_debtor_inn':1,'er_debtor_kpp':2,'er_req_date':3,'er_pack_date':4,'er_debtor_birthday':5,'er_debtor_ogrn':6,'er_ip_sum':7,'er_processed':8,'er_ip_num':9,'er_req_number':10,'er_mvv_agent_code':11,'er_debtor_document':12,'er_mvv_agreement_code':13,'er_mvv_agent_dept_code':14,'er_pack_number':15,'er_req_id':16,'er_pack_id':17,'er_h_spi':18, 'er_fio_spi':19,'er_osp_number':20,'er_debtor_name':21,'er_debtor_address':22,'er_debtor_birthplace':23,'er_entity_type':24,'er_spi_id':25,'er_ip_id':26,'er_ip_risedate':27,
'eih_id':0,'eih_pack_number':1,'eih_proceed':2,'eih_agent_code':3,'eih_agent_dept_code':4,'eih_agreement_code':5,'eih_external_key':6,'eih_metaobjectname':7,'eih_date_import':8,'eih_source_barcode':9}
numstr=('0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','X','Y','Z')
cln=', '
getdivnamesql="select osp.div_fullname_title from osp"
getsbnumsql="select counter from sbcount where sbcount.req_date="
import datetime
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
 for i in ansfields:
  nd=ansnodes.find(i)
  if str(type(nd)) =="<type 'lxml.etree._Element'>":
   ans.append(nd.tag)  
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
def getentity(cur,dbsystcp,dbcp,ipid):
 return

def setnegative(cur,con,dbsystcp,dbcp,mvv_agent_code,mvv_agreement_code,mvv_dept_code,req_id,dt):
 meta="ext_response"
 id=getgenerator(cur,"SEQ_DOCUMENT")
 ipid=getipid (cur,dbsystcp,dbcp,req_id)
 packid=getgenerator(cur,"SEQ_DOCUMENT")
 #cur.execute(sq.encode(dbcp))
 packid=getgenerator(cur,"SEQ_DOCUMENT") 
 sq="INSERT INTO EXT_INPUT_HEADER (ID, PACK_NUMBER, PROCEED, AGENT_CODE, AGENT_DEPT_CODE, AGENT_AGREEMENT_CODE, EXTERNAL_KEY, METAOBJECTNAME, DATE_IMPORT, SOURCE_BARCODE) VALUES ("+str(id)+cln+str(packid)+cln+"0"+cln+ quoted(mvv_agent_code)+cln+ quoted(mvv_dept_code)+cln+quoted(mvv_agreement_code)+cln+str(ipid)+cln+quoted(meta)+cln+quoted(dt)+cln+" NULL)"
# cur.execute(sq.encode(dbcp))
 print sq
 datastr="Нет сведений"
 idnum='1'
 ent_name="Беланов"
 ent_bdt="01.01.2013"
 ent_by="2013"
 ent_inn="090000000"
 req_num="1/1/1/"
 ipnum="1/2/2/"
 sq="INSERT INTO EXT_RESPONSE (ID, RESPONSE_DATE, ENTITY_NAME, ENTITY_BIRTHYEAR, ENTITY_BIRTHDATE, ENTITY_INN, ID_NUM, IP_NUM, REQUEST_NUM, REQUEST_ID, DATA_STR) VALUES ("+str(id)+cln+quoted(dt)+cln+quoted(ent_name)+cln+quoted(ent_by)+cln+quoted(ent_bdt)+cln+quoted(ent_inn)+cln+idnum+cln+ quoted(ipnum)+cln+quoted(req_num)+cln+req_id+cln+quoted(datastr)
 print sq
# cur.execute(sq.encode(dbcp))
# con.commit() 
 return
#def main():
#if __name__ == "__main__":
#    main()
