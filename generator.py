import cx_Oracle
import pandas as pd
from configuration.appConfig import getConfig

# user,pswd,server=getConfig()
remote,local=getConfig()
con=cx_Oracle.connect(remote)
cur=con.cursor()
con2=cx_Oracle.connect(local)
cur2=con2.cursor()
# cur.execute("select 'Hello World' from dual")
# res=cur.fetchall()
# print(res)

dataPullQuery='''select rto.ID,rto.ELEMENTNAME, rto.ELEMENT_ID, ent_master.ENTITY_NAME, rto.ENTITY_ID, gen_unit.installed_capacity,\
 rto.OUTAGE_DATE, rto.OUTAGE_TIME, rto.REVIVED_DATE, rto.REVIVED_TIME, sd_tag.name as shutdown_tag,rto.SHUTDOWN_TAG_ID,\
  sd_type.name as shutdown_typeName,rto.SHUT_DOWN_TYPE, rto.CREATED_DATE, rto.MODIFIED_DATE, rto.OUTAGE_REMARKS, reas.reason,\
   rto.REASON_ID, rto.REVIVAL_REMARKS, rto.REGION_ID, rto.SHUTDOWNREQUEST_ID,rto.IS_DELETED
from real_time_outage rto left join outage_reason reas on reas.id = rto.reason_id
left join shutdown_outage_tag sd_tag on sd_tag.id = rto.shutdown_tag_id
left join shutdown_outage_type sd_type on sd_type.id = rto.shut_down_type
left join entity_master ent_master on ent_master.id = rto.ENTITY_ID
left join generating_unit gen_unit on gen_unit.id = rto.element_id where rownum<5'''
cur.execute(dataPullQuery)
res=cur.fetchall()
for r in res:
	print(r)
cur2.execute('select * from student')
res2=cur2.fetchall()
print(res2)
# cur.execute('DROP TABLE student')
# cur.execute("create table student(sid number,Name varchar2(10), fees number(10, 2), primary key (sid))") 
# print("Table Created successful") 
# cur.execute("insert into student values(1,'rohit', 72000)")

# con.commit() 
if cur:
    cur.close() 
con.close()
if cur2:
    cur2.close() 
con2.close()
