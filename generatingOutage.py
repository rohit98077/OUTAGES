import cx_Oracle
import pandas as pd
import datetime as dt
import math
import numpy as np
from configuration.appConfig import getConfig
def time_formatter(string):
	if string:
		if '.' in string:
			string=string.replace('.','')
		lst=string.split(':')
		length=len(lst)
		if length>1 and lst[1]=='':
			lst[1]='00'
			string=":".join(lst)
		if length==3:
			return string
		elif length==2:
			return string+':00'
		elif length==1:
			return string+':00:00'
	else:
		return string

try:
	remote,local=getConfig()
	con=cx_Oracle.connect(remote)
	cur=con.cursor()
	con2=cx_Oracle.connect(local)
	cur2=con2.cursor()
	print(con.version,con.version)
	# cur.execute("select 'Hello World' from dual")
	# res=cur.fetchall()
	# print(res)

	outagesFetchQuery='''select rto.ID, rto.ELEMENT_ID,rto.ELEMENTNAME,rto.ENTITY_ID,ent_master.ENTITY_NAME,gen_unit.installed_capacity,rto.OUTAGE_DATE,
	 rto.OUTAGE_TIME, rto.REVIVED_DATE, rto.REVIVED_TIME,rto.CREATED_DATE,rto.MODIFIED_DATE, sd_tag.name as shutdown_tag,rto.SHUTDOWN_TAG_ID,
	 sd_type.name as shutdown_typeName,rto.SHUT_DOWN_TYPE,rto.OUTAGE_REMARKS, reas.reason,rto.REASON_ID, rto.REVIVAL_REMARKS, rto.REGION_ID,
	rto.SHUTDOWNREQUEST_ID,rto.IS_DELETEDfrom real_time_outage rto left join outage_reason reas on reas.id = rto.reason_id
	left join shutdown_outage_tag sd_tag on sd_tag.id = rto.shutdown_tag_id
	left join shutdown_outage_type sd_type on sd_type.id = rto.shut_down_type
	left join entity_master ent_master on ent_master.id = rto.ENTITY_ID
	left join generating_unit gen_unit on gen_unit.id = rto.element_id where outage_date between TO_DATE(:1,'DD-MM-YYYY') and TO_DATE(:2,'DD-MM-YYYY')'''

	cur.execute(outagesFetchQuery,('01-07-2019','05-07-2019'))
	res=cur.fetchall()
	print(type(res),type(res[0]),len(res))
	# for r in res[:5]:
		# print(r)

	df=pd.DataFrame(res,columns=['ID','ELEMENT_ID','ELEMENTNAME','ENTITY_ID','ENTITY_NAME','INSTALLED_CAPACITY','OUTAGE_DATETIME','OUTAGE_TIME',
		'REVIVED_DATETIME','REVIVED_TIME','CREATED_DATETIME','MODIFIED_DATETIME','SHUTDOWN_TAG','SHUTDOWN_TAG_ID','SHUTDOWN_TYPENAME','SHUT_DOWN_TYPE_ID',
		'OUTAGE_REMARKS','REASON','REASON_ID','REVIVAL_REMARKS','REGION_ID','SHUTDOWNREQUEST_ID','IS_DELETED'])
	df['PWC_ID']=df['ID']
	# df.to_excel('Generator2.xlsx',index=False,engine='xlsxwriter')
	# print(df.info())
	# df['OUTAGE_DATE']=pd.to_datetime(df['OUTAGE_DATE'].dt.date)+pd.to_timedelta(df['OUTAGE_TIME'])
	# df['REVIVED_DATE']=pd.to_datetime(df['REVIVED_DATE'].dt.date)+pd.to_timedelta(df['REVIVED_TIME'])
	# print(df['OUTAGE_DATE'])

	pd.set_option("display.max_rows",None,'display.max_columns',None)
	# print(df['REVIVED_TIME'])
	# df['REVIVED_TIME']=(pd.to_timedelta(df['REVIVED_TIME']+':00'))
	# print(df['REVIVED_TIME'])


	df['REVIVED_TIME']=pd.to_timedelta([time_formatter(i) for i in df['REVIVED_TIME'] ])
	df['REVIVED_DATETIME']=pd.to_datetime(df['REVIVED_DATETIME'].dt.date)+(df['REVIVED_TIME'])
	# print(df['REVIVED_DATE'])

	# df['OUTAGE_TIME']=pd.to_timedelta([i+':00' if len(i.split(':'))==2 else i for i in df['OUTAGE_TIME'] ])
	df['OUTAGE_TIME']=pd.to_timedelta([time_formatter(i) for i in df['OUTAGE_TIME']])
	df['OUTAGE_DATETIME']=pd.to_datetime(df['OUTAGE_DATETIME'].dt.date)+(df['OUTAGE_TIME'])
	# print(df['OUTAGE_DATE'])

	df=df.drop(['OUTAGE_TIME','REVIVED_TIME'],axis=1)
	# for i in df['INSTALLED_CAPACITY']:
	# 	print(np.isnan(i))
	# df['INSTALLED_CAPACITY']=[0 if np.isnan(i) else i for i in df['INSTALLED_CAPACITY']]
	df['INSTALLED_CAPACITY'].fillna(0,inplace=True)
	# print(df['INSTALLED_CAPACITY'])
	# print(df.info())
	df['SHUTDOWN_TAG_ID'].fillna(0,inplace=True)
	# print(df['SHUTDOWN_TAG_ID'])
	# print(type(df.values),type(df.values[0]))
	# arr=df.to_records(index=False)
	# print(arr)
	# print(type(arr),type(arr[0]),type(arr[0][6]))
	# for r in arr:
		# print(r,type(r[6]))
	# print(type(arr),type(arr[0]))
	# print(df['OUTAGE_TIME'])
	# tmp=pd.to_datetime(df['OUTAGE_DATE'].dt.date)+df['OUTAGE_TIME']
	# print(tmp)

	rows=[tuple(r) for r in df.values]
	for r in rows:
		print(r)
	# length,batchSize=len(rows),1000
	# quotient,reminder=length//batchSize,length%batchSize
	# start,end=0,batchSize
	# for i in range(quotient):
	# 	cur2.executemany('''insert into GEN_UNIT (ID,ELEMENT_ID,ELEMENT_NAME,ENTITY_ID,ENTITY_NAME,INSTALLED_CAPACITY,
	# 	OUTAGE_DATETIME,REVIVED_DATETIME,CREATED_DATETIME,MODIFIED_DATETIME,SHUTDOWN_TAG,SHUTDOWN_TAG_ID,SHUTDOWN_TYPENAME,SHUT_DOWN_TYPE_ID,
	# 	OUTAGE_REMARKS,REASON,REASON_ID,REVIVAL_REMARKS,REGION_ID,SHUTDOWNREQUEST_ID,IS_DELETED,PWC_ID) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22)''',rows[start:end])
	# 	con2.commit() 
	# 	start+=batchSize
	# 	end+=batchSize
	# end=start+reminder
	# cur2.executemany('''insert into GEN_UNIT (ID,ELEMENT_ID,ELEMENT_NAME,ENTITY_ID,ENTITY_NAME,INSTALLED_CAPACITY,
	# 	OUTAGE_DATETIME,REVIVED_DATETIME,CREATED_DATETIME,MODIFIED_DATETIME,SHUTDOWN_TAG,SHUTDOWN_TAG_ID,SHUTDOWN_TYPENAME,SHUT_DOWN_TYPE_ID,
	# 	OUTAGE_REMARKS,REASON,REASON_ID,REVIVAL_REMARKS,REGION_ID,SHUTDOWNREQUEST_ID,IS_DELETED,PWC_ID) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22)''',rows[start:end])
	# con2.commit() 
except Exception as e:
	print(e)
finally:
	if cur:
	    cur.close() 
	con.close()
	if cur2:
	    cur2.close() 
	con2.close()


# %%
# insertion bacth size
# rowBatchSize = 100

# consider this as the data insertion rows, this might even have 1000 rows

# get a cursor object from the connection
# cur = conn.cursor()

# try:
#     for batchStartItr in range(0, len(dataInsertionTuples), rowBatchSize):
#         # deive the endIterator of the batch
#         # also avoid end index overflow by comparing with data array length
#         batchEndItr = min(len(dataInsertionTuples)-1, batchStartItr+rowBatchSize-1)
#         '''
# 		'PWC_ID','ELEMENT_ID','ELEMENTNAME','ENTITY_ID','ENTITY_NAME','INSTALLED_CAPACITY','OUTAGE_DATE',
# 		'OUTAGE_TIME','REVIVED_DATE','REVIVED_TIME','CREATED_DATE','MODIFIED_DATE','SHUTDOWN_TAG',
# 		'SHUTDOWN_TAG_ID','SHUTDOWN_TYPENAME','SHUT_DOWN_TYPE','OUTAGE_REMARKS','REASON','REASON_ID',
# 		'REVIVAL_REMARKS','REGION_ID','SHUTDOWNREQUEST_ID','IS_DELETED'
#         '''
#         for tupItr in range(batchStartItr, batchEndItr+1):
#         	dataTup = df.iloc[tupItr,:]
#         	primaryId = dataTup.loc['PWC_ID']
#         	pwc_id = dataTup.loc['PWC_ID']
#         	elementId = dataTup.loc['ELEMENT_ID']
#         	elementName = dataTup.loc['ELEMENTNAME']
#         	entityId = dataTup.loc['ENTITY_ID']
#         	entityName = dataTup.loc['ENTITY_NAME']
#         	installedCapacity = dataTup.loc['INSTALLED_CAPACITY']
#         	outageTimestamp = dataTup.loc['OUTAGE_DATE']
#         	revivalTimestamp = dataTup.loc['REVIVED_DATE']
#         	createdTimestamp = dataTup.loc['CREATED_DATE']
#         	modifiedTimestamp = dataTup.loc['MODIFIED_DATE']
#         	modifiedTimestamp = dataTup.loc['MODIFIED_DATE']




        # dataTextInSql = ','.join(['(\'{0}\',\'{1}\',{2},\'{3}\',{4})'.format(dataTup[0], dataTup[1], dataTup[2],dataTup[3], dataTup[4]) for dataTup in dataInsertionTuples[batchStartItr: batchEndItr+1]])

        # create sql for insertion
        # you can also use UPSERT for updating existing data on conflict
        # sqlTxt = 'INSERT INTO public.schedules(sch_utility, sch_date, sch_block, sch_type, sch_val) VALUES {0}'.format(dataTextInSql)

        # execute the sql to perform insertion
        # cur.execute(sqlTxt)
# except:
    # print('Error while bulk insert into db')
# finally:
    # closing database cursor and connection
    # if cur is not None:
        # cur.close()
    # conn.close()
