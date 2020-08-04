# %%
import cx_Oracle
import pandas as pd
import datetime as dt
import math
import numpy as np
from configuration.appConfig import getConfig
# %%
# user,pswd,server=getConfig()
remote, local = getConfig()
con = cx_Oracle.connect(remote)
cur = con.cursor()

outagesFetchQuery = '''select rto.ID as pwc_id, rto.ELEMENT_ID,rto.ELEMENTNAME,
rto.ENTITY_ID, ent_master.ENTITY_NAME, gen_unit.installed_capacity, rto.OUTAGE_DATE, 
rto.OUTAGE_TIME, rto.REVIVED_DATE, rto.REVIVED_TIME,rto.CREATED_DATE, 
rto.MODIFIED_DATE, sd_tag.name as shutdown_tag,rto.SHUTDOWN_TAG_ID, 
sd_type.name as shutdown_typeName,rto.SHUT_DOWN_TYPE,  rto.OUTAGE_REMARKS, 
reas.reason,rto.REASON_ID, rto.REVIVAL_REMARKS, rto.REGION_ID, rto.SHUTDOWNREQUEST_ID,rto.IS_DELETED
from real_time_outage rto left join outage_reason reas on reas.id = rto.reason_id
left join shutdown_outage_tag sd_tag on sd_tag.id = rto.shutdown_tag_id
left join shutdown_outage_type sd_type on sd_type.id = rto.shut_down_type
left join entity_master ent_master on ent_master.id = rto.ENTITY_ID
left join generating_unit gen_unit on gen_unit.id = rto.element_id where rownum<4'''

cur.execute(outagesFetchQuery)
res = cur.fetchall()

# %%
# ['ID','ELEMENT_ID','ELEMENT_NAME','ENTITY_ID','ENTITY_NAME','INSTALLED_CAPACITY','OUTAGE_DATETIME','REVIVED_DATETIME','CREATED_DATETIME',
# 'MODIFIED_DATETIME','SHUTDOWN_TAG','SHUTDOWN_TAG_ID','SHUTDOWN_TYPENAME','SHUT_DOWN_TYPE_ID','OUTAGE_REMARKS','REASON_ID','REASON',
# 'REVIVAL_REMARKS','REGION_ID','SHUTDOWNREQUEST_ID','IS_DELETED','PWC_ID']

df = pd.DataFrame(res, columns=['PWC_ID', 'ELEMENT_ID', 'ELEMENT_NAME', 'ENTITY_ID', 'ENTITY_NAME', 
                                'INSTALLED_CAPACITY', 'OUTAGE_DATETIME', 'OUTAGE_TIME', 'REVIVED_DATETIME', 
                                'REVIVED_TIME', 'CREATED_DATETIME', 'MODIFIED_DATETIME', 'SHUTDOWN_TAG', 'SHUTDOWN_TAG_ID', 
                                'SHUTDOWN_TYPENAME', 'SHUT_DOWN_TYPE_ID', 'OUTAGE_REMARKS', 'REASON', 'REASON_ID', 
                                'REVIVAL_REMARKS', 'REGION_ID', 'SHUTDOWNREQUEST_ID', 'IS_DELETED'])

# temporary id arrangement till auto increment feature is implemented
df['ID'] = df['PWC_ID']

def getTimeDeltaFromDbStr(timeStr):
    if not(':' in timeStr):
        return dt.timedelta(seconds=0)
    else:
        timeSegs = timeStr.split(':')
        timeSegs = timeSegs[0:2]
        return dt.timedelta(hours=int(timeSegs[0]), minutes=int(timeSegs[1]))

df['REVIVED_TIME'] = [getTimeDeltaFromDbStr(i) for i in df['REVIVED_TIME']]

df['REVIVED_DATETIME'] = pd.to_datetime(df['REVIVED_DATETIME'].dt.date)+(df['REVIVED_TIME'])
# print(df['REVIVED_DATE'])

df['OUTAGE_TIME'] = [getTimeDeltaFromDbStr(i) for i in df['OUTAGE_TIME']]
df['OUTAGE_DATETIME'] = pd.to_datetime(df['OUTAGE_DATETIME'].dt.date)+(df['OUTAGE_TIME'])

df = df.drop(['OUTAGE_TIME', 'REVIVED_TIME'], axis=1)


# %%
# convert datetime columns to string
# for colName in ['REVIVED_DATETIME', 'OUTAGE_DATETIME', 'CREATED_DATETIME', 'MODIFIED_DATETIME']:
#     df[colName] = df[colName].dt.strftime('TIMESTAMP %Y-%m-%d %H:%M:%S')

# insertion batch size
rowBatchSize = min(100, len(df))

# get a cursor object from the connection
conLocal=cx_Oracle.connect(local)
curLocal=conLocal.cursor()

sqlColsText = ','.join(df.columns.tolist())
try:
    for batchStartItr in range(0, len(df), rowBatchSize):
        # derive the endIterator of the batch
        # also avoid end index overflow by comparing with data array length
        batchEndItr = min(len(df)-1, batchStartItr+rowBatchSize-1)
        tupDataTexts = []
        dataTups = []
        pwcIds = []
        for tupItr in range(batchStartItr, batchEndItr+1):
            dataTup = df.iloc[tupItr, :].tolist()
            for itr in range(len(dataTup)):
                if 'numpy' in str(type(dataTup[itr])):
                    dataTup[itr] = dataTup[itr].item()
            dataTups.append(tuple(dataTup))
            pwcIds.append((df.iloc[tupItr,:]['PWC_ID'].item(),))
        curLocal.executemany("delete from generating_unit where PWC_ID=:1", pwcIds)
        sqlTxt = "insert into generating_unit ({0}) values ({1})".format(sqlColsText, ','.join([':'+str(i) for i in range(1,len(dataTup)+1)]))
        print(sqlTxt)
        print(dataTups)
        curLocal.executemany(sqlTxt, dataTups)

except Exception as e:
    print('Error while bulk insert into db')
    print(e)
finally:
    # closing database cursor and connection
    if curLocal is not None:
        curLocal.close()
    conLocal.close()


# %%
