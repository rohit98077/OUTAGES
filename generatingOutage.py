import cx_Oracle
import pandas as pd
import datetime as dt
import numpy as np
from config.appConfig import getConfig

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
		return '00:00:00'

def timeDeltaFromStr(string):
	if pd.isnull(string):
		return dt.timedelta()
	if '.' in string:
		string=string.replace('.','')

	if ':' not in string:
		return dt.timedelta()
	else:
		lst=string.split(':')
		if lst[1]=='':
			lst[1]='0'
		return dt.timedelta(hours=int(lst[0]),minutes=int(lst[1]))

def extractVoltFromName(elemName):
	try:
		if ' AND ' in elemName:
			name=elemName.split('AND')
			req=name[0]
			if '/' in req:
				req=name[1]
			ind=req.index('KV ')
			elemVoltLvl=req[ind-3:ind+2]
		elif 'ICT' in elemName:
			ind=elemName.index('KV ')
			elemVoltLvl=elemName[:ind+2]
		else:
			if 'KV ' in elemName:
				ind=elemName.index('KV')
			elif 'KV-' in elemName:
				ind=elemName.index('KV-')

			kvStartInd=ind-1
			while kvStartInd > 0:
				if elemName[kvStartInd-1] not in ['1','2','3','4','5','6','7','8','9','0','.']:
					break
				kvStartInd += -1
			elemVoltLvl=elemName[kvStartInd:ind+2]
	except:
		elemVoltLvl = 'NA'
	return elemVoltLvl

def getOwnersForAcTransLineCktIds(reportsConnStr, ids):
    l=len(ids)
    if l == 0:
        return {}
    # requiredIds in tuple list form
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or ckt.id in '.join(lst)

    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''select ckt.id as ckt_id,
                    owner_details.owners
                from REPORTING_WEB_UI_UAT.ac_transmission_line_circuit ckt
                    left join REPORTING_WEB_UI_UAT.ac_trans_line_master ac_line on ckt.line_id = ac_line.id
                    left join (
                        select LISTAGG(own.owner_name, ',') WITHIN GROUP (
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id as element_id
                        from REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            left join REPORTING_WEB_UI_UAT.owner own on own.id = ent_reln.child_entity_attribute_id
                        where ent_reln.CHILD_ENTITY = 'OWNER'
                            and ent_reln.parent_entity = 'AC_TRANSMISSION_LINE'
                            and ent_reln.CHILD_ENTITY_ATTRIBUTE = 'OwnerId'
                            and ent_reln.PARENT_ENTITY_ATTRIBUTE = 'Owner'
                        group by parent_entity_attribute_id
                    ) owner_details on owner_details.element_id = ac_line.id where ckt.id in {0}'''.format(reqIdsTxt)

    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForBayIds(reportsConnStr, ids):
    l=len(ids)
    if l == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or bay.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT bay.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.bay bay
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'BAY'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = bay.id where bay.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForBusIds(reportsConnStr, ids):
    l=len(ids)
    if l == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or bus.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT bus.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.bus bus
                    left join REPORTING_WEB_UI_UAT.associate_substation subs on subs.id = bus.fk_substation_id
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'ASSOCIATE_SUBSTATION'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = subs.id where bus.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForBusReactorIds(reportsConnStr, ids):
    l=len(ids)
    if l== 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or bus_reactor.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT bus_reactor.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.bus_reactor bus_reactor
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'BUS_REACTOR'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = bus_reactor.id where bus_reactor.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForCompensatorIds(reportsConnStr, ids):
    l=len(ids)
    if l == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or tcsc.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT tcsc.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.tcsc tcsc
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity IN ('STATCOM','TCSC','MSR','MSC')
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = tcsc.id where tcsc.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForFscIds(reportsConnStr, ids):
    l=len(ids)
    if len(ids) == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or fsc.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT fsc.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.fsc fsc
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'FSC'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = fsc.id where fsc.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict= {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForGenUnitIds(reportsConnStr, ids):
    l=len(ids)
    if len(ids) == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or gen_unit.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT gen_unit.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.generating_unit gen_unit
                    LEFT JOIN REPORTING_WEB_UI_UAT.generating_station gen_stn ON gen_stn.id = gen_unit.fk_generating_station
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'Owner'
                            AND ent_reln.parent_entity = 'GENERATING_STATION'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = gen_stn.id where gen_unit.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForHvdcLineCktIds(reportsConnStr, ids):
    l=len(ids)
    if len(ids) == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or hvdc_ckt.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT hvdc_ckt.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.hvdc_line_circuit hvdc_ckt
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'HVDC_LINE'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = hvdc_ckt.id where hvdc_ckt.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForHvdcPoleIds(reportsConnStr, ids):
    l=len(ids)
    if len(ids) == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or hvdc_pole.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT hvdc_pole.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.hvdc_pole hvdc_pole
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'HVDC_POLE'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = hvdc_pole.id where hvdc_pole.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict


def getOwnersForLineReactorIds(reportsConnStr, ids):
    l=len(ids)
    if len(ids) == 0:
        return {}
    q,r,batchSize=0,0,1000
    if l<=1000:
    	reqIdsTxt = ','.join([str(x) for x in ids])
    	reqIdsTxt='('+reqIdsTxt+')'
    else:
    	start,end,lst=0,batchSize,[]
    	q,r=l//batchSize,l%batchSize    	
    	for i in range(q):
    		txt = ','.join([str(x) for x in ids[start:end] ])
    		txt='('+txt+')'
    		lst.append(txt)
    		start+=batchSize
    		end+=batchSize
    	end=start+r
    	txt = ','.join([str(x) for x in ids[start:end] ])
    	txt='('+txt+')'
    	lst.append(txt)
    	reqIdsTxt=' or line_reactor.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
    fetchSql = '''SELECT line_reactor.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.line_reactor line_reactor
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'LINE_REACTOR'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = line_reactor.id where line_reactor.id in {0}'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForTransformerIds(reportsConnStr, ids):
	l=len(ids)
	if len(ids) == 0:
		return {}
	q,r,batchSize=0,0,1000
	if l<=1000:
		reqIdsTxt = ','.join([str(x) for x in ids])
		reqIdsTxt='('+reqIdsTxt+')'
	else:
		start,end,lst=0,batchSize,[]
		q,r=l//batchSize,l%batchSize    	
		for i in range(q):
			txt = ','.join([str(x) for x in ids[start:end] ])
			txt='('+txt+')'
			lst.append(txt)
			start+=batchSize
			end+=batchSize
		end=start+r
		txt = ','.join([str(x) for x in ids[start:end] ])
		txt='('+txt+')'
		lst.append(txt)
		reqIdsTxt=' or transformer.id in '.join(lst)
    # connect to reports database
    # con = cx_Oracle.connect(reportsConnStr)
	fetchSql = '''SELECT transformer.id,
                    owner_details.owners
                FROM REPORTING_WEB_UI_UAT.transformer transformer
                    LEFT JOIN (
                        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                                ORDER BY owner_name
                            ) AS owners,
                            parent_entity_attribute_id AS element_id
                        FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                            LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                        WHERE ent_reln.child_entity = 'OWNER'
                            AND ent_reln.parent_entity = 'TRANSFORMER'
                            AND ent_reln.child_entity_attribute = 'OwnerId'
                            AND ent_reln.parent_entity_attribute = 'Owner'
                        GROUP BY parent_entity_attribute_id
                    ) owner_details ON owner_details.element_id = transformer.id where transformer.id in {0}'''.format(reqIdsTxt)

	cur = con.cursor()
	cur.execute(fetchSql, [])
	dbRows = cur.fetchall()
	ownersDict: Dict[int, str] = {}
	for row in dbRows:
		ownersDict[row[0]] = row[1]
	return ownersDict


try:
	appConfig=getConfig()
	remote,local=appConfig['remoteConStr'],appConfig['localConStr']
	con=cx_Oracle.connect(remote)
	cur=con.cursor()
	con2=cx_Oracle.connect(local)
	cur2=con2.cursor()
	print(con.version,con2.version)
	
	outagesFetchSql='''select rto.ID as pwc_id, rto.ELEMENT_ID,rto.ELEMENTNAME as ELEMENT_NAME,rto.ENTITY_ID, ent_master.ENTITY_NAME,
	gen_unit.installed_capacity as CAPACITY,rto.OUTAGE_DATE as OUTAGE_DATETIME,rto.REVIVED_DATE as REVIVED_DATETIME, rto.CREATED_DATE as CREATED_DATETIME, 
    rto.MODIFIED_DATE as MODIFIED_DATETIME, sd_tag.name as shutdown_tag,rto.SHUTDOWN_TAG_ID,sd_type.name as shutdown_typename,
    rto.SHUT_DOWN_TYPE as SHUT_DOWN_TYPE_ID, rto.OUTAGE_REMARKS,reas.reason,rto.REASON_ID, rto.REVIVAL_REMARKS, rto.REGION_ID,rto.SHUTDOWNREQUEST_ID,
    rto.OUTAGE_TIME, rto.REVIVED_TIME from REPORTING_WEB_UI_UAT.real_time_outage rto 
    left join REPORTING_WEB_UI_UAT.outage_reason reas on reas.id = rto.reason_id
    left join REPORTING_WEB_UI_UAT.shutdown_outage_tag sd_tag on sd_tag.id = rto.shutdown_tag_id
    left join REPORTING_WEB_UI_UAT.shutdown_outage_type sd_type on sd_type.id = rto.shut_down_type
    left join REPORTING_WEB_UI_UAT.entity_master ent_master on ent_master.id = rto.ENTITY_ID
    left join REPORTING_WEB_UI_UAT.generating_unit gen_unit on gen_unit.id = rto.element_id 
    '''
	cur.execute(outagesFetchSql)
	# cur.execute(outagesFetchSql,(dt.datetime(2019,7,5),dt.datetime(2019,7,6)))
	# cur.execute(outagesFetchSql, (startDate, endDate))
	colNames = [row[0] for row in cur.description]
	colNames = colNames[0:-2]
	colNames.append('OWNERS')
	dbRows = cur.fetchall()
    # print(dbRows)
	elemIdIndex= 1
	elemNameIndex= 2
	elemTypeIndex= 4
	instCapIndex= 5
	outDateIndex= 6
	revDateIndex= 7
	acTransLineCktOwners=[]
	bayOwners=[]
	busOwners=[]
	busReactorOwners=[]
	compensatorOwners=[]
	fscOwners=[]
	genUnitOwners=[]
	hvdcLineCktOwners=[]
	hvdcPoleOwners=[]
	lineReactorOwners=[]
	transfomerOwners=[]
	for rIter in range(len(dbRows)):
	    # convert tuple to list to facilitate manipulation
	    dbRows[rIter] = list(dbRows[rIter])
	    # get the element Id and element type of outage entry
	    elemName = dbRows[rIter][elemNameIndex]
	    elemId = dbRows[rIter][elemIdIndex]
	    elemType = dbRows[rIter][elemTypeIndex]
	    if elemType == 'AC_TRANSMISSION_LINE_CIRCUIT':
	        acTransLineCktOwners.append(elemId)
	    elif elemType == 'GENERATING_UNIT':
	        genUnitOwners.append(elemId)
	    elif elemType == 'FSC':
	        fscOwners.append(elemId)
	    elif elemType == 'HVDC_LINE_CIRCUIT':
	        hvdcLineCktOwners.append(elemId)
	    elif elemType == 'BUS REACTOR':
	        busReactorOwners.append(elemId)
	    elif elemType == 'LINE_REACTOR':
	        lineReactorOwners.append(elemId)
	    elif elemType == 'TRANSFORMER':
	        transfomerOwners.append(elemId)
	    elif elemType == 'HVDC POLE':
	        hvdcPoleOwners.append(elemId)
	    elif elemType == 'BUS':
	        busOwners.append(elemId)
	    elif elemType == 'Bay':
	        bayOwners.append(elemId)
	    elif elemType in ['TCSC', 'MSR', 'MSC', 'STATCOM']:
	        compensatorOwners.append(elemId)
	    # convert installed capacity to string
	    instCap = dbRows[rIter][instCapIndex]
	    if elemType == 'GENERATING_UNIT':
	        instCap = str(instCap)
	    else:
	        instCap = extractVoltFromName(elemName)
	    dbRows[rIter][instCapIndex] = instCap

	    outageDateTime = dbRows[rIter][outDateIndex]
	    if not pd.isnull(outageDateTime):
	    	# strip off hours and minute components
	        outageDateTime = outageDateTime.replace(hour=0, minute=0, second=0, microsecond=0)
	        outTimeStr = dbRows[rIter][-2]
	        outageDateTime += timeDeltaFromStr(outTimeStr)
	        dbRows[rIter][outDateIndex] = outageDateTime

	    revivalDateTime = dbRows[rIter][revDateIndex]
	    if not pd.isnull(revivalDateTime):
	    	# strip off hours and minute components
	        revivalDateTime = revivalDateTime.replace(hour=0, minute=0, second=0, microsecond=0)
	        revTimeStr = dbRows[rIter][-1]
	        revivalDateTime +=timeDeltaFromStr(revTimeStr)
	        dbRows[rIter][revDateIndex] = revivalDateTime
	    # remove last 2 column of the row
	    dbRows[rIter] = dbRows[rIter][0:-2]

    # fetch owners for each type separately
	acTransLineCktOwners = getOwnersForAcTransLineCktIds(con, acTransLineCktOwners)
	bayOwners = getOwnersForBayIds(con,bayOwners)
	busOwners = getOwnersForBusIds(con,busOwners)
	busReactorOwners = getOwnersForBusReactorIds(con, busReactorOwners)
	compensatorOwners = getOwnersForCompensatorIds(con,compensatorOwners)
	fscOwners = getOwnersForFscIds(con,fscOwners)
	genUnitOwners = getOwnersForGenUnitIds(con,genUnitOwners)
	hvdcLineCktOwners = getOwnersForHvdcLineCktIds(con,hvdcLineCktOwners)
	hvdcPoleOwners = getOwnersForHvdcPoleIds(con, hvdcPoleOwners)
	lineReactorOwners = getOwnersForLineReactorIds(con,lineReactorOwners)
	transfomerOwners = getOwnersForTransformerIds(con,transfomerOwners)

	for rIter in range(len(dbRows)):
	    elemId = dbRows[rIter][elemIdIndex]
	    elemType = dbRows[rIter][elemTypeIndex]
	    if elemType == 'AC_TRANSMISSION_LINE_CIRCUIT':
	        dbRows[rIter].append(acTransLineCktOwners[elemId])
	    elif elemType == 'GENERATING_UNIT':
	        dbRows[rIter].append(genUnitOwners[elemId])
	    elif elemType == 'FSC':
	        dbRows[rIter].append(fscOwners[elemId])
	    elif elemType == 'HVDC_LINE_CIRCUIT':
	        dbRows[rIter].append(hvdcLineCktOwners[elemId])
	    elif elemType == 'BUS REACTOR':
	        dbRows[rIter].append(busReactorOwners[elemId])
	    elif elemType == 'LINE_REACTOR':
	        dbRows[rIter].append(lineReactorOwners[elemId])
	    elif elemType == 'TRANSFORMER':
	        dbRows[rIter].append(transfomerOwners[elemId])
	    elif elemType == 'HVDC POLE':
	        dbRows[rIter].append(hvdcPoleOwners[elemId])
	    elif elemType == 'BUS':
	        dbRows[rIter].append(busOwners[elemId])
	    elif elemType == 'Bay':
	        dbRows[rIter].append(bayOwners[elemId])
	    elif elemType in ['TCSC', 'MSR', 'MSC', 'STATCOM']:
	        dbRows[rIter].append(compensatorOwners[elemId])
	    # convert row to tuple
	    dbRows[rIter] = tuple(dbRows[rIter])

	sqlPlceHldrsTxt = ','.join([':{0}'.format(x+1) for x in range(len(colNames))])
	pwcIdColInd = colNames.index('PWC_ID')
	pwcIds = [(x[pwcIdColInd],) for x in dbRows]
	cur2.executemany("delete from outage_events where PWC_ID=:1", pwcIds)
	# insert the raw data
	ouatageEvntsInsSql = 'insert into outage_events({0}) values ({1})'.format(','.join(colNames), sqlPlceHldrsTxt)
	cur2.executemany(ouatageEvntsInsSql,dbRows)
	con2.commit()


	# df=pd.DataFrame(res,columns=['ID','ELEMENT_ID','ELEMENTNAME','ENTITY_ID','ENTITY_NAME','INSTALLED_CAPACITY','OUTAGE_DATETIME',
	# 	'REVIVED_DATETIME','CREATED_DATETIME','MODIFIED_DATETIME','SHUTDOWN_TAG','SHUTDOWN_TAG_ID','SHUTDOWN_TYPENAME','SHUT_DOWN_TYPE_ID',
	# 	'OUTAGE_REMARKS','REASON','REASON_ID','REVIVAL_REMARKS','REGION_ID','SHUTDOWNREQUEST_ID','IS_DELETED','OUTAGE_TIME','REVIVED_TIME'])
	# df['PWC_ID']=df['ID']

	# df.to_excel('Generator2.xlsx',index=False,engine='xlsxwriter')
	# print(df.info())
	# df['OUTAGE_DATE']=pd.to_datetime(df['OUTAGE_DATE'].dt.date)+pd.to_timedelta(df['OUTAGE_TIME'])
	# df['REVIVED_DATE']=pd.to_datetime(df['REVIVED_DATE'].dt.date)+pd.to_timedelta(df['REVIVED_TIME'])
	# print(df['OUTAGE_DATE'])

	# pd.set_option("display.max_rows",None,'display.max_columns',None)

	# print(df['REVIVED_TIME'])
	# df['REVIVED_TIME']=(pd.to_timedelta(df['REVIVED_TIME']+':00'))
	# print(df['REVIVED_TIME'])


	# df['REVIVED_TIME']=pd.to_timedelta([time_formatter(i) for i in df['REVIVED_TIME'] ])

	# df['REVIVED_TIME']=[timeDeltaFromStr(i) for i in df['REVIVED_TIME']]
	# df['REVIVED_DATETIME']=pd.to_datetime(df['REVIVED_DATETIME'].dt.date)+df['REVIVED_TIME']
	# print(df['REVIVED_DATETIME'])

	# df['OUTAGE_TIME']=pd.to_timedelta([i+':00' if len(i.split(':'))==2 else i for i in df['OUTAGE_TIME'] ])
	# df['OUTAGE_TIME']=pd.to_timedelta([time_formatter(i) for i in df['OUTAGE_TIME']])
	# print(df['OUTAGE_DATETIME'])

	# df['OUTAGE_TIME']=[timeDeltaFromStr(i) for i in df['OUTAGE_TIME']]
	# df['OUTAGE_DATETIME']=pd.to_datetime(df['OUTAGE_DATETIME'].dt.date)+df['OUTAGE_TIME']
	# # print(df['OUTAGE_DATETIME'])

	# df=df.drop(['OUTAGE_TIME','REVIVED_TIME'],axis=1)

	# for i in df['INSTALLED_CAPACITY']:
	# 	print(np.isnan(i))
	# df['INSTALLED_CAPACITY']=[0 if np.isnan(i) else i for i in df['INSTALLED_CAPACITY']]

	# df['INSTALLED_CAPACITY'].fillna(0,inplace=True)

	# print(df['INSTALLED_CAPACITY'])
	# print(df.info())

	# df['SHUTDOWN_TAG_ID'].fillna(0,inplace=True)

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

	# print(type(df.values))
	# print(len(df.values))
	# rows=[tuple(r) for r in df.values]
	# for r in rows:
	# 	print(r)

	# length,batchSize=len(rows),1000
	# quotient,reminder=length//batchSize,length%batchSize
	# start,end=0,batchSize
	# for i in range(quotient):
	# 	cur2.executemany('''insert into GEN_UNIT (ID,ELEMENT_ID,ELEMENT_NAME,ENTITY_ID,ENTITY_NAME,INSTALLED_CAPACITY,
	# 	OUTAGE_DATETIME,REVIVED_DATETIME,CREATED_DATETIME,MODIFIED_DATETIME,SHUTDOWN_TAG,SHUTDOWN_TAG_ID,SHUTDOWN_TYPENAME,SHUT_DOWN_TYPE_ID,
	# 	OUTAGE_REMARKS,REASON,REASON_ID,REVIVAL_REMARKS,REGION_ID,SHUTDOWNREQUEST_ID,IS_DELETED,PWC_ID) 
	# 	values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22)''',rows[start:end])
	# 	con2.commit() 
	# 	start+=batchSize
	# 	end+=batchSize
	# end=start+reminder
	# cur2.executemany('''insert into GEN_UNIT (ID,ELEMENT_ID,ELEMENT_NAME,ENTITY_ID,ENTITY_NAME,INSTALLED_CAPACITY,
	# 	OUTAGE_DATETIME,REVIVED_DATETIME,CREATED_DATETIME,MODIFIED_DATETIME,SHUTDOWN_TAG,SHUTDOWN_TAG_ID,SHUTDOWN_TYPENAME,SHUT_DOWN_TYPE_ID,
	# 	OUTAGE_REMARKS,REASON,REASON_ID,REVIVAL_REMARKS,REGION_ID,SHUTDOWNREQUEST_ID,IS_DELETED,PWC_ID) 
	# 	values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22)''',rows[start:end])
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