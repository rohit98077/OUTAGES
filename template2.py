from docx.shared import Cm
from docxtpl import DocxTemplate, InlineImage
import datetime as dt
import cx_Oracle

def fetchGenUnitOutages(con,stDate,endDate):
    cur=con.cursor()
    sqlFetch='''
    SELECT oe.ELEMENT_NAME,
    oe.OWNERS,
    oe.CAPACITY,
    oe.OUTAGE_DATETIME,
    oe.REVIVED_DATETIME,
    oe.OUTAGE_REMARKS,
    oe.REASON,
    oe.shutdown_tag 
    from outage_events oe
    where (oe.entity_name = 'GENERATING_UNIT') and 
    (
        (oe.OUTAGE_DATETIME between :1 and :2) 
        or (oe.REVIVED_DATETIME between :1 and :2) 
        or (oe.OUTAGE_DATETIME <= :1 and oe.REVIVED_DATETIME >= :2)
        or (oe.OUTAGE_DATETIME <= :1 and oe.REVIVED_DATETIME IS NULL)
    ) order by oe.OUTAGE_DATETIME desc'''
    
    cur.execute(sqlFetch,(stDate,endDate))
    res=cur.fetchall()
    colNames=[r[0] for r in cur.description]
    col=['elName','owners','capacity','outageTime','outageDate','revivalTime','revivalDate','reason' ]
    lst=[]
    for r in res:
        outDateStr= dt.datetime.strftime(r[3], "%d-%m-%Y")
        outTimeStr= dt.datetime.strftime(r[3], "%H:%M")
        revivalDateStr = 'Still out'
        revivalTimeStr = 'Still out'
        revivalDt = r[4]     
        if revivalDt:
            revivalDateStr = dt.datetime.strftime(revivalDt, "%d-%m-%Y")
            revivalTimeStr = dt.datetime.strftime(revivalDt, "%H:%M")
        remarks = r[5]
        reason = r[6]
        outageTag = r[7]
        if outageTag == 'Outage':
            outageTag = None
        reasonStr = ' / '.join([r for r in [outageTag, reason,remarks] if r])
        r=[r[0],r[1],r[2],outTimeStr,outDateStr,revivalTimeStr,revivalDateStr,reasonStr]
        lst.append(r)  
    lstOfDict=[dict(zip(col,r)) for r in lst]
    return lstOfDict

def fetchTransElOutages(con,stDate,endDate):
    cur=con.cursor()
    sqlFetch='''
    SELECT oe.ELEMENT_NAME,
    oe.OWNERS, oe.CAPACITY,
    oe.OUTAGE_DATETIME, 
    oe.REVIVED_DATETIME,
    oe.OUTAGE_REMARKS, 
    oe.REASON,oe.shutdown_tag 
    from outage_events oe
    where (oe.entity_name != 'GENERATING_UNIT') and 
    (
        (oe.OUTAGE_DATETIME between :1 and :2) 
        or (oe.REVIVED_DATETIME between :1 and :2) 
        or (oe.OUTAGE_DATETIME <= :1 and oe.REVIVED_DATETIME >= :2)
        or (oe.OUTAGE_DATETIME <= :1 and oe.REVIVED_DATETIME IS NULL)
    ) order by oe.OUTAGE_DATETIME desc'''
    cur.execute(sqlFetch,(stDate,endDate))
    res=cur.fetchall()
    colNames=[r[0] for r in cur.description]
    # print(colNames)
    col=['elName','owners','capacity','outageTime','outageDate','revivalTime','revivalDate','reason' ]
    lst=[]
    for r in res:
        outDateStr= dt.datetime.strftime(r[3], "%d-%m-%Y")
        outTimeStr= dt.datetime.strftime(r[3], "%H:%M")
        revivalDateStr = 'Still out'
        revivalTimeStr = 'Still out'
        revivalDt = r[4]        
        if revivalDt:
            revivalDateStr = dt.datetime.strftime(revivalDt, "%d-%m-%Y")
            revivalTimeStr = dt.datetime.strftime(revivalDt, "%H:%M")
        remarks = r[5]
        reason=r[6]
        outageTag=r[7]
        if outageTag == 'Outage':
            outageTag = None
        reasonStr = ' / '.join([r for r in [outageTag, reason,remarks] if r])
        r=[r[0],r[1],r[2],outTimeStr,outDateStr,revivalTimeStr,revivalDateStr,reasonStr]
        lst.append(r)  
    lstOfDict=[dict(zip(col,r)) for r in lst]
    return lstOfDict

def fetchlongTimeUnrevivedForcedOutages(con,stDate,endDate):
    cur=con.cursor()
    sqlFetch='''
    SELECT oe.ELEMENT_NAME,
    oe.OWNERS, oe.CAPACITY,
    oe.OUTAGE_DATETIME, 
    oe.REVIVED_DATETIME,
    oe.OUTAGE_REMARKS, 
    oe.REASON,
    oe.shutdown_tag 
    from outage_events oe
    where (oe.shutdown_typename = 'FORCED') and 
    (oe.OUTAGE_DATETIME < :1) and ((oe.REVIVED_DATETIME IS NULL) or (oe.REVIVED_DATETIME>:1)) and
    ((:1 - oe.OUTAGE_DATETIME) > INTERVAL '180' DAY(3)) 
    order by oe.OUTAGE_DATETIME'''

    cur.execute(sqlFetch,(endDate,))
    res=cur.fetchall()
    colNames=[r[0] for r in cur.description]
    # print(colNames)
    col=['elName','owners','capacity','outageTime','outageDate','revivalTime','revivalDate','reason' ]
    lst=[]
    for r in res:
        outDateStr= dt.datetime.strftime(r[3], "%d-%m-%Y")
        outTimeStr= dt.datetime.strftime(r[3], "%H:%M")
        revivalDateStr = 'Still out'
        revivalTimeStr = 'Still out'
        revivalDt = r[4]
        
        if revivalDt:
            revivalDateStr = dt.datetime.strftime(revivalDt, "%d-%m-%Y")
            revivalTimeStr = dt.datetime.strftime(revivalDt, "%H:%M")
        
        remarks = r[5]
        reason = r[6]
        outageTag = r[7]
        if outageTag == 'Outage':
            outageTag = None
        reasonStr = ' / '.join([r for r in [outageTag, reason,remarks] if r])
        r=[r[0],r[1],r[2],outTimeStr,outDateStr,revivalTimeStr,revivalDateStr,reasonStr]
        lst.append(r)  

    lstOfDict=[dict(zip(col,r)) for r in lst]
    return lstOfDict

def get_context(stDate,endDate):
    # con=cx_Oracle.connect('system/12345678@localhost:1521/xe')
    con=cx_Oracle.connect("mis_warehouse","wrldc#123","10.2.100.56:15210/ORCLWR")
    return {
    'genOtgs':fetchGenUnitOutages(con,stDate,endDate),
    'transOtgs':fetchTransElOutages(con,stDate,endDate),
    'longTimeOtgs':fetchlongTimeUnrevivedForcedOutages(con,stDate,endDate)
    }

template = DocxTemplate('outageTemplate.docx')
context = get_context(dt.datetime(2020,9,10),dt.datetime(2020,9,17))  # gets the context used to render the document
# img_size = Cm(7)  # sets the size of the image
# context['signature'] = InlineImage(template, 'signature.png', img_size)  # adds the InlineImage object to the context
template.render(context)
template.save('testReport20.docx')