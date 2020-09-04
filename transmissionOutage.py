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


lst=['400KV-APL-MUNDRA-SAMI-1 FSC@ SAMI', 
'HVDC400KV-Vindyachal(PS)-RIHAND-1',
'AURANGABAD - 400KV - BUS 2 MSR@AURANGABAD', 
'AKOLA (2) - 765KV B/R 1',
'400KV-APL-MUNDRA-SAMI-1 L/R@ SAMI - 400KV',
'132KV-BINA-MP-MORWA-1',
'1200KV/400KV BINA-ICT-1',
'HVDC 500KV APL  POLE 1',
'ACBIL - 400KV - BUS 2',
'MAIN BAY - 765KV/400KV BHOPAL-ICT-1 AND BHOPAL - 765KV - BUS 2 at BHOPAL - 765KV']
# print(type(lst),type(lst[0]))
l=[print(i) for i in lst]
# print(l)

# for i in lst:
# 	if ' AND ' in i:
# 		name=i.split('AND')
# 		req=name[0]
# 		if '/' in req:
# 			req=name[1]
# 		ind=req.index('KV ')
# 		print(req[ind-3:ind+2])
# 	elif 'ICT' in i:
# 		ind=i.index('KV ')
# 		print(i[:ind+2])
# 	else:
# 		ind=i.index('KV')
# 		print(i[ind-3:ind+2])

for i in lst:
	print(extractVoltFromName(i))