import pandas as pd
def getConfig():
	df=pd.read_excel('D:/MIS/configuration/config.xlsx',header=None,index_col=0)
	configDict=df[1].to_dict()
	print(configDict)
	# return configDict['user'], configDict['pass'], configDict['server']
	return configDict['conStr'], configDict['localDb']

# print(getConfig())