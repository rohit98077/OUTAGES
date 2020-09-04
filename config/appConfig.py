import pandas as pd
def getConfig():
	df=pd.read_excel('D:/MIS/config/config.xlsx',header=None,index_col=0)
	return df[1].to_dict()

# print(getConfig())