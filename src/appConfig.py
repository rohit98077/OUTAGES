import pandas as pd
def getConfig(configFilename='config.xlsx'):
	df=pd.read_excel(configFilename,header=None,index_col=0)
	return df[1].to_dict()

if __name__ == '__main__':
	print(getConfig())