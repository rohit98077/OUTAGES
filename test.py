def time_formatter(string):
	if string:
		if '.' in string:
			string=string.replace('.','')
		lst=string.split(':')
		if len(lst)>1 and lst[1]=='':
			lst[1]='00'
			string=":".join(lst)
		if len(lst)==3:
			return string
		elif len(lst)==2:
			return string+':00'
		elif len(lst)==1:
			return string+':00:00'
	else:
		return string

# print(time_formatter('10:90:12'))
# print(time_formatter('10:12'))
# print(time_formatter('10::12'))
# print(time_formatter(''))
# print(time_formatter('10'))
# lst=list(range(19373))

length=19373
batchSize=1000
quotient,reminder=length//batchSize,length%batchSize
start,end=0,batchSize
for i in range(quotient):
	print(start,end)
	start+=batchSize
	end+=batchSize
end=start+reminder
print(start,end)

# rowBatchSize=1000
# for batchStartItr in range(0, length, rowBatchSize):
# 	batchEndItr = min(length, batchStartItr+rowBatchSize)
# 	print(batchStartItr,batchEndItr)
