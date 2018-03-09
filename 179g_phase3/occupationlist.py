import json

f=open("corgilist.txt","r")
lines=f.readlines()
result=[]
for x in lines:
    result.append(x.split('\t')[1].strip())
f.close()
with open("occlist.json", 'wb') as f:
    json.dump(result, f)
f.close()