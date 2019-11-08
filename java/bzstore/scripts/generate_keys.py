n = 1
alphabets = []
for i in range (0, 3):
    alphabets.append (chr( ord('a')+i))
v=''
keys = []
dic = {}
x=alphabets[0]
lkeys=[]
while n<=3:
    for j in alphabets:
        if len(lkeys)<=0:
            for i in alphabets:
                keys.append(v+i)
                lkeys.append(v+i)
        else:
            mkeys = []
            for k in lkeys:
                for i in alphabets:
                    keys.append(k+i)
                    mkeys.append(k+i)
            lkeys = mkeys
    n+=1
for k in keys:
    print (k)
    dic[k]=True
# print("#keys = %d" %len(keys) )
# print("#keys in dic = %d" %len(dic) )


