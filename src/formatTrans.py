
file = open("soc-Epinions1.txt") 

# 临时列表，记录一个起始节点及其所有目标节点
arr=[]
arr.append(-1)
arr.append(0)
#大列表，记录所有节点的情况，也就是arr作为元素形成的大列表
big=[]

for line in file:
    #将每一行的数字分开放在列表中
    wordlist=line.split()
    #如果第一个数字相同（同一个起始节点），添加到列表arr中
    if wordlist[0]==arr[0]:
        arr.append(wordlist[1])
    else:
        #一个起始节点遍历完，这个临时的arr构造完成，添加到大列表中，并清空一下
        tem=arr.copy()
        #tem是为了避免指向同一个内存空间造成覆盖
        big.append(tem)
        arr.clear()
        arr.append(wordlist[0])
        arr.append(wordlist[1])
big.append(arr)
file.close()
big.pop(0)
# print(big)
#重整格式并重新写入
txtName = "codingWord.txt"
f=open(txtName, "a+")
for i in big:
    new_context=''
    for j in i:
        if j==i[0]:
            new_context+=str(j+' '+'1.0'+' ')
        else:
            new_context+=str(j+' ')
    new_context+='\n'
    f.write(new_context)
f.close()