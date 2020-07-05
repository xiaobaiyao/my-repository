# my-repository


本次大作业选题是基于MapReduce的PageRank算法实现。源码主要包含两个部分，即数据的预处理部分和PageRank算法实现的部分。

#####  数据的预处理部分
由于初始数据的格式为“起始节点 ID  目标节点 ID”，为了将其转化为”起始节点 ID  目标节点A ID 目标节点B ID ...“的格式，需要对数据进行预处理。

1. python版本

&emsp;&emsp;一开始没有意识到用MapReduce做数据预处理，因此使用了python做预处理。python的处理主要思路为：读取数据，对每一个相同的起始节点，将其与其所有目标节点存到一个list中，最后重新读取list，重整格式并输出即可。

&emsp;&emsp;读取并存储到一个列表中：

```
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
```
&emsp;&emsp;将所有起始节点及其目标节点存到list后，list为双重列表，里面一重为一个存放一个起始节点和它的目标节点的列表，外面一重为所有起始节点的列表形成的大列表。只需读取，并重整格式即可输出：

```
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
```
2. MapReduce版本
- map的过程：在读取字符流后，以第一个数字（起始节点）为key，第二个数字（目标节点）作为value即可
```
public class FormatTransformMapper extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable id;
        IntWritable nextId;
        // 得到字符流
        StringTokenizer str = new StringTokenizer(value.toString());
        //依赖java强大的字符串处理功能可以读下一个字符
        if(str.hasMoreTokens()) {
            id = new IntWritable(Integer.parseInt(str.nextToken()));
        } else{
            return;
        }
        // 得到目标节点
        nextId = new IntWritable(Integer.parseInt(str.nextToken()));
        // 通过$标志目标节点
        context.write(id, new Text("$" + nextId));

    }

}
```
- reduce的过程：对key相同的，进行组合，就可以获得一个起始节点所有的目标节点

```
public class FormatTransformReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        ArrayList<String> idList = new ArrayList<String>();       
        // key相同的时候进行reduce，当出现了标志时就提取出目标节点
        for(Text id : values) {
            String idStr = id.toString();
            if (idStr.substring(0,1).equals("$")) {
                idList.add(idStr.substring(1));
            }
        }
        // 初始的PR值
        float pr = 1.0f;
        // 得到结果字符串
        String result = String.valueOf(pr) + " "; 
        for(int i = 0; i< idList.size();i++){
            result += idList.get(i) + " ";
        }

        context.write(key, new Text(result));
    }

}

```

##### PageRank部分
基本思路：节点的权重值（PR值）取决于其他所有页面对其的引用情况。因此为了得到总的PR值，可以用所有节点对其贡献来做MapReduce，也就是使用（**被引用的节点**，**被贡献的值**）进行map，reduce时每一个被引用的节点将所有的被贡献的值加起来，再加上bonus就可以得到PR值。为了使输出格式仍然为“起始节点 ID  临时PR值 目标节点A ID 目标节点B ID ...”，以便多次迭代，还要用（**起始节点**，****目标节点****）作为map，并进行reduce。

- map过程：读取输入的文件，文件格式为“起始节点 ID  临时PR值 目标节点A ID 目标节点B ID ...”，在读取token流后，以起始节点为key，各个目标节点为value进行map，同时以目标节点为key，对其贡献的PR值为value进行map。
```
public class PageRankMapper extends Mapper<Object, Text, IntWritable, Text> {
    
    private IntWritable id;
    private String nextId;
    private int count;
    private float averagePr;
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        
        StringTokenizer str = new StringTokenizer(value.toString());
        
        // 读取token流
        if (str.hasMoreTokens()) {
            id = new IntWritable(Integer.parseInt(str.nextToken()));
        } else {
            return;
        }

        nextId = str.nextToken();
        
        // 得到引用总数
        count = str.countTokens();

        // 引用的占比
        averagePr = Float.parseFloat(nextId) / count;
        
        // 在这里使用&来标记目标节点，把$挪用为标记PR值
        while (str.hasMoreTokens()) {

            String nextId = str.nextToken();
            //对目标节点及其PR做map
            Text tmpText = new Text("$" + averagePr);
            context.write(new IntWritable(Integer.parseInt(nextId)), tmpText);
            //对节点和目标节点做map
            tmpText = new Text("&" + nextId);
            context.write(id, tmpText);
        }
    }
}
```
- reduce过程：结合map过程，分别对目标节点和贡献的PR值做reduce即可：
```
public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    

    ArrayList<String> idList = new ArrayList<String>();

    String output = "  ";

    float pr = 0;

    
    // reduce的过程，分别对目标节点和贡献的PR值做reduce
    for (Text id : values) {
        String idStr = id.toString();
        if (idStr.substring(0, 1).equals("$")) {
            pr += Float.parseFloat(idStr.substring(1));
        }
        else if (idStr.substring(0, 1).equals("&")) {
            String nextId = idStr.substring(1);
            idList.add(nextId);
        }
    }
    // PR计算公式，bonus为避免崩溃所用
    pr = pr * alpha + bonus;

    // 以下部分为重整，调整格式
    for (int i = 0; i < idList.size(); i++) {
        output = output + idList.get(i) + "  ";
    }
    
    
    String result = pr + output;

    // 写入context
    context.write(key, new Text(result));
}
```

