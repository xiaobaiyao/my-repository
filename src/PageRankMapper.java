import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

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
