import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

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
