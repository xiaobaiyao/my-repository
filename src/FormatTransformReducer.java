import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

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
