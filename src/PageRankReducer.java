import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private static float alpha = 0.85f;
    private static float bonus = 0.2f;
    
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
}
