import java.net.URI;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import java.util.StringTokenizer;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();

        String InputPath = "dataconvert";

		String output = "dataoutput";
		
        String pathOut = "";

        // 输出格式
        pathOut = output + "1";

        // 连续迭代9次的MapReduce过程，使节点的PR值逐步稳定
        for (int i = 1; i <= 9; i++) {
            Job job = new Job(conf, "MapReduce pagerank");

            //固定的5条
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(InputPath));
            FileOutputFormat.setOutputPath(job, new Path(pathOut));
    
            // 不断用最新的输出作为输入
            InputPath = pathOut;
            pathOut = output + (i + 1);
            
            job.waitForCompletion(true);
        }
    }
}
