package h2p2;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PDPreProcess {
    public static class PDMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable dest = new IntWritable();
        private Text result = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner sc = new Scanner(value.toString());
            int source = sc.nextInt();
            int destination = sc.nextInt();
            int weight = sc.nextInt();
            dest.set(destination);
            result.set(Integer.toString(source) + "," + Integer.toString(weight) + " ");
            context.write(dest, result);
        }
    }

    public static class PDReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text result = new Text();
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text val: values) {
                sb.append(val.toString());
        }
            result.set(sb.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PDPreProcess");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(PDMapper.class);
        job.setCombinerClass(PDReducer.class);
        job.setReducerClass(PDReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
