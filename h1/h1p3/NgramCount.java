import java.io.IOException;
import java.util.Scanner;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCount {
    private static int N = 0;
    private static int volume = 0;
    private static LinkedList<Integer> list = new LinkedList<Integer>();
    private static StringBuilder sb = new StringBuilder();
    public static class NgramMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            N = Integer.parseInt(context.getConfiguration().get("N"));
            Scanner sc = new Scanner(value.toString());
            sc.useDelimiter("\\W+");
            while(sc.hasNext()){
                String temp = sc.next();
                list.add(temp.length());
                sb.append(" " + temp);
                if(volume == 0){
                    sb.delete(0, 1);
                }
                if(volume < N){
                    volume++;
                }
                if(volume == N){
                    word.set(sb.toString());
                    context.write(word, one);
                    sb.delete(0, list.poll() + 1);
                }
            }
        }
    }

    public static class NgramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        Job job = Job.getInstance(conf, "ngram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(NgramMapper.class);
        job.setCombinerClass(NgramReducer.class);
        job.setReducerClass(NgramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
