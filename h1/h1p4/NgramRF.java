import java.io.IOException;
import java.util.Scanner;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramRF {
    private static int N;
    private static double theta;
    private static int volume = 0;
    private static LinkedList<Integer> list = new LinkedList<Integer>();
    private static StringBuilder sb = new StringBuilder();
    public static class NgramRFMapper extends Mapper<Object, Text, Text, Text> {
        private Text ngram = new Text();
        private Text head = new Text();
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
                    String output = sb.toString();
                    ngram.set(Integer.toString(1) + ", " + output);
                    if(N == 1){
                        head.set(output);
                    } else {
                        head.set(output.substring(0, list.peek()));
                    }
                    context.write(head, ngram);
                    sb.delete(0, list.poll() + 1);
                }
            }
        }
    }

    public static class NgramRFCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            Text output = new Text();
            for(Text val: values){
                String[] part = val.toString().split(",", 2);
                String ngram = part[1];
                int count = Integer.parseInt(part[0]);
                if(map.containsKey(ngram)){
                    map.put(ngram, map.get(ngram) + count);
                } else {
                    map.put(ngram, count);
                }
            }
            for(Entry<String, Integer> entry: map.entrySet()){
                output.set(entry.getValue() + "," + entry.getKey());
                context.write(key, output);
            }
        }
    }

    public static class NgramRFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            int sum = 0;
            theta = Double.parseDouble(context.getConfiguration().get("theta"));
            Text output = new Text();
            for(Text val: values){
                String[] part = val.toString().split(",", 2);
                int count = Integer.parseInt(part[0]);
                String ngram = part[1];
                sum += count;
                if(map.containsKey(ngram)){
                    map.put(ngram, map.get(ngram) + count);
                } else {
                    map.put(ngram, count);
                }
            }
            for(Entry<String, Integer> entry: map.entrySet()){
                double freq = (entry.getValue() + 0.0) / sum;
                if(freq >= theta){
                    output.set(entry.getKey());
                    result.set(freq);
                    context.write(output, result);
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        conf.set("theta", args[3]);
        Job job = Job.getInstance(conf, "ngram RF");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(NgramRFMapper.class);
        job.setCombinerClass(NgramRFCombiner.class);
        job.setReducerClass(NgramRFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
