package h2p2;

import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PRAdjust {
    public static class PGAMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private int numNodes;
        private double alpha;
        private double missing;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            numNodes = Integer.parseInt(conf.get("numNodes"));
            alpha = Double.parseDouble(conf.get("alpha"));
            missing = Double.parseDouble(conf.get("missing")) / numNodes;
            Scanner sc = new Scanner(value.toString());
            int node = sc.nextInt();
            double mass = sc.nextDouble();
            context.write(new IntWritable(node), new DoubleWritable(alpha / numNodes + (1 - alpha) * (missing + mass)));
        }
    }
    public static void main(FileSystem fs, int iter, int numNodes, double alpha) throws Exception {
        double missing = 0;
        try {
            Scanner sc = new Scanner(fs.open(new Path("/temp" + Integer.toString(iter) + "/lost-r-00000")));
            while (sc.hasNextLine()) {
                Scanner sc2 = new Scanner(sc.nextLine());
                int node = sc2.nextInt();
                missing -= sc2.nextDouble();
            }
        } catch (FileNotFoundException e) {

        }

        Configuration conf = new Configuration();
        conf.set("numNodes", Integer.toString(numNodes));
        conf.set("alpha", Double.toString(alpha));
        conf.set("missing", Double.toString(missing));
        Job job = Job.getInstance(conf, "PageRankAdjust");
        job.setJarByClass(PRAdjust.class);
        job.setMapperClass(PGAMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("/temp" + Integer.toString(iter) + "/mass-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/temp" + Integer.toString(iter) + "updated"));
        job.waitForCompletion(true);
    }
}
