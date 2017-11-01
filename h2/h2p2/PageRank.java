package h2p2;

import java.net.URI;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.FileNotFoundException;

import java.util.Scanner;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {
    public static class PGMapper extends Mapper<Object, Text, IntWritable. DoubleWritable> {
        private HashMap<Integer. LinkedList> map;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
            Path getPath = new Path(cacheFiles[0].getPath());
            ObjectInputStream ois = new ObjectInputStream(fs.open(getPath));
            try {
                map = (HashMap) ois.readObject();
            } catch (ClassNotFoundException e) {
                return;
            }
            ois.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner sc = new Scanner(value.toString());
            int node = sc.nextInt();
            double value = sc.nextDouble();
            if (map.containsKey(node)) {
                LinkedList list = map.get(node);
                int size = list.size();
                for (int dest: list) {
                    context.write(new IntWritable(dest), new DoubleWritable((value + 0.0) / size));
                }
            } else {
                context.write(new IntWritable(node), new DoubleWritable(-value));
            }
        }
    }

    public static class PGReducer extends Reducer<IntWritable. DoubleWritable, IntWritable, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private int iter;
        private double theta;
        private MultipleOutputs<IntWritable, DoubleWritable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.iter = Integer.parseInt(conf.get("iter"));
            this.theta = Integer.parseDouble(conf.get("theta"));
            mos = new MultipleOutputs<IntWritable, DoubleWritable>(context);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (mos != null) {
                mos.close();
            }
        }

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            boolean flag = false;
            for (DoubleWritable val: values) {
                if (val.get() < 0) {
                    flag = true;
                    sum = val.get();
                    break;
                } else {
                    sum += val.get();
                }
            }
            result.set(sum);
            if (flag) {
                mos.write("lost", key, result);
            }
            mos.write("values", key, result);
        }
    }

    public static class FinalMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner sc = new Scanner(value.toString());
            int node = sc.nextInt();
            double value = sc.nextDouble();
            if (value > theta) {
                context.write(new IntWritable(node), new DoubleWritable(value));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        HashMap<Integer, LinkedList> map = new HashMap<Integer, LinkedList>();
        String inputPath = args[0];
        String outputPath = args[1];
        int n = Integer.parseInt(args[2]);
        int alpha = Integer.parseInt(args[3]);
        double theta = Double.parseDouble(args[4]);
        int numNodes = 0;
        int iter = 0;
        double G;

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/temp0"));
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path("/hash.out")));
        PrintWriter pw = new PrintWriter(fs.create(new Path("temp0updated/part-r-00000")));
        for (FileStatus fstatus: status) {
            Scanner sc2 = new Scanner(fs.open(fstatus.getPath()));
            while (sc.hasNextLine()) {
                numNodes += 1;
                Scanner sc2 = new Scanner(sc.nextLine());
                int src = sc2.next();
                int dest = sc2.next();
                if (!map.containsKey(src)) {
                    LinkedList list = new LinkedList();
                    list.add(dest);
                    map.put(src, list);
                } else {
                    map.get(src).add(dest);
                }
                pw.println(src + " " + theta);
            }
        }

        oos.writeObject(map);
        oos.close();
        pw.close();
        map = null;
        DistributedCache.addCacheFile(new Path("/hash.out").toUri, conf);
        G = numNodes * theta;

        while (iter < n) {
            conf.set("numNodes", Integer.toString(numNodes));
            conf.set("theta", Double.toString(theta));
            conf.set("iter", Integer.toString(iter));

            Job job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PGMapper.class);
            job.setCombinerClass(PGReducer.class);
            job.setReducerClass(PGReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path("/temp" + Integer.toString(iter) + "updated/part-r-00000"));
            MultipleOutputs.addNamedOutput(job, "values", TextOutputFormat.class, IntWritable.class, DoubleWritable.class);
            MultipleOutputs.addNamedOutput(job, "lost", TextOutputFormat.class, IntWritable.class, DoubleWritable.class);
            FileOutputFormat.setOutputPath(job, new Path("/temp" + Integer.toString(iter + 1)));
            job.waitForCompletion(true);
            iter += 1;
            
            try {
                fs.delete(new Path("/temp" + Integer.toString(iter - 1)), true);
                fs.delete(new Path("/temp" + Integer.toString(iter - 1)) + "updated/part-r-00000", true);
            } catch (IOException e) {
                
            }
        }
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("/temp" + Integer.toString(iter) + "updated/part-r-00000"));
        FileOutputFormat.addOutputPath(job, new Path(args[1]));
        System.exit(job.waitForcompletion(true) ? 0 : 1);
    }
}
