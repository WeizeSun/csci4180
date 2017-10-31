package h2p1;

import java.net.URI;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.FileNotFoundException;

import java.util.Scanner;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ParallelDijkstra {
    public static class PDMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private int pivot;
        private int distance;
        private Hash hash;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
            Path getPath = new Path(cacheFiles[0].getPath());
            ObjectInputStream ois = new ObjectInputStream(fs.open(getPath));
            try {
                hash = (Hash) ois.readObject();
            } catch (ClassNotFoundException e) {
                return;
            }
            ois.close();
            pivot = Integer.parseInt(conf.get("pivot"));
            distance = Integer.parseInt(conf.get("distance"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner sc = new Scanner(value.toString());
            int node = Math.abs(sc.nextInt());
            int dist = sc.nextInt();
            if (hash.containsKey(pivot, node)) {
                int weight = hash.get(pivot, node);
                if (distance != Integer.MAX_VALUE && dist == Integer.MAX_VALUE) {
                    context.write(new IntWritable(node), new IntWritable(distance + weight));
                    context.write(new IntWritable(node), new IntWritable(-1));
                } else if (distance != Integer.MAX_VALUE && dist != Integer.MAX_VALUE && distance + weight < dist) {
                    context.write(new IntWritable(node), new IntWritable(distance + weight));
                    context.write(new IntWritable(node), new IntWritable(-1));
                }
            }
            context.write(new IntWritable(node), new IntWritable(dist));
        }
    }

    public static class PDReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();
        private int iter;
        private MultipleOutputs<IntWritable, IntWritable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.iter = Integer.parseInt(conf.get("iter"));
            mos = new MultipleOutputs<IntWritable, IntWritable>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (mos != null) {
                mos.close();
            }
        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int minValue = Integer.MAX_VALUE;
            boolean flag = false;
            for (IntWritable val: values) {
                if (val.get() == -1) {
                    flag = true;
                } else {
                    minValue = minValue < val.get() ? minValue : val.get();
                }
            }
            result.set(minValue);
            if (flag) {
                mos.write("changed", key, result);
            }
            mos.write("distance", key, result);
        }
    }

    public static class FinalMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner sc = new Scanner(value.toString());
            int node = sc.nextInt();
            int dist = sc.nextInt();
            if (dist != Integer.MAX_VALUE) {
                context.write(new IntWritable(Math.abs(node)), new IntWritable(dist));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Hash hash = new Hash();
        PriorityQueue<PDNodeWritable> heap = new PriorityQueue<PDNodeWritable>(new Comparator<PDNodeWritable>() {
            public int compare(PDNodeWritable n1, PDNodeWritable n2) {
                if (n1.getDist() < n2.getDist()) {
                    return -1;
                } else if (n1.getDist() > n2.getDist()) {
                    return 1;
                }
                return 0;
            }
        });
        HashSet<Integer> set = new HashSet<Integer>();
        HashSet<Integer> nodes = new HashSet<Integer>();
        String inputPath = args[0];
        String outputPath = args[1];
        int source = Integer.parseInt(args[2]);
        int numIters = Integer.parseInt(args[3]);
        int iter = 0;
        int numNodes = 0;
        PDNodeWritable pivot;

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/temp0"));
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path("/hash.out")));
        PrintWriter pw = new PrintWriter(fs.create(new Path("/temp0/distance-m-00000")));
        for (FileStatus fstatus: status) {
            Scanner sc = new Scanner(fs.open(fstatus.getPath()));
            while (sc.hasNextLine()) {
                Scanner sc2 = new Scanner(sc.nextLine());
                int src = sc2.nextInt();
                int dest = sc2.nextInt();
                int weight = sc2.nextInt();
                if (!hash.containsKey(src, dest) || weight < hash.get(src, dest)) {
                    hash.put(src, dest, weight);
                }
                if (nodes.contains(src)) {
                    continue;
                } else {
                    numNodes += 1;
                }
                if (src == source) {
                    pw.println(src + " " + "0");
                    heap.offer(new PDNodeWritable(src, 0));
                } else {
                    pw.println(src + " " + Integer.MAX_VALUE);
                    heap.offer(new PDNodeWritable(src, Integer.MAX_VALUE));
                }
                nodes.add(src);
            }
        }
        oos.writeObject(hash);
        oos.close();
        pw.close();
        hash = null;
        nodes = null;
        DistributedCache.addCacheFile(new Path("/hash.out").toUri(), conf);

        while (numIters > 0 && iter < numIters || numIters == 0) {
            while (true) {
                pivot = heap.poll();
                if (!set.contains(pivot.getNode())) {
                    break;
                }
            }
            conf.set("pivot", Integer.toString(pivot.getNode()));
            conf.set("distance", Integer.toString(pivot.getDist()));
            conf.set("iter", Integer.toString(iter));

            Job job = Job.getInstance(conf, "Dijkstra");
            job.setJarByClass(ParallelDijkstra.class);
            job.setMapperClass(PDMapper.class);
            job.setCombinerClass(PDReducer.class);
            job.setReducerClass(PDReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("/temp" + Integer.toString(iter) + "/distance-m-00000"));
            MultipleOutputs.addNamedOutput(job, "changed", TextOutputFormat.class, IntWritable.class, IntWritable.class);
            MultipleOutputs.addNamedOutput(job, "distance", TextOutputFormat.class, IntWritable.class, IntWritable.class);
            FileOutputFormat.setOutputPath(job, new Path("/temp" + Integer.toString(iter + 1)));
            job.waitForCompletion(true);
            set.add(pivot.getNode());
            iter += 1;
            if (set.size() >= numNodes) {
                break;
            }
            try {
                Scanner sc = new Scanner(fs.open(new Path("/temp" + Integer.toString(iter) + "/changed-m-00000")));
                while (sc.hasNextLine()) {
                    Scanner sc2 = new Scanner(sc.nextLine());
                    heap.offer(new PDNodeWritable(sc2.nextInt(), sc2.nextInt()));
                }
            } catch (FileNotFoundException e) {

            }

            try {
                FileWriter fw = new FileWriter("heapSize.txt", true);
                fw.write(Integer.toString(iter) + " " + Integer.toString(heap.size()) + "\n");
                fw.close();
            } catch (IOException e) {

            }

            try {
                fs.delete(new Path("/temp" + Integer.toString(iter - 1)), true);
            } catch (IOException e) {
                
            }
        }

        Job job = Job.getInstance(conf, "Dijkstra");
        job.setJarByClass(ParallelDijkstra.class);
        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/temp" + Integer.toString(iter) + "/distance-m-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

