package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
Main Driver: Orchestrates the execution of Job 1 (SessionPairs) and Job 2 (Recommend).

Job 1 extracts session co-occurrences and writes intermediate results to <output>_stage1.

Job 2 processes the intermediate output to aggregate scores and extract the top-N recommendations,
saving the final result to the designated <output> path.
*/

public class RecoDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: RecoDriver [-D ...] <input> <output> [numReducers]");
            return 2;
        }

        Configuration conf = getConf();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path stage1 = new Path(args[1] + "_stage1");
        int numReducers = args.length >= 3 ? Integer.parseInt(args[2]) : 2;

        FileSystem fs = FileSystem.get(conf);
        
        // Pre-clean existing output directories to prevent HDFS deployment failures
        fs.delete(stage1, true);
        fs.delete(output, true);

        // ---------- Job 1: Session co-occurrence generation ----------
        Job job1 = Job.getInstance(conf, "ecommerce-reco: Job1 SessionPairs");
        job1.setJarByClass(RecoDriver.class);
        job1.setMapperClass(SessionPairs.PairMapper.class);
        job1.setReducerClass(SessionPairs.PairReducer.class);
        
        // Enable parallel execution for Job 1 to prevent single-reducer bottleneck
        job1.setNumReduceTasks(numReducers);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(ProductPairWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, stage1);

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 (SessionPairs) failed.");
            return 1;
        }

        // ---------- Job 2: Aggregation and top-N ranking ----------
        Job job2 = Job.getInstance(conf, "ecommerce-reco: Job2 Recommend");
        job2.setJarByClass(RecoDriver.class);
        job2.setMapperClass(Recommend.RecoMapper.class);
        job2.setCombinerClass(Recommend.RecoCombiner.class);
        job2.setPartitionerClass(AnchorPartitioner.class);
        job2.setReducerClass(Recommend.RecoReducer.class);
        
        job2.setNumReduceTasks(numReducers);
        
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(ProductCountWritable.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, stage1);
        FileOutputFormat.setOutputPath(job2, output);

        boolean ok = job2.waitForCompletion(true);

        // Clean up intermediate data if configuration allows it
        if (ok && !conf.getBoolean("cc.keep.intermediate", false)) {
            fs.delete(stage1, true);
        }
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Configuration(), new RecoDriver(), args);
        System.exit(rc);
    }
}