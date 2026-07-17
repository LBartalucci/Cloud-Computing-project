package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
Main Driver modificato per lo Stripes Pattern.
Job 1 (SessionPairs con StripeReducer) -> Job 2 (Recommend con RecoMapper + IMC).

L'output del Job 1 viene scritto in <output>_stage1 e usato come input del Job 2, 
che scrive l'output finale in <output>.
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
        fs.delete(stage1, true);
        fs.delete(output, true);

        // ---------- Job 1: Generazione delle co-occorrenze (Stripes) ----------
        Job job1 = Job.getInstance(conf, "ecommerce-reco: Job1 SessionPairs (Stripes)");
        job1.setJarByClass(RecoDriver.class);
        
        // Mapper e Reducer aggiornati per lo Stripes Pattern
        job1.setMapperClass(SessionPairs.PairMapper.class);
        job1.setReducerClass(SessionPairs.StripeReducer.class); 
        
        // Tipi di output del Mapper (Input dello Shuffle)
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        
        // Tipi di output del Reducer (Formato finale scritto su HDFS _stage1)
        job1.setOutputKeyClass(LongWritable.class);      // ID del prodotto ancora
        job1.setOutputValueClass(StripeWritable.class);  // La mappa delle co-occorrenze serializzata
        
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, stage1);

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 (SessionPairs) failed.");
            return 1;
        }

        // ---------- Job 2: Aggregazione locale (IMC) e ordinamento Top-N ----------
        Job job2 = Job.getInstance(conf, "ecommerce-reco: Job2 Recommend (IMC)");
        job2.setJarByClass(RecoDriver.class);
        
        job2.setMapperClass(Recommend.RecoMapper.class);
        job2.setPartitionerClass(AnchorPartitioner.class);
        job2.setReducerClass(Recommend.RecoReducer.class);
        job2.setNumReduceTasks(numReducers);
        
        // NOTA: La riga setCombinerClass è stata eliminata. 
        // L'aggregazione avviene in-memory nel Mapper tramite localAggregation.
        
        // Tipi di output del Mapper (Stripe aggregato localmente prima dello shuffle)
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(StripeWritable.class); 
        
        // Tipi di output del Reducer (Formato finale della raccomandazione)
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, stage1);
        FileOutputFormat.setOutputPath(job2, output);

        boolean ok = job2.waitForCompletion(true);

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