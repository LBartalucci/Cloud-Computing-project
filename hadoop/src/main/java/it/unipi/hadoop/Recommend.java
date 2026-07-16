package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/*
Job 2 — Aggregation and top-N ranking.
 
Input: Job 1 output lines.
Each line is one session in which product a and b co-occurred.

For each anchor product it sums co-occurrence counts over all candidate products and emits the top-N strongest candidates.

Output: Top N Ranking of the co-occurred products per product.

 */


public final class Recommend {
    public static final String CONF_TOPN = "cc.topn";

    public static class RecoMapper extends Mapper<LongWritable, Text, LongWritable, StripeWritable> {
        // In-Mapper 
        private Map<Long, StripeWritable> localAggregation;
        private int recordsProcessed;
        private static final int FLUSH_THRESHOLD = 10000;

        @Override
        protected void setup(Context context) {
            localAggregation = new HashMap<>();
            recordsProcessed = 0;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) return;

            String[] fields = line.split("\t");
            if (fields.length != 2) return;

            try {
                long anchor = Long.parseLong(fields[0].trim());
                StripeWritable inputStripe = new StripeWritable();
                
                // Parsing
                String[] items = fields[1].split(",");
                for (String item : items) {
                    String[] pc = item.split(":");
                    inputStripe.add(Long.parseLong(pc[0]), Long.parseLong(pc[1]));
                }

                // Local aggregation (combiner inside the mapper)
                localAggregation.putIfAbsent(anchor, new StripeWritable());
                localAggregation.get(anchor).merge(inputStripe);
                recordsProcessed++;

                if (recordsProcessed >= FLUSH_THRESHOLD) {
                    flush(context);
                }
            } catch (Exception e) {
                context.getCounter("Recommend", "MALFORMED_STRIPES").increment(1);
            }
        }
        // Flushes local data periodically to avoid Out-Of-Memory.
        private void flush(Context context) throws IOException, InterruptedException {
            LongWritable outKey = new LongWritable();
            for (Map.Entry<Long, StripeWritable> entry : localAggregation.entrySet()) {
                outKey.set(entry.getKey());
                context.write(outKey, entry.getValue());
            }
            localAggregation.clear();
            recordsProcessed = 0;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!localAggregation.isEmpty()) {
                flush(context);
            }
        }
    }

    // Now the partitioner does his work by routing to same reducer same anchor product pairs

    public static class RecoReducer extends Reducer<LongWritable, StripeWritable, LongWritable, Text> {
        private int topN;
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) {
            topN = context.getConfiguration().getInt(CONF_TOPN, 10);
        }

        @Override
        public void reduce(LongWritable key, Iterable<StripeWritable> values, Context context)
                throws IOException, InterruptedException {

            StripeWritable finalStripe = new StripeWritable();
            for (StripeWritable v : values) {
                finalStripe.merge(v);
            }

            // Min-Heap for Top-N
            PriorityQueue<long[]> heap = new PriorityQueue<>(
                    (x, y) -> x[1] != y[1] ? Long.compare(x[1], y[1]) : Long.compare(y[0], x[0]));
            
            for (Map.Entry<Long, Long> e : finalStripe.getStripe().entrySet()) {
                heap.offer(new long[]{e.getKey(), e.getValue()});
                if (heap.size() > topN) heap.poll();
            }

            java.util.ArrayList<long[]> ranked = new java.util.ArrayList<>(heap);
            ranked.sort((x, y) -> x[1] != y[1] ? Long.compare(y[1], x[1]) : Long.compare(x[0], y[0]));

            StringBuilder sb = new StringBuilder();
            for (long[] pc : ranked) {
                if (sb.length() > 0) sb.append(',');
                sb.append(pc[0]).append(':').append(pc[1]);
            }
            outVal.set(sb.toString());
            context.write(key, outVal);
        }
    }
}