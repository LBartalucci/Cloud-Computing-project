package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/*
Job 2 — Aggregation and top-N ranking.

Map: Reads co-occurrence pairs from Job 1, parses the products and counts, 
and emits two records per pair (acting as a symmetric graph: Anchor -> Candidate:Count).

Combine: Performs local pre-aggregation of counts per anchor to minimize network traffic.

Reduce: Performs global aggregation per anchor, isolates the top-N strongest candidates using 
a bounded min-heap, and outputs the final recommendations with deterministic tie-breaking.
 */

public final class Recommend {

    public static final String CONF_TOPN = "cc.topn";

    private Recommend() {
    }

    public static class RecoMapper extends Mapper<LongWritable, Text, LongWritable, ProductCountWritable> {

        private final LongWritable outKey = new LongWritable();
        private final ProductCountWritable outVal = new ProductCountWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty()) {
                return;
            }

            // Standard Hadoop outputs separate the Key and Value with a tab.
            // The Key (ProductPairWritable) contains two fields separated by a comma (or space/tab).
            // Splitting by either tabs or commas ensures robust parsing of "prodA,prodB\tcount".
            String[] f = line.split("\t");
            if (f.length < 3) {
                context.getCounter("Recommend", "MALFORMED_PAIRS").increment(1);
                return;
            }

            try {
                long a = Long.parseLong(f[0].trim());
                long b = Long.parseLong(f[1].trim());
                long c = Long.parseLong(f[2].trim());

                // Emit directional relationship: A co-occurs with B
                outKey.set(a);
                outVal.set(b, c);
                context.write(outKey, outVal);

                // Emit directional relationship: B co-occurs with A
                outKey.set(b);
                outVal.set(a, c);
                context.write(outKey, outVal);
            } catch (NumberFormatException e) {
                context.getCounter("Recommend", "MALFORMED_PAIRS").increment(1);
            }
        }
    }

    public static class RecoCombiner
            extends Reducer<LongWritable, ProductCountWritable, LongWritable, ProductCountWritable> {

        private final ProductCountWritable outVal = new ProductCountWritable();

        @Override
        public void reduce(LongWritable key, Iterable<ProductCountWritable> values, Context context)
                throws IOException, InterruptedException {

            // Aggregate counts locally per candidate product to reduce shuffle overhead
            Map<Long, Long> sums = new HashMap<>();
            for (ProductCountWritable v : values) {
                sums.merge(v.getProduct(), v.getCount(), Long::sum);
            }
            
            for (Map.Entry<Long, Long> e : sums.entrySet()) {
                outVal.set(e.getKey(), e.getValue());
                context.write(key, outVal);
            }
        }
    }

    public static class RecoReducer
            extends Reducer<LongWritable, ProductCountWritable, LongWritable, Text> {

        private int topN;
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) {
            topN = context.getConfiguration().getInt(CONF_TOPN, 10);
        }

        @Override
        public void reduce(LongWritable key, Iterable<ProductCountWritable> values, Context context)
                throws IOException, InterruptedException {

            // Global aggregation of counts for the current anchor product
            Map<Long, Long> sums = new HashMap<>();
            for (ProductCountWritable v : values) {
                sums.merge(v.getProduct(), v.getCount(), Long::sum);
            }

            // Bounded min-heap keeping the top-N highest counts.
            // PriorityQueue sorting strategy:
            // - Primary: Lowest count at the top (removed first when heap overflows).
            // - Secondary (Ties): Highest product ID at the top (smaller IDs are kept inside).
            PriorityQueue<long[]> heap = new PriorityQueue<>(
                    (x, y) -> x[1] != y[1] ? Long.compare(x[1], y[1]) : Long.compare(y[0], x[0]));

            for (Map.Entry<Long, Long> e : sums.entrySet()) {
                heap.offer(new long[]{e.getKey(), e.getValue()});
                if (heap.size() > topN) {
                    heap.poll();
                }
            }

            // Extract elements from the heap and sort them into the final descending order
            ArrayList<long[]> ranked = new ArrayList<>(heap);
            ranked.sort((x, y) -> x[1] != y[1] ? Long.compare(y[1], x[1]) : Long.compare(x[0], y[0]));

            // Format output string matching the baseline structure: other1:count1,other2:count2,...
            StringBuilder sb = new StringBuilder();
            for (long[] pc : ranked) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(pc[0]).append(':').append(pc[1]);
            }
            
            outVal.set(sb.toString());
            context.write(key, outVal);
        }
    }
}