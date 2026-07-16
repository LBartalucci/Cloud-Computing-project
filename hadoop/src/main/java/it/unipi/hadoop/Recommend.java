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

    private Recommend() {
    }

    public static class RecoMapper extends Mapper<LongWritable, Text, LongWritable, ProductCountWritable> {

        private final LongWritable outKey = new LongWritable();
        private final ProductCountWritable outVal = new ProductCountWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Removes bad lines
            String line = value.toString();
            if (line.isEmpty()) {
                return;
            }

            // Removes bad lines
            String[] f = line.split("\t");
            if (f.length != 3) {
                context.getCounter("Recommend", "MALFORMED_PAIRS").increment(1);
                return;
            }

            // For each pair, keeps the anchor item as key and uses as value the pair "co-occured item, 1" and viceversa.
            // So it duplicate the lines in input
            try {
                long a = Long.parseLong(f[0].trim());
                long b = Long.parseLong(f[1].trim());
                long c = Long.parseLong(f[2].trim());

                outKey.set(a);
                outVal.set(b, c);
                context.write(outKey, outVal);

                outKey.set(b);
                outVal.set(a, c);
                context.write(outKey, outVal);
            } catch (NumberFormatException e) {
                context.getCounter("Recommend", "MALFORMED_PAIRS").increment(1);
            }
        }
    }

    // The recombiner makes a local aggregation between the same lines 
    public static class RecoCombiner
            extends Reducer<LongWritable, ProductCountWritable, LongWritable, ProductCountWritable> {

        private final ProductCountWritable outVal = new ProductCountWritable();

        @Override
        public void reduce(LongWritable key, Iterable<ProductCountWritable> values, Context context)
                throws IOException, InterruptedException {

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

    /* 
    Now the local output goes on the custom partitioner,
    to be sure that same anchor product lines are grouped to the same reducer.
    */

    /* 
    The reducer aggregates the values of the same co-occurred product counters and 
    select the top N and output them for each product
    */

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

            // Now it's the global sum for an anchor product, not the local made by the recombiner.
            Map<Long, Long> sums = new HashMap<>();
            for (ProductCountWritable v : values) {
                sums.merge(v.getProduct(), v.getCount(), Long::sum);
            }

            // The min-heap keeps only the top-N highest counts.
            PriorityQueue<long[]> heap = new PriorityQueue<>(
                    (x, y) -> x[1] != y[1] ? Long.compare(x[1], y[1]) : Long.compare(y[0], x[0]));
            for (Map.Entry<Long, Long> e : sums.entrySet()) {
                heap.offer(new long[]{e.getKey(), e.getValue()});
                if (heap.size() > topN) {
                    heap.poll();
                }
            }

            // Output in descending order from high to low.
            java.util.ArrayList<long[]> ranked = new java.util.ArrayList<>(heap);
            ranked.sort((x, y) -> x[1] != y[1] ? Long.compare(y[1], x[1]) : Long.compare(x[0], y[0]));

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
