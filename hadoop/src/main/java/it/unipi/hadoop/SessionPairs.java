package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Job 1 — Session co-occurrence generation.
 *
 * Map: Inputs REES46 CSV rows, filters by configured event types and outputs (user_session -> product_id).
 * Reduce: Groups by session, extracts distinct products, sorts them to ensure deterministic pair generation,
 *         and emits (ProductPairWritable -> 1) for each unique pair.
 *         Sessions exceeding the maximum size threshold are skipped to filter out bot traffic.
 */

public final class SessionPairs {

    public static final String CONF_EVENT_TYPES = "cc.event.types";
    public static final String CONF_MAX_SESSION = "cc.max.session.size";

    private static final int IDX_EVENT_TYPE = 1;
    private static final int IDX_PRODUCT_ID = 2;
    private static final int IDX_USER_SESSION = 8;
    private static final int MIN_FIELDS = 9;

    private SessionPairs() {
    }

    public static class PairMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text outSession = new Text();
        private final LongWritable outProduct = new LongWritable();
        private final Set<String> eventTypes = new HashSet<>();

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String types = conf.get(CONF_EVENT_TYPES, "cart,purchase");
            for (String t : types.split(",")) {
                String trimmed = t.trim().toLowerCase();
                if (!trimmed.isEmpty()) {
                    eventTypes.add(trimmed);
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty()) {
                return;
            }

            // Skip the CSV header row
            if (line.startsWith("event_time")) {
                return;
            }

            // Skip malformed rows
            String[] f = line.split(",", -1);
            if (f.length < MIN_FIELDS) {
                context.getCounter("SessionPairs", "MALFORMED_ROWS").increment(1);
                return;
            }

            // Filter out unwanted event types
            String eventType = f[IDX_EVENT_TYPE].trim().toLowerCase();
            if (!eventTypes.contains(eventType)) {
                return;
            }

            // Skip records missing critical identifiers
            String session = f[IDX_USER_SESSION].trim();
            String productStr = f[IDX_PRODUCT_ID].trim();
            if (session.isEmpty() || productStr.isEmpty()) {
                return;
            }

            long product;
            try {
                product = Long.parseLong(productStr);
            } catch (NumberFormatException e) {
                context.getCounter("SessionPairs", "BAD_PRODUCT_ID").increment(1);
                return;
            }

            outSession.set(session);
            outProduct.set(product);
            context.write(outSession, outProduct);
        }
    }

    /**
     * Note: A Combiner is explicitly omitted in Job 1 because user sessions are mostly unique.
     * Since there are no duplicate keys within the same input split, a Combiner
     * would introduce CPU and RAM overhead without reducing the shuffle data volume.
     */
    public static class PairReducer extends Reducer<Text, LongWritable, ProductPairWritable, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final ProductPairWritable outPair = new ProductPairWritable();
        private int maxSessionSize;

        @Override
        protected void setup(Context context) {
            maxSessionSize = context.getConfiguration().getInt(CONF_MAX_SESSION, 50);
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            // Extract unique products while enforcing the bot-filtering threshold
            Set<Long> distinct = new HashSet<>();
            for (LongWritable v : values) {
                distinct.add(v.get());
                if (distinct.size() > maxSessionSize) {
                    context.getCounter("SessionPairs", "SKIPPED_LARGE_SESSIONS").increment(1);
                    return;
                }
            }

            // A valid co-occurrence requires at least two distinct products
            if (distinct.size() < 2) {
                return;
            }

            // Convert to a List and sort explicitly. This guarantees that pairs are always generated 
            // in a strict, ascending numerical order, ensuring consistency with the Python baseline.
            List<Long> products = new ArrayList<>(distinct);
            Collections.sort(products);

            // Generate and emit all unique combinations
            for (int i = 0; i < products.size(); i++) {
                for (int j = i + 1; j < products.size(); j++) {
                    outPair.set(products.get(i), products.get(j));
                    context.write(outPair, ONE);
                }
            }
        }
    }
}