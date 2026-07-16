package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*

Job 1 — Session co-occurrence generation.

Map: input REES46 rows, keep configured event types, then output (user_session -> product_id).
Reducer: per session, dedupe products and emit {(ProductPair -> 1)} for each unordered pair. 
Sessions larger than {cc.max.session.size} are skipped to avoid bot sessions.

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

        // event_types selected: cart, purhcase 
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

            // Skip the header
            if (line.startsWith("event_time")) {
                return;
            }

            // Skip bad lines
            String[] f = line.split(",", -1);
            if (f.length < MIN_FIELDS) {
                context.getCounter("SessionPairs", "MALFORMED_ROWS").increment(1);
                return;
            }

            // Skip lines with other events
            String eventType = f[IDX_EVENT_TYPE].trim().toLowerCase();
            if (!eventTypes.contains(eventType)) {
                return;
            }

            // Skip lines without session or products id
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


    /*
    Note: A combiner is not used here because user sessions are mostly unique.
    
    Since there are almost no duplicate keys within the same data split, 
    a combiner cannot reduce the data size. It would only waste RAM. 
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


            // Skip bot sessions
            Set<Long> distinct = new HashSet<>();
            for (LongWritable v : values) {
                distinct.add(v.get());
                if (distinct.size() > maxSessionSize) {
                    context.getCounter("SessionPairs", "SKIPPED_LARGE_SESSIONS").increment(1);
                    return;
                }
            }

            // Keep only session with 2 or more products interacted with
            if (distinct.size() < 2) {
                return;
            }

            // Output all the pairs 
            List<Long> products = new ArrayList<>(distinct);
            for (int i = 0; i < products.size(); i++) {
                for (int j = i + 1; j < products.size(); j++) {
                    outPair.set(products.get(i), products.get(j));
                    context.write(outPair, ONE);
                }
            }
        }
    }
}
