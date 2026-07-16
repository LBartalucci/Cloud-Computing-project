package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StripeWritable implements Writable {
    private Map<Long, Long> stripe;

    public StripeWritable() {
        this.stripe = new HashMap<>();
    }

    public void add(long product, long count) {
        this.stripe.put(product, this.stripe.getOrDefault(product, 0L) + count);
    }

    public void merge(StripeWritable other) {
        for (Map.Entry<Long, Long> entry : other.stripe.entrySet()) {
            this.add(entry.getKey(), entry.getValue());
        }
    }

    public Map<Long, Long> getStripe() {
        return stripe;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(stripe.size());
        for (Map.Entry<Long, Long> entry : stripe.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stripe.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long product = in.readLong();
            long count = in.readLong();
            stripe.put(product, count);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Long> entry : stripe.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    }
}