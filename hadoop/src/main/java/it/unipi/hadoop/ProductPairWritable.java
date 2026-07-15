package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProductPairWritable implements WritableComparable<ProductPairWritable> {

    private long a;
    private long b;

    public ProductPairWritable() {
    }

    public ProductPairWritable(long x, long y) {
        set(x, y);
    }


    public void set(long x, long y) {
        if (x <= y) {
            this.a = x;
            this.b = y;
        } else {
            this.a = y;
            this.b = x;
        }
    }

    public long getA() {
        return a;
    }

    public long getB() {
        return b;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(a);
        out.writeLong(b);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        a = in.readLong();
        b = in.readLong();
    }

    @Override
    public int compareTo(ProductPairWritable o) {
        int cmp = Long.compare(a, o.a);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(b, o.b);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProductPairWritable)) {
            return false;
        }
        ProductPairWritable that = (ProductPairWritable) o;
        return a == that.a && b == that.b;
    }

    @Override
    public int hashCode() {
        return (int) (a * 31 + b);
    }

    @Override
    public String toString() {
        return a + "\t" + b;
    }
}
