package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class ProductCountWritable implements Writable {

    private long product;
    private long count;

    public ProductCountWritable() {
    }

    public ProductCountWritable(long product, long count) {
        this.product = product;
        this.count = count;
    }

    public void set(long product, long count) {
        this.product = product;
        this.count = count;
    }

    public long getProduct() {
        return product;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(product);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        product = in.readLong();
        count = in.readLong();
    }

    @Override
    public String toString() {
        return product + ":" + count;
    }
}
