package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
Custom partitioner for Job 2.
Routes all candidate pairs sharing the same anchor product ID to the exact same reducer.

Uses the built-in Writable hashCode masked with a bitwise AND to prevent negative partition indices,
ensuring uniform distribution across available reducers without risking OutOfBounds exceptions.
*/

public class AnchorPartitioner extends Partitioner<LongWritable, ProductCountWritable> {

    @Override
    public int getPartition(LongWritable key, ProductCountWritable value, int numPartitions) {
        // key.hashCode() efficiently compresses the 64-bit long into a 32-bit integer.
        // The bitwise AND with 0x7fffffff strips the sign bit, forcing the value to be positive
        // before applying the '%' operation.
        return (key.hashCode() & 0x7fffffff) % numPartitions;
    }
}

