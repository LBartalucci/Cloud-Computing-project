package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
Custom partitioner for Job 2: routes all candidate pairs sharing the same anchor product id to the same reducer
*/
public class AnchorPartitioner extends Partitioner<LongWritable, ProductCountWritable> {

    @Override
    public int getPartition(LongWritable key, ProductCountWritable value, int numPartitions) {
        long anchor = key.get();
        return (int) ((anchor & Long.MAX_VALUE) % numPartitions);
    }
}
