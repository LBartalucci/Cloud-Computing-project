package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
Custom partitioner modificato per lo Stripes Pattern.
Invia tutti gli stripe che condividono lo stesso ID del prodotto "anchor" 
allo stesso identico Reducer per l'aggregazione e il ranking finale.
*/
public class AnchorPartitioner extends Partitioner<LongWritable, StripeWritable> {

    @Override
    public int getPartition(LongWritable key, StripeWritable value, int numPartitions) {
        long anchor = key.get();
        // Evita valori negativi dovuti all'overflow applicando una maschera bitwise
        return (int) ((anchor & Long.MAX_VALUE) % numPartitions);
    }
}