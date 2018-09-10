package com.DSALatency;

import com.google.common.base.Joiner;
import org.HdrHistogram.Histogram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class DSAReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text text, Iterator<LongWritable> iterator, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter)
            throws IOException {


        Joiner joiner = Joiner.on("\t").skipNulls();

        Histogram histogram = new Histogram(DSALatency.HistMin, DSALatency.HistMax, DSALatency.HistSigfig);
        String key = text.toString();

        while(iterator.hasNext()){
            long value = iterator.next().get();
            histogram.recordValue(value);
        }

        for(double percentile : DSALatency.Percentiles){
            String reducerKey = joiner.join(key, percentile, histogram.getValueAtPercentile(percentile));
            outputCollector.collect(new Text(reducerKey), null);
        }

    }
}
