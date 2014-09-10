package cooc;


import java.io.*;
import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class Stripes {
	public static class StripesOccurrenceMapper extends Mapper<LongWritable,Text,Text, myMapWritable> {
		  private myMapWritable occurrenceMap = new myMapWritable();
		  private Text word = new Text();

		 // @Override
		 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		   int neighbors = context.getConfiguration().getInt("neighbors", 2);
		   String[] tokens = value.toString().split("\\s+");
		   if (tokens.length > 1) {
		      for (int i = 0; i < tokens.length; i++) {
		          word.set(tokens[i]);
		          occurrenceMap.clear();

		          int start = (i - neighbors < 0) ? 0 : i - neighbors;
		          int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
		           for (int j = start; j <= end; j++) {
		                if (j == i) continue;
		                Text neighbor = new Text(tokens[j]);
		                if(occurrenceMap.containsKey(neighbor)){
		                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
		                   count.set(count.get()+1);
		                }else{
		                   occurrenceMap.put(neighbor,new IntWritable(1));
		                }
		           }
		          context.write(word,occurrenceMap);
		     }
		   }
		  }
		}
	
	public static class StripesReducer extends Reducer<Text, MapWritable, Text, myMapWritable> {
	    private myMapWritable incrementingMap = new myMapWritable();

	    //@Override
	    protected void reduce(Text key, Iterable<myMapWritable> values, Context context) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (myMapWritable value : values) {
	            addAll(value);
	        }
	        context.write(key, incrementingMap);
	    }

	    private void addAll(myMapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) mapWritable.get(key);
	            if (incrementingMap.containsKey(key)) {
	                IntWritable count = (IntWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key, fromCount);
	            }
	        }
	    }
	}
}
