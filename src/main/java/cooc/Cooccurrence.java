package cooc;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/**
 * User: Bill Bejeck
 * Date: 9/10/12
 * Time: 9:42 PM
 */
public class Cooccurrence extends Configured implements Tool{
	
	public int run(String[] args) throws Exception{
		//Create job instance
		Job job = new Job(getConf());
		
		//Configure job
		job.setJarByClass(Cooccurrence.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
		
		if(args[0].equalsIgnoreCase("pairs")){
			job.setJobName("Pairs-cooccurrence");
			job.setOutputKeyClass(WordPair.class);
			job.setOutputValueClass(IntWritable.class);
			FileOutputFormat.setOutputPath(job, new Path("pairs"));
            job.setMapperClass(Pairs.PairsOccurrenceMapper.class);
            job.setReducerClass(Pairs.PairsReducer.class);
            job.setCombinerClass(Pairs.PairsReducer.class); 
		}
		else if(args[0].equalsIgnoreCase("stripes")){
			job.setJobName("Stripes-cooccurrence");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(myMapWritable.class);
			FileOutputFormat.setOutputPath(job, new Path("stripes"));
            job.setMapperClass(Stripes.StripesOccurrenceMapper.class);
            job.setReducerClass(Stripes.StripesReducer.class);
            job.setCombinerClass(Stripes.StripesReducer.class); 
		}
         					
		//Final steps
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

    public static void main(String[] args) throws Exception {
    	int ret = ToolRunner.run(new Cooccurrence(), args);
    	System.exit(ret);
    }


}
