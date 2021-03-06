package my.tutorial.lesson4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MyJob extends Configured implements Tool {

	static final Log LOG = LogFactory.getLog(MyJob.class);
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			if(key.get() > 0){
				String fields[] = value.toString().split(",", -20);
				String country = fields[4];
				String numClaims = fields[9];
				//LOG.info("num of Claims is " + numClaims);
				if(!numClaims.isEmpty() && isDouble(numClaims)); {
					context.write(new Text(country), new Text(numClaims + ",1"));
				}
			}
		}
		
		public static boolean isDouble(String s) {
		    try { 
		    	Double.parseDouble(s); 
		    } catch(NumberFormatException e) { 
		        return false; 
		    }
		    // only got here if we didn't return false
		    return true;
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double sum = 0;
			int count = 0;
			for(Text val:values) {
				LOG.info("value is " + val);
				String fields[] = val.toString().split(",");
				try { 
					sum += Double.parseDouble(fields[0]);
					count += Integer.parseInt(fields[1]);
				} catch(NumberFormatException e) { 
				}
			}
			context.write(key, new DoubleWritable(sum/count));
		}
	}
	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double sum = 0;
			int count = 0;
			//private final static IntWritable sum = new IntWritable(1);
			//private final static IntWritable count = new IntWritable(1);
			for(Text val:values){
				String fields[] = val.toString().split(",");
				sum += Double.parseDouble(fields[0]);
				count += Integer.parseInt(fields[1]);
			}
			context.write(key, new Text(sum + "," + count));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf, "myjob");
		job.setJarByClass(MyJob.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combine.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		System.exit(job.waitForCompletion(true)?0:1);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);       
        System.exit(res);		
	}

}
