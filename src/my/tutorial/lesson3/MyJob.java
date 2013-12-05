package my.tutorial.lesson3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyJob extends Configured implements Tool {
	/*
	 * input key must be LongWritable.
	 */
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
    		String[] citation = value.toString().split(",");
    		context.write(new Text(citation[1]), new Text(citation[0]));
    	}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
    	public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
    		int count = 0;
    		for (Text val:values) {
    			val.toString();
    			count++;
    		}
    		context.write(key, new IntWritable(count));
    	}
    }

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf, "myjob");
		job.setJarByClass(MyJob.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reducer.class);
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

