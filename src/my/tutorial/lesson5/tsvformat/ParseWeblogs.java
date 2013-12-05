package my.tutorial.lesson5.tsvformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParseWeblogs extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		int returnCode = ToolRunner.run(new ParseWeblogs(), args);
		System.exit(returnCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		Job weblogJob = new Job(conf);
		weblogJob.setJarByClass(ParseWeblogs.class);
		weblogJob.setJobName("weblog transformer");
		weblogJob.setNumReduceTasks(0);
		weblogJob.setMapperClass(CLFMapper.class);
		weblogJob.setMapOutputKeyClass(Text.class);
		weblogJob.setMapOutputValueClass(Text.class);
		weblogJob.setOutputKeyClass(Text.class);
		weblogJob.setOutputValueClass(Text.class);
		weblogJob.setInputFormatClass(TextInputFormat.class);
		weblogJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(weblogJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(weblogJob, new Path(args[1]));
		
		
		if(weblogJob.waitForCompletion(true)){
			return 0;
		}
		return 1;
	}

}
