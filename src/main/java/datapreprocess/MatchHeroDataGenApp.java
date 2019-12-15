package datapreprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import datatype.MatchHeroWritable;

public class MatchHeroDataGenApp {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String jobName = "Match_Hero_Join";
		try {
			Job job = Job.getInstance(conf, jobName);
			job.setJarByClass(MatchHeroDataGenApp.class);
			
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatchJoinMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, HeroJoinMapper.class);
			
			// If the reducer input and output are different, mapoutput key and value should be specified here
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			// set Reducer
			job.setReducerClass(MatchHeroReducer.class);

			// Set OutputFormat classes
			job.setOutputFormatClass(TextOutputFormat.class);
			
			// Set the output key and value type
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(MatchHeroWritable.class);
			
			// Output path
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			// Exit out with 
			System.exit(job.waitForCompletion(true)? 0: 1);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
