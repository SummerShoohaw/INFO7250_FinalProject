package allyenemy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EnemyAnalysisApp {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String jobName = "Win rate again enemies";
		try {
			Job job = Job.getInstance(conf, jobName);
			job.setJarByClass(EnemyAnalysisApp.class);
			
			job.setMapperClass(EnemyMapper.class);
			job.setReducerClass(EnemyReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
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
