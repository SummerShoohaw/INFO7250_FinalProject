package idnameconvert;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IdNameConvertApp {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String jobName = "Id_Name_Convertion";
		try {
			Job job = Job.getInstance(conf, jobName);
			job.setJarByClass(IdNameConvertApp.class);
			
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, HeroIdMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AnalysisResultMapper.class);
			
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(HeroIdNameReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			
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
