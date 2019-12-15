package itemnameconvert;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import datatype.HeroItemWinRateWritable;

public class ItemNameConvertionApp {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String jobName = "Item_Name_Converter";
		try {
			Job job = Job.getInstance(conf, jobName);
			job.setJarByClass(ItemNameConvertionApp.class);
			// cache item map file
			job.addCacheFile(new Path(args[0]).toUri());
			
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, HeroNameMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ItemNameMapper.class);
			
			job.setReducerClass(ItemNameReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(HeroItemWinRateWritable.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileOutputFormat.setOutputPath(job, new Path(args[3]));
			
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
