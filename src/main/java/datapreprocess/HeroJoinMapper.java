package datapreprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HeroJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	
	private IntWritable outkey = new IntWritable();
	private Text heros = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		if (key.get() == 0l)
			return;
		String[] tokens = value.toString().split(",");
		outkey.set(Integer.parseInt(tokens[0]));
		heros.set(tokens[2] + "," + tokens[3]);
		context.write(outkey, heros);
	}

	
}
