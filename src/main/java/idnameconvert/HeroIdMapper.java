package idnameconvert;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HeroIdMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		if (key.get() == 0l)
			return;
		String[] tokens = value.toString().split(",");
		// tokens[1] --> Id, tokens[2] --> name
		context.write(NullWritable.get(), new Text("@" + tokens[1] + ":" + tokens[2]));
	}
	

}
