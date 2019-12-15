package itemnameconvert;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HeroNameMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		if (key.get() == 0l)
			return;
		context.write(new Text(tokens[1]), new Text("@" + tokens[2]));
	}

}
