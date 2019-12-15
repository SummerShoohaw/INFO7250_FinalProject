package idnameconvert;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisResultMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		// tokens[0] --> main heroId
		// tokens[1] --> enemy heroId list
		context.write(NullWritable.get(), new Text(tokens[0] + "," + tokens[1]));
	}

}
