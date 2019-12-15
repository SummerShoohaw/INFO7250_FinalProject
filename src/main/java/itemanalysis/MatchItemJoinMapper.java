package itemanalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatchItemJoinMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (key.get() == 0l)
			return;
		String[] tokens = value.toString().split(",");
		// tokens[0] --> item_id, tokens[2] --> player_slot, tokens[3] -->match_id
//		System.out.println("In MatchItemJoinMapper===================");
//		System.out.println(tokens[2] + ":" + tokens[3]);
//		System.out.println("MatchItemJoinMapper End =================");
		
		context.write(new Text(tokens[3] + ":" + tokens[2]), new Text(tokens[0]));
	}
	
	
}
