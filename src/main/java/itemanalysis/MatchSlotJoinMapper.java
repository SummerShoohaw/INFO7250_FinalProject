package itemanalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatchSlotJoinMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		// tokens[0] --> match_id, tokens[1] --> match_info
		String[] subTokens = tokens[1].split(",");
		for (int i = 1; i < subTokens.length; i++) {
			int slot = -1;
			boolean winlose = false;
			if (i <= 5) {
				slot = i - 1;
				winlose = subTokens[0].equalsIgnoreCase("true");
			}
			else if (i <= 10) {
				slot = i + 122;
				winlose = subTokens[0].equalsIgnoreCase("false");
			}
//			System.out.println("In MatchJoinMapper----------------");
			Text outputkey = new Text(tokens[0] + ":" + slot);
//			System.out.println(outputkey.toString());
//			System.out.println("MatchJoinMapper End---------------\n");
			
			context.write(outputkey, new Text("@" + winlose + ":" + subTokens[i]));
		}
	}
	
}
