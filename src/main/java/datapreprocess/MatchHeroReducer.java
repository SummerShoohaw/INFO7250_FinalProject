package datapreprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import datatype.MatchHeroWritable;

public class MatchHeroReducer extends Reducer<IntWritable, Text, IntWritable, MatchHeroWritable>{

	private MatchHeroWritable matchHeroData;
	@Override
	public void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, IntWritable, MatchHeroWritable>.Context context)
			throws IOException, InterruptedException {
		matchHeroData = new MatchHeroWritable();
		for (Text text: values) {
			if (text.toString().equalsIgnoreCase("true") || text.toString().equalsIgnoreCase("false") ) {
				matchHeroData.setRadiantWin(text.toString().equalsIgnoreCase("true"));
			}
			else {
				String[] tokens = text.toString().split(",");
				// This hero is radiant
				if (Integer.parseInt(tokens[1]) <= 4) {
					matchHeroData.addRadiantHeros(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
				}
				// this hero is dire
				else {
					matchHeroData.addDireHeros(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
				}
				
				// Debugging
//				if (matchHeroData.getDireHeroCount() > 5 || matchHeroData.getRadiantHeroCount() > 5) {
//					throw new RuntimeException("Hero count bigger than 5, match_id: " + key.toString() + "Hero: " + ) 
//				}
			}
		}
		
		context.write(key, matchHeroData);
	}

	
}
