package idnameconvert;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HeroIdNameReducer extends Reducer<NullWritable, Text, Text, Text>{

	HashMap<Integer, String> idNameMap;
	LinkedList<String> toConvert;
	
	
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		idNameMap = new HashMap<Integer, String>();
		toConvert = new LinkedList<String>();
		for (Text t: values) {
			if (t.toString().startsWith("@")) {
				String[] tokens = t.toString().split(":");
				idNameMap.put(Integer.parseInt(tokens[0].substring(1)), tokens[1]);
			}
			else
				toConvert.add(t.toString());
		}
		idNameMap.put(0, "NullHero");
		
		for (String t: toConvert) {
			System.out.println(t);
			String[] tokens = t.toString().split(",");
			String mainHeroName = idNameMap.get(Integer.parseInt(tokens[0]));
			StringBuffer sb = new StringBuffer();
			for (int i = 1; i < tokens.length; i++) {
				String enemyName = idNameMap.get(Integer.parseInt(tokens[i]));
				sb.append(enemyName);
				sb.append(",");
			}
			context.write(new Text(mainHeroName), new Text(sb.toString()));
		}
	}
	
}
