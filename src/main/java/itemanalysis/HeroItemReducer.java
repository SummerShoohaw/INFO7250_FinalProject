package itemanalysis;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HeroItemReducer extends Reducer<Text, Text, Text, Text>{

	private LinkedList<Integer> itemList;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		itemList = new LinkedList<Integer>();
		boolean winlose = false;
		String heroId = "null";
		for (Text t: values) {
			if (t.toString().startsWith("@")) {
				String[] tokens = t.toString().substring(1).split(":");
				winlose = tokens[0].equalsIgnoreCase("True");
				heroId = tokens[1];
			}
			else {
				itemList.add(Integer.parseInt(t.toString()));
			}
		}
		if (heroId.equals("null")) {
			System.out.println("null heroId detected, printing match_id and slot:\n\t");
			System.out.println(key.toString());
			System.out.println("\n");
		}
		String matchId = key.toString().split(":")[0];
		
		for (int item: itemList) {
			context.write(new Text(heroId), new Text(matchId + "," + winlose + "," + item));
		}
	}
	
}
