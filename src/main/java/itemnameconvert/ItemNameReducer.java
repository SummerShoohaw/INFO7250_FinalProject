package itemnameconvert;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import datatype.HeroItemWinRateWritable;

public class ItemNameReducer extends Reducer<Text, Text, Text, HeroItemWinRateWritable>{

	// key -> itemId, value -> item name
	HashMap<Integer, String> itemNameMap;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, HeroItemWinRateWritable>.Context context) throws IOException, InterruptedException {
		String heroName = "null";
		HeroItemWinRateWritable outputvalue = new HeroItemWinRateWritable();
		for (Text t: values) {
			if (t.toString().startsWith("@"))
				heroName = t.toString().substring(1);
			else {
				String[] tokens = t.toString().split(",");
				int matchId = Integer.parseInt(tokens[0]);
				boolean winlose = tokens[1].equalsIgnoreCase("true");
				int itemId = Integer.parseInt(tokens[2]);
				outputvalue.addItem(itemId, winlose, matchId);
			}
		}
		
		context.write(new Text(heroName), outputvalue);
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, HeroItemWinRateWritable>.Context context)
			throws IOException, InterruptedException {
		if (HeroItemWinRateWritable.getItemNameMap() != null)
			return;
		itemNameMap = new HashMap<Integer, String>();
		readItemName(context.getConfiguration());
	}
	
	private void readItemName(Configuration conf) {
		Path path = new Path("hdfs:///dota2dataset/item_ids.csv");
		try {
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = null;
			while((line = br.readLine()) != null) {
				if (line.startsWith("item_id"))
					continue;
				String[] tokens = line.split(",");
				System.out.println(line);
				itemNameMap.put(Integer.parseInt(tokens[0]), tokens[1]);
			}
			HeroItemWinRateWritable.setItemNameMap(itemNameMap);
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
