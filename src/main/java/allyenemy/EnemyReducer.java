package allyenemy;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class EnemyReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

	private Map<Integer, Integer> totalGamesMap;
	private Map<Integer, Integer> totalWinsMap;
	
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		totalGamesMap = new HashMap<Integer, Integer>();
		totalWinsMap = new HashMap<Integer, Integer>();
		
		// Calculate number of games and number of games that key wins
		for (Text t: values) {
			String[] tokens = t.toString().split(",");
			int enemyId = Integer.parseInt(tokens[1]);
			// tokens[0] -> win or lose
			// tokens[1] -> enemy hero_id
			int totalGames = totalGamesMap.getOrDefault(enemyId, 0);
			totalGamesMap.put(enemyId, ++totalGames);
			
			int wins = totalWinsMap.getOrDefault(enemyId, 0);
			if (tokens[0].equals("win"))
				totalWinsMap.put(enemyId, ++wins);
		}
		
		
		// Sort
		LinkedList<Integer> enemyIds = new LinkedList<Integer>(totalGamesMap.keySet());
		enemyIds.sort(new WinRateComparator(totalGamesMap, totalWinsMap));
		
		// Output
		StringBuffer sb = new StringBuffer();
		for (int heroId: enemyIds) {
			if (totalGamesMap.get(heroId) < 5)
				continue;
			sb.append(heroId);
			sb.append(",");
		}
		context.write(key, new Text(sb.toString()));
	}
	
	class WinRateComparator implements Comparator<Integer>{
		
		private Map<Integer, Integer> totalMap;
		private Map<Integer, Integer> winsMap;
		
		public WinRateComparator(Map<Integer, Integer> totalMap, Map<Integer, Integer> winsMap) {
			this.totalMap = totalMap;
			this.winsMap = winsMap;
		}

		public int compare(Integer i, Integer j) {
			int total1 = totalMap.get(i);
			int total2 = totalMap.get(j);
			int wins1 = winsMap.getOrDefault(i, 0);
			int wins2 = winsMap.getOrDefault(j, 0);
			double winRate1 = wins1 * 1.0 / total1;
			double winRate2 = wins2 * 1.0 / total2;
			if (winRate1 > winRate2)
				return -1;
			else if (winRate1 < winRate2)
				return 1;
			else return 0;
		}
		
	}
}
