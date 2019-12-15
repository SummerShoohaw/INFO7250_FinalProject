package datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;


public class HeroItemWinRateWritable implements Writable{

	// key ->itemId, value -> number of wins when hero have this item
	private HashMap<Integer, Integer> itemWinsMap;
	
	// key -> itemId, value -> total number of games that hero have this item
	private HashMap<Integer, Integer> itemTotalMap;
	
	// key -> itemId, value -> win rate of this item
	private HashMap<Integer, Double> itemWinRateMap;
	
	private List<Map.Entry<Integer, Double>> itemWinRateList;
	
	private static HashMap<Integer, String> itemNameMap = null;
	
	private int matchCount;
	private HashSet<Integer> matchIdSet;
	
	public HeroItemWinRateWritable() {
		itemWinsMap = new HashMap<Integer, Integer>();
		itemTotalMap = new HashMap<Integer, Integer>();
		itemWinRateMap = new HashMap<Integer, Double>();
		matchIdSet = new HashSet<Integer>();
		matchCount = 0;
	}
	
	public static HashMap<Integer, String> getItemNameMap() {
		return itemNameMap;
	}

	public static void setItemNameMap(HashMap<Integer, String> itemNameMap) {
		HeroItemWinRateWritable.itemNameMap = itemNameMap;
	}

	public void addItem(int itemId, boolean winlose, int matchId) {
		if (!matchIdSet.contains(matchId)) {
			matchCount++;
			matchIdSet.add(matchId);
		}			
		if (winlose) {
			int numOfWins = itemWinsMap.getOrDefault(itemId, 0);
			itemWinsMap.put(itemId, ++numOfWins);
		}
		
		int numOfGames = itemTotalMap.getOrDefault(itemId, 0);
		itemTotalMap.put(itemId, ++numOfGames);
	}
	
	private void itemWinRateGenerater() {
		for (int itemId: itemTotalMap.keySet()) {
			double winrate = itemWinsMap.getOrDefault(itemId, 0) * 1.0 / itemTotalMap.get(itemId);
			itemWinRateMap.put(itemId, winrate);
		}
	}
	
	private void sortWinRateMap() {
		itemWinRateList = new LinkedList<Map.Entry<Integer, Double>>(itemWinRateMap.entrySet());
		itemWinRateList.sort(new Comparator<Map.Entry<Integer, Double>>(){
			public int compare(Entry<Integer, Double> entry1, Entry<Integer, Double> entry2) {
				double winrate1 = entry1.getValue();
				double winrate2 = entry2.getValue();
				if (winrate1 > winrate2)
					return -1;
				else if (winrate1 < winrate2)
					return 1;
				else return 0;
			}
		});
	}
	
	@Override
	public String toString() {
		itemWinRateGenerater();
		sortWinRateMap();
		StringBuffer sb = new StringBuffer();
		for (Map.Entry<Integer, Double> entry: itemWinRateList) {
			sb.append(itemNameMap.get(entry.getKey()));
			sb.append(":");
			double itemPurchaseRate = itemTotalMap.get(entry.getKey()) * 1.0 / matchCount;
			sb.append("PR" + String.format("%.2f", itemPurchaseRate * 100) + "%");
			sb.append(":");
			sb.append("WR" + String.format("%.2f", (entry.getValue()) * 100) + "%");
			sb.append(",");
		}
		return sb.toString();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.write(0);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		in.readInt();
	}

}
