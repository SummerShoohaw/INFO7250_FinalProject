package datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatchHeroWritable implements Writable{
	
	private boolean radiantWin;
	private final int[] radiantHeros = new int[5];
	private final int[] direHeros = new int[5];
	
	public boolean isRadiantWin() {
		return radiantWin;
	}

	public void setRadiantWin(boolean radiantWin) {
		this.radiantWin = radiantWin;
	}

//	public Set<Integer> getRadiantHeros() {
//		return radiantHeros;
//	}
//
//	public Set<Integer> getDireHeros() {
//		return direHeros;
//	}
	
//	public int getRadiantHeroCount() {
//		return radiantHeros.size();
//	}
//	
//	public int getDireHeroCount() {
//		return direHeros.size();
//	}
	
	public void addRadiantHeros(int hero, int slot) {
		radiantHeros[slot] = hero;
	}
	
	public void addDireHeros(int hero, int slot) {
		direHeros[slot - 128] = hero;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(radiantWin);
		for (int i: radiantHeros) {
			out.write(i);
		}
		for (int i: direHeros) {
			out.write(i);
		}
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		return;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(radiantWin);
		sb.append(",");
		for (int i: radiantHeros) {
			sb.append(i);
			sb.append(",");
		}
		for (int i: direHeros) {
			sb.append(i);
			sb.append(",");
		}
		return sb.toString();
	}

}
