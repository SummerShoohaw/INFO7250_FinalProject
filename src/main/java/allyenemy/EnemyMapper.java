package allyenemy;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EnemyMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		tokens = tokens[1].split(",");
		// this game is invalid, some player abandoned the game
		if (tokens.length < 11)
			return;
		String winlose = tokens[0].equals("true")? "win": "lose";
		for (int i = 1; i <= 5; i++)
		{
			for (int j = 6; j <= 10; j++) {
				IntWritable hero1 = new IntWritable(Integer.parseInt(tokens[i]));
				IntWritable hero2 = new IntWritable(Integer.parseInt(tokens[j]));
				context.write(hero1, new Text(winlose + "," + hero2));
				context.write(hero2, new Text(winlose + "," + hero1));
			}
		}
	}
	
}
