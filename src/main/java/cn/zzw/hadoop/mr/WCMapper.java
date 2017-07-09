package cn.zzw.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//receive data
		String line = value.toString();
		//切分数据
		String[] words = line.split(" ");
		for (String w : words) {
			context.write(new Text(w), new LongWritable(1));
		}
	}
	

}
