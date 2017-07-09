package cn.zzw.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());

		// 注意：main方法所在的类一定要设置
		job.setJarByClass(WordCount.class);

		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.31.130:9000/data/input/kpi-test.log"));

		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.31.130:9000/data/output/0709"));

		// 提交
		job.waitForCompletion(true);
		
		System.out.println("Finish");
	}

}
