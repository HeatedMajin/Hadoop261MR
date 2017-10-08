package xyz.majin.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//mapperÊä³ökey£¬mapperÊä³övalue
public class WcReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context content)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : values) {
			sum = sum + i.get();
		}
		content.write(key, new IntWritable(sum));
	}

}
