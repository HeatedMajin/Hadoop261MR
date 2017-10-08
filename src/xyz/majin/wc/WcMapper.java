package xyz.majin.wc;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<输入key的类型,输入value的类型，>
//前两个一般是固定的
//内容在内容中的下标，文件的内容，单词，次数
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		while(st.hasMoreTokens()){
			String temp =st.nextToken();
			context.write(new Text(temp),new IntWritable(1));
		}
	}

	
}
