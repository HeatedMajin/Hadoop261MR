package xyz.majin.wc;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<����key������,����value�����ͣ�>
//ǰ����һ���ǹ̶���
//�����������е��±꣬�ļ������ݣ����ʣ�����
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
