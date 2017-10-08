package xyz.majin.ReverseIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RIRunner extends Configured implements Tool {
	/**
	 * �����ı�
	 * �����
	 * 		< word , FileName >
	 * @author majin
	 *
	 */
	private static class RIMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//����ļ���
			FileSplit split = (FileSplit) context.getInputSplit();
			String fileName = split.getPath().getName();

			//������еĵ���
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				String word = st.nextToken();
				context.write(new Text(word), new Text(fileName));
			}
		}
	}

	/**
	 * ���룺
	 * 		<word,(FileName,FileName����)>
	 * �����
	 * 		<word,"File1-->count1,File2-->count2����">
	 * @author majin
	 *
	 */
	private static class RIReducer extends Reducer<Text, Text, Text, Text> {
		private HashMap<String, Long> map = new HashMap<String, Long>();

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			//���map
			map.clear();

			String word = key.toString();

			//��filename���뵽map��ȥ�ز�ͳ��
			for (Text v : value) {
				String filename = v.toString();
				Long res = map.remove(filename);
				map.put(filename, res == null ? 1 : res + 1);
			}

			//��map�л�ȡͳ�ƽ����ƴ�ӳɽ��
			String res_value = "";
			Set<Entry<String, Long>> entrySet = map.entrySet();
			for (Entry<String, Long> entry : entrySet) {
				String file = entry.getKey();
				long count = entry.getValue();
				res_value += file + "-->" + count + ",";
			}

			res_value.substring(0, res_value.lastIndexOf(",")-1);//ȥ�����ģ�

			context.write(new Text(word), new Text(res_value));
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(RIRunner.class);

		//����Mapper�����KV
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		//����reduce�����KV
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//����Mapper��Reducer��
		job.setMapperClass(RIMapper.class);
		job.setReducerClass(RIReducer.class);

		//�����Ŀ¼����ʱ��ɾ�����Ŀ¼
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(arg0[1]))) {
			fileSystem.delete(new Path(arg0[1]));
		}

		//�����������Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RIRunner(), args);
		System.exit(res);
	}
}
