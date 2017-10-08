package xyz.majin.FlowGroup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowGroupRunner extends Configured implements Tool {
	private static class FlowGroupMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\t");
			long up = Long.parseLong(words[3]);
			long down = Long.parseLong(words[4]);
			String userID = words[1];

			FlowBean bean = new FlowBean(userID, up, down);
			context.write(new Text(userID), bean);
		}
	}

	private static class FlowGroupReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		@Override
		protected void reduce(Text key, Iterable<FlowBean> beans, Context context)
				throws IOException, InterruptedException {
			long sum_up = 0;
			long sum_down = 0;
			for (FlowBean bean : beans) {
				sum_down += bean.getDown();
				sum_up += bean.getUp();
			}
			FlowBean flowBean = new FlowBean(key.toString(), sum_up, sum_down);
			context.write(key, flowBean);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowGroupRunner.class);

		//设置Mapper输出的KV
		job.setMapOutputValueClass(FlowBean.class);
		job.setMapOutputKeyClass(Text.class);

		//设置reduce输出的KV
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		//设置Mapper和Reducer类
		job.setMapperClass(FlowGroupMapper.class);
		job.setReducerClass(FlowGroupReducer.class);

		/**** 指定 分组的方式 ****/
		job.setPartitionerClass(FlowPartition.class);
		/**** 指定 reduce的数量 ****/
		job.setNumReduceTasks(7);

		//当输出目录存在时，删除这个目录
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(arg0[1]))) {
			fileSystem.delete(new Path(arg0[1]));
		}

		//设置输入输出目录
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FlowGroupRunner(), args);
		System.exit(res);
	}
}
