package xyz.majin.FlowSumSort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 对上一个结果进行排序
 * 输入：用户	up down sum
 * @author majin
 *
 */
public class FlowSortRunner extends Configured implements Tool {
	private static class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String arr[] = line.split("\t");
			String userId = arr[0];
			long sum = Long.parseLong(arr[3]);
			FlowSortBean bean = new FlowSortBean(userId, sum);

			context.write(bean, NullWritable.get());
		}
	}

	private static class FlowSortReducer extends Reducer<FlowSortBean, NullWritable, FlowSortBean, NullWritable> {

		@Override
		protected void reduce(FlowSortBean key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());

		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowSortRunner.class);

		job.setMapperClass(FlowSortMapper.class);
		job.setReducerClass(FlowSortReducer.class);

		job.setMapOutputKeyClass(FlowSortBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(FlowSortBean.class);
		job.setOutputValueClass(NullWritable.class);

		//当输出目录存在时，删除这个目录
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(arg0[1]))) {
			fileSystem.delete(new Path(arg0[1]));
		}

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FlowSortRunner(), args);
		System.exit(res);
	}
}
