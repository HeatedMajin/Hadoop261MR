package xyz.majin.FlowGroup;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 对用户的手机号按照地区分组，
 * @author majin
 *
 * @param <KEY> 用户手机号
 * @param <VALUE> 流量信息的封装
 */
public class FlowPartition<KEY, VALUE> extends Partitioner<KEY, VALUE> {
	//key为用户手机的前三位，value是用户所在地区的编号
	private static Map<String, Integer> map = new HashMap<String, Integer>();

	static {
		//这里应该是从数据库中读取手机和地区的对应表，这里省略
		map.put("170", 0);
		map.put("171", 1);
		map.put("172", 2);
		map.put("173", 3);
		map.put("174", 4);
		map.put("175", 5);

	}

	@Override
	public int getPartition(KEY key, VALUE value, int numReduce) {
		Integer res = map.get(key.toString().substring(0, 3));

		return res == null ? 6 : res;
	}

}
