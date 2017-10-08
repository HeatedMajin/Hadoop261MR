package xyz.majin.FlowGroup;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * ���û����ֻ��Ű��յ������飬
 * @author majin
 *
 * @param <KEY> �û��ֻ���
 * @param <VALUE> ������Ϣ�ķ�װ
 */
public class FlowPartition<KEY, VALUE> extends Partitioner<KEY, VALUE> {
	//keyΪ�û��ֻ���ǰ��λ��value���û����ڵ����ı��
	private static Map<String, Integer> map = new HashMap<String, Integer>();

	static {
		//����Ӧ���Ǵ����ݿ��ж�ȡ�ֻ��͵����Ķ�Ӧ������ʡ��
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
