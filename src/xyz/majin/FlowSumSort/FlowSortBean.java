package xyz.majin.FlowSumSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowSortBean implements WritableComparable<FlowSortBean> {
	private String userId = null;
	private long flow = 0;

	//һ��Ҫд�ϲ��������Ĺ��캯�����ڷ����л�ʱ����ã������޷���������
	public FlowSortBean() {

	}

	public FlowSortBean(String userId, long flow) {
		this.userId = userId;
		this.flow = flow;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public long getFlow() {
		return flow;
	}

	public void setFlow(long flow) {
		this.flow = flow;
	}

	//����ķ����л�
	@Override
	public void readFields(DataInput in) throws IOException {
		this.userId = in.readUTF();
		this.flow = in.readLong();
	}

	//��������л�
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userId);
		out.writeLong(flow);
	}

	@Override
	public String toString() {
		return this.userId+" "+this.flow + "MB";
	}

	@Override
	public int compareTo(FlowSortBean o) {
		return this.flow > o.getFlow() ? 1 : -1;
	}
}
