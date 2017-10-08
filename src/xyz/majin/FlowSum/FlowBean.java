package xyz.majin.FlowSum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	private String userId = "";
	private long up = 0;
	private long down = 0;
	private long sum = 0;

	//一定要写上不带参数的构造函数，在反序列化时会调用，否则无法创建对象
	public FlowBean() {

	}

	public FlowBean(String userId, long up, long down) {
		this.userId = userId;
		this.up = up;
		this.down = down;
		this.sum = up + down;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public long getUp() {
		return up;
	}

	public void setUp(long up) {
		this.up = up;
	}

	public long getDown() {
		return down;
	}

	public void setDown(long down) {
		this.down = down;
	}

	public long getSum() {
		return sum;
	}

	public void setSum(long sum) {
		this.sum = sum;
	}

	//对象的反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.userId = in.readUTF();
		this.up = in.readLong();
		this.down = in.readLong();
		this.sum = in.readLong();
	}

	//对象的序列化
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userId);
		out.writeLong(up);
		out.writeLong(down);
		out.writeLong(sum);
	}

	@Override
	public String toString() {
		return this.up + "\t" + this.down + "\t" + this.sum;
	}

}
