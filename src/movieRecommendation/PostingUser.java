import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PostingUser implements WritableComparable<PostingUser>{
	public long userID;
	public float avgRating;
	public int rate;

	public PostingUser() {}

	public PostingUser(long _userID, float avgR, int _rate) {
		this.userID = _userID;
		this.avgRating = avgR;
		this.rate = _rate;
	}

	public void set(long _userID, float avgR, int _rate) {
		this.userID = _userID;
		this.avgRating = avgR;
		this.rate = _rate;
	}

	@Override
	public String toString() {
		return userID + " " + avgRating + " " + rate;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(userID);
		out.writeFloat(avgRating);
		out.writeInt(rate);
	}

	public void readFields(DataInput in) throws IOException {
		this.userID = in.readLong();
		this.avgRating = in.readFloat();
		this.rate = in.readInt();
	}

	public int compareTo(PostingUser other) {
		if (this.userID < other.userID)
			return -1;
		else if (this.userID > other.userID)
			return 1;
		else
			return 0;
	}

}
