import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserNeighbour implements WritableComparable<UserNeighbour>{
	public long userID;
	public float weight;
	public UserNeighbour(){
		//default initial funxtion
	}
	
	public UserNeighbour(long _userID, float _weight){
		this.userID = _userID;
		this.weight = _weight;
	}
	
	public int compareTo(UserNeighbour other){
		if(this.weight<other.weight){
			return 1;
		}else if(this.weight>other.weight){
			return -1;
		}else{
			return 0;
		}
	}
	
	public String toString(){
		return userID+" "+weight;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(userID);
		out.writeFloat(weight);
	}

	public void readFields(DataInput in) throws IOException {
		userID = in.readLong();
		weight = in.readFloat();
	}
}

