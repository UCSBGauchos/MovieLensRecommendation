package movieRecommendation;

import java.io.DataInput;
import java.io.DataOutput;

public class UserPosting implements WritableComparable<UserPosting>{
	public long userID;
	public float avgRating;
	public int rate;
	
	public UserPosting(long _userID, float _avgRating, int _rate){
		this.userID = userID;
		this.avgRating = _avgRating;
		this.rate = _rate;
	}
	
	public void set(long _userID, float _avgRating, int _rate){
		this.userID = _userID;
		this.avgRating = _avgRating;
		this.rate = _rate;
	}
	public String toString(){
		return userID+" "+avgRating+" "+rate;
	}
	public void write(DataOutput out) throws IOExpection{
		out.writeLong(userID);
		out.writeFloat(avgRating);
		out.writeInt(rate);
	}
	public void readFields(DataInput in) throws IOException{
		this.userID = in.readLong();
		this.avgRating = in.readFloat();
		this.rate = in.readInt();
	}
}
