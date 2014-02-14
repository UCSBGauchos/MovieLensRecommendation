package movieRecommendation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MovieRateType implements Writable{
	public int movieID;
	public int rate;
	
	public MovieRateType(int _movieID, int _rate){
		this.movieID = _movieID;
		this.rate = _rate;
	}
	
	public void readFields(DataInput in) throws IOException{
		this.movieID = in.readInt();
		this.rate = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeInt(movieID);
		out.writeInt(rate);
	}
}
