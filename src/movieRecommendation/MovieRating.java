import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MovieRating implements WritableComparable<MovieRating>{
	public long movieID;
	public int rate;

	public MovieRating() {}

	public MovieRating(long _movieID, int _rate) {
		this.movieID = _movieID;
		this.rate = _rate;
	}
	
	public void set(long _movieID, int _rate) {
		this.movieID = _movieID;
		this.rate = _rate;
	}
	
	@Override
	public String toString() {
		return movieID + " " + rate;
	}

	public void readFields(DataInput in) throws IOException {
		this.movieID = in.readLong();
		this.rate = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(movieID);
		out.writeInt(rate);
	}
	
	public int compareTo(MovieRating other) {
		if (this.movieID < other.movieID)
			return -1;
		else if (this.movieID > other.movieID)
			return 1;
		else
			return 0;
	}

}
