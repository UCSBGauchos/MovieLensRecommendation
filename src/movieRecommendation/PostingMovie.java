import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PostingMovie implements WritableComparable<PostingMovie	>{
	public long movieID;
	public float avgRating;
	public int rate;

	public PostingMovie() {}

	public PostingMovie(long _movieID, float avgR, int _rate) {
		this.movieID = _movieID;
		this.avgRating = avgR;
		this.rate = _rate;
	}

	public void set(long _movieID, float avgR, int _rate) {
		this.movieID = _movieID;
		this.avgRating = avgR;
		this.rate = _rate;
	}

	@Override
	//this function is to make the result visible
	public String toString() {
		return movieID + " " + avgRating + " " + rate;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(movieID);
		out.writeFloat(avgRating);
		out.writeInt(rate);
	}

	public void readFields(DataInput in) throws IOException {
		this.movieID = in.readLong();
		this.avgRating = in.readFloat();
		this.rate = in.readInt();
	}

	public int compareTo(PostingMovie other) {
		if (this.movieID < other.movieID)
			return -1;
		else if (this.movieID > other.movieID)
			return 1;
		else
			return 0;
	}

}
