import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ItemNeighbour implements WritableComparable<ItemNeighbour>{
	public long movieID;
	public float weight;
	public ItemNeighbour(){
		//default initial funxtion
	}
	
	public ItemNeighbour(long _movieID, float _weight){
		this.movieID = _movieID;
		this.weight = _weight;
	}
	
	public int compareTo(ItemNeighbour other){
		if(this.weight<other.weight){
			return 1;
		}else if(this.weight>other.weight){
			return -1;
		}else{
			return 0;
		}
	}
	
	public String toString(){
		return movieID+" "+weight;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(movieID);
		out.writeFloat(weight);
	}

	public void readFields(DataInput in) throws IOException {
		movieID = in.readLong();
		weight = in.readFloat();
	}
}
