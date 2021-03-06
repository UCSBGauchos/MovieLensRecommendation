import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserNeighbourArrayWritable implements Writable{
	public int size;
	public UserNeighbour [] array;
	
	public UserNeighbourArrayWritable(){
		
	}
	
	public UserNeighbourArrayWritable(UserNeighbour[] values) {
		this.size = values.length;
		this.array = values;
	}

	
	public UserNeighbour [] getPosting(){
		return array;
	}
	
	public String toString(){
		StringBuffer buffer = new StringBuffer();
		for(int i=0; i<this.size; i++){
			buffer.append(array[i].toString()+",");
		}
		return buffer.toString();
	}
	
	
	//neighbour: mivieID weight
	public void readFields(DataInput in) throws IOException {
		this.size = in.readInt();
		this.array = new UserNeighbour[this.size];
		for (int i = 0; i < size; i++){
			array[i] = new UserNeighbour(in.readLong(), in.readFloat());
		}
	}	
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			this.array[i].write(out);
		}
	}
	
}
