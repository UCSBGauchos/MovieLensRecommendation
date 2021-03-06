import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PostingMovieArrayWritable implements Writable{
	public int size = 0;
	public PostingMovie [] array;
	
	public PostingMovieArrayWritable(){}
	
	public PostingMovieArrayWritable(PostingMovie[] values){
		this.size = values.length;
		this.array = values;
	}
	
	public PostingMovie[] getPosting() {
		return array;
	}
	
	public void readFields(DataInput in) throws IOException{
		this.size = in.readInt();
		this.array = new PostingMovie[this.size];
		for(int i=0; i<size; i++){
			this.array[i] = new PostingMovie(in.readLong(), in.readInt(), in.readInt());
		}
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeInt(size);
		for(int i=0; i<size; i++){
			this.array[i].write(out);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder();
		for (int i = 0; i < size; i++)
			bld.append("| " + array[i].toString() + ", ");
		return bld.toString();
	}
}
