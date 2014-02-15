package movieRecommendation;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.w3c.dom.Text;

public class CountUserInfoMap extends MapReduceBase implements Mapper<Object, Text, LongWritable, IntWritable>{
	public IntWritable one = new IntWritable(1);
	public void map(Object unusedInKey, Text inValue, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException{
		String eachLine = inValue.toString();
		StringTokenizer token = new StringTokenizer(eachLine, " |\t");
		while(token.hasMoreTokens()){
			long uID = Long.parseLong(token.nextToken());
			LongWritable userID = new LongWritable(uID); 
			output.collect(userID, one);
		}
	}
}