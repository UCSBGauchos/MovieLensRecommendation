package movieRecommendation;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CountUserInfoReduce extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable>{
	public void reduce(LongWritable inputKey, Iterator<IntWritable> inputValues, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException{
		int sum = 0;
		while(inputValues.hasNext()){
			sum += inputValues.next().get();
		}
		output.collect(inputKey, new IntWritable(sum));
	}
}
