import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class CountUserMain {
	
	public static class CountUserInfoMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable>{
		public IntWritable one = new IntWritable(1);
		public void map(LongWritable unusedInKey, Text inValue, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException{
			String eachLine = inValue.toString();
			StringTokenizer token = new StringTokenizer(eachLine, " |\t");
	
			long uID = Long.parseLong(token.nextToken());
			LongWritable userID = new LongWritable(uID);
			long iID = Long.parseLong(token.nextToken());
			LongWritable itemID = new LongWritable(iID);
			int r = Integer.parseInt(token.nextToken());
			IntWritable rating = new IntWritable(r);
			Long d = Long.parseLong(token.nextToken());
			LongWritable date = new LongWritable(d); 
			output.collect(userID, one);
		}
	}
	
	public static class CountUserInfoReduce extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable>{
		public void reduce(LongWritable inputKey, Iterator<IntWritable> inputValues, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException{
			int sum = 0;
			while(inputValues.hasNext()){
				sum += inputValues.next().get();
			}
			output.collect(inputKey, new IntWritable(sum));
		}
	}

	
	public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(CountUserMain.class);
	     conf.setJobName("CountUser");
	     conf.setOutputKeyClass(LongWritable.class);
	     conf.setOutputValueClass(IntWritable.class);
	     conf.setMapperClass(CountUserInfoMap.class);
	     conf.setCombinerClass(CountUserInfoReduce.class);
	     conf.setReducerClass(CountUserInfoReduce.class);
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	     JobClient.runJob(conf);
	   }
}
