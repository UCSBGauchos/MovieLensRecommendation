import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class Preprocess {
	
	public static class CountUserInfoMap extends MapReduceBase implements Mapper<Object, Text, LongWritable, PostingUser>{
		//public IntWritable one = new IntWritable(1);
		public long previousUserID = -1;
		public int totalRate = 0;
		public int ratingNumber = 0;
		public PostingUser userValue = new PostingUser();
		public float avgRating = 0;
		public ArrayList<MovieRating> movieRatingList = new ArrayList<MovieRating>();
		public void map(Object unusedInKey, Text inValue, OutputCollector<LongWritable, PostingUser> output, Reporter reporter) throws IOException{			
			//eachline get four elements
			String eachLine = inValue.toString();
			StringTokenizer token = new StringTokenizer(eachLine);
			long uID = Long.parseLong(token.nextToken());
			LongWritable userID = new LongWritable(uID);
			long mID = Long.parseLong(token.nextToken());
			LongWritable movieID = new LongWritable(mID);
			int r = Integer.parseInt(token.nextToken());
			IntWritable rating = new IntWritable(r);
			Long d = Long.parseLong(token.nextToken());
			LongWritable date = new LongWritable(d); 
			
			
			if(uID!=previousUserID){
				
			}else{
				
				movieRatingList.add(new MovieRating(mID, r));
			}
		}
	}
	
	public static class CountUserInfoReduce extends MapReduceBase implements Reducer<LongWritable, PostingUser, LongWritable, PostingUserArrayWritable>{
		public void reduce(LongWritable movieID, Iterator<PostingUser> nextUser, OutputCollector<LongWritable, PostingUserArrayWritable> output, Reporter reporter) throws IOException{
			ArrayList<PostingUser> users = new ArrayList<PostingUser>();
			while(nextUser.hasNext()){
				PostingUser hold = nextUser.next();
				users.add(new PostingUser(hold.userID, hold.avgRating, hold.rate));
			}
			PostingUser[] arrayUsers = new PostingUser[users.size()];
			arrayUsers = users.toArray(new PostingUser[users.size()]);
			Arrays.sort(arrayUsers);// sorting checked
			output.collect(movieID, new PostingUserArrayWritable(arrayUsers));
		}
	}

	
	public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(Preprocess.class);
	     conf.setJobName("preprocess");
	     conf.setMapOutputKeyClass(LongWritable.class);
	     conf.setMapOutputValueClass(PostingUser.class);
	     conf.setOutputKeyClass(LongWritable.class);
	     conf.setOutputValueClass(PostingUserArrayWritable.class);
	     conf.setMapperClass(CountUserInfoMap.class);
	     //conf.setCombinerClass(CountUserInfoReduce.class);
	     conf.setReducerClass(CountUserInfoReduce.class);
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     //conf.setOutputFormat(SequenceFileOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path("KNNInput"));
	     MainDriver.run(conf);
	   }
}
