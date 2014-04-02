import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class UserPreprocess {
	public static class CountUserInfoMap extends MapReduceBase implements Mapper<LongWritable, MovieRatingArrayWritable, LongWritable, PostingMovie>{
		public LongWritable userID = new LongWritable();
		public PostingMovie movieValue = new PostingMovie();
		
		public long uID;
		public int ratingNumber = 0;
		public float totalRating;
		public float avgRating = 0;
		public void map(LongWritable key, MovieRatingArrayWritable value, OutputCollector<LongWritable, PostingMovie> output, Reporter reporter) throws IOException{			
			totalRating = 0;
			uID = key.get();
			MovieRating [] array = value.getPosting();
			ratingNumber = array.length;
			for(int index = 0; index<ratingNumber; index++){
				totalRating+=array[index].rate;
			}
			avgRating = totalRating/ratingNumber;
			for(int index = 0; index<ratingNumber; index++){
				long mID = array[index].movieID;
				userID.set(uID);
				movieValue.set(mID, avgRating, array[index].rate);
				output.collect(userID, movieValue);
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
	     JobConf conf = new JobConf(UserPreprocess.class);
	     conf.setJobName("UserPreprocess");
	     conf.setMapOutputKeyClass(LongWritable.class);
	     conf.setMapOutputValueClass(PostingMovie.class);
	     conf.setOutputKeyClass(LongWritable.class);
	     conf.setOutputValueClass(PostingMovieArrayWritable.class);
	     conf.setMapperClass(CountUserInfoMap.class);
	     //conf.setCombinerClass(CountUserInfoReduce.class);
	     conf.setReducerClass(CountUserInfoReduce.class);
	     conf.setInputFormat(SequenceFileInputFormat.class);
	     //conf.setOutputFormat(TextOutputFormat.class);
	     conf.setOutputFormat(SequenceFileOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path("Collect"));
	     //each time remove the outpt first
	     FileSystem.get(conf).delete(new Path("UserPreprocess"), true);
	     FileOutputFormat.setOutputPath(conf, new Path("UserPreprocess"));
	     MainDriver.run(conf);
}
	
}
