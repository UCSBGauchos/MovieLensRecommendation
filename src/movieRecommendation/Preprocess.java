import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class Preprocess {
	
	public static class CountUserInfoMap extends MapReduceBase implements Mapper<LongWritable, MovieRatingArrayWritable, LongWritable, PostingUser>{
		public LongWritable movieID = new LongWritable();
		public PostingUser userValue = new PostingUser();
		
		public long userID;
		public int ratingNumber = 0;
		public float totalRating;
		public float avgRating = 0;
		public void map(LongWritable key, MovieRatingArrayWritable value, OutputCollector<LongWritable, PostingUser> output, Reporter reporter) throws IOException{			
			totalRating = 0;
			userID = key.get();
			MovieRating [] array = value.getPosting();
			ratingNumber = array.length;
			for(int index = 0; index<ratingNumber; index++){
				totalRating+=array[index].rate;
			}
			avgRating = totalRating/ratingNumber;
			for(int index = 0; index<ratingNumber; index++){
				long mID = array[index].movieID;
				movieID.set(mID);
				userValue.set(userID, avgRating, array[index].rate);
				output.collect(movieID, userValue);
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
	     conf.setInputFormat(SequenceFileInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     //conf.setOutputFormat(SequenceFileOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	     MainDriver.run(conf);
	   }
}
