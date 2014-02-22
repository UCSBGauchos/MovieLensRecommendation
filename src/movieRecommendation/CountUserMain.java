import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class CountUserMain {
	
	public static class CountUserInfoMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, PostingUser>{
		//public IntWritable one = new IntWritable(1);
		public long previousUserID = -1;
		public int totalRate;
		public int ratingNumber = 0;
		public PostingUser userValue = new PostingUser();
		public float avgRating = 0;
		public ArrayList<MovieRating> movieRatingList = new ArrayList<MovieRating>();
		public void map(LongWritable unusedInKey, Text inValue, OutputCollector<LongWritable, PostingUser> output, Reporter reporter) throws IOException{
			String eachLine = inValue.toString();
			StringTokenizer token = new StringTokenizer(eachLine, " |\t");
			long uID = Long.parseLong(token.nextToken());
			LongWritable userID = new LongWritable(uID);
			long mID = Long.parseLong(token.nextToken());
			LongWritable movieID = new LongWritable(mID);
			int r = Integer.parseInt(token.nextToken());
			IntWritable rating = new IntWritable(r);
			Long d = Long.parseLong(token.nextToken());
			LongWritable date = new LongWritable(d); 
			//when uID != previousone, a new user section begin
			//else just continue one user section
			if(uID!=previousUserID){
				int currentUserNum = movieRatingList.size();
				//if curNum = 0, just begin, just initial the attributes
				//if curNum!= 0, means one user end, need to calculate the avg of the previous user, add ont outout case, and then clear and go to the next one
				if(currentUserNum == 0){
					totalRate = 0;
					ratingNumber = 0;
					movieRatingList.clear();
					totalRate+=r;
					movieRatingList.add(new MovieRating(mID, r));
					previousUserID = uID;
				}else if(currentUserNum != 0){
					avgRating = totalRate/currentUserNum;
					for(int i=0; i<currentUserNum; i++){
						LongWritable movieIDKey = new LongWritable(movieRatingList.get(i).movieID);
						userValue.set(previousUserID, avgRating, movieRatingList.get(i).rate);
						output.collect(movieIDKey, userValue);
					}
					totalRate = 0;
					ratingNumber = 0;
					movieRatingList.clear();
					totalRate+=r;
					movieRatingList.add(new MovieRating(mID, r));
					previousUserID = uID;
				}
			}else{
				totalRate+=r;
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
	     JobConf conf = new JobConf(CountUserMain.class);
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
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path("KNNInput"));
	     MainDriver.run(conf);
	   }
}
