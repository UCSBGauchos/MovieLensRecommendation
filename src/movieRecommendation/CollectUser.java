import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//input: user, movie, rating, date
//output: user, movieList

public class CollectUser {
	public static class CollectUserMap extends MapReduceBase implements Mapper<Object, Text, LongWritable, MovieRating>{
		MovieRating movieInfo = new MovieRating();
		public void map(Object unusedInKey, Text inValue, OutputCollector<LongWritable, MovieRating> output, Reporter reporter) throws IOException{			
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
			movieInfo.set(mID, r);
			output.collect(userID, movieInfo);
		}
	}
	public static class CollectUserReduce extends MapReduceBase implements Reducer<LongWritable, MovieRating, LongWritable, MovieRatingArrayWritable>{
		public void reduce(LongWritable movieID, Iterator<MovieRating> nextMovie, OutputCollector<LongWritable, MovieRatingArrayWritable> output, Reporter reporter) throws IOException{
			ArrayList<MovieRating> movies = new ArrayList<MovieRating>();
			while(nextMovie.hasNext()){
				MovieRating movieInfo = nextMovie.next();
				movies.add(new MovieRating(movieInfo.movieID, movieInfo.rate));
			}
			MovieRating[] moviesArray = new MovieRating[movies.size()];
			moviesArray = movies.toArray(new MovieRating[movies.size()]);
			Arrays.sort(moviesArray);
			output.collect(movieID, new MovieRatingArrayWritable(moviesArray));
		}
	}
	
	public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(CollectUser.class);
	     conf.setJobName("collectuser");
	     conf.setMapOutputKeyClass(LongWritable.class);
	     conf.setMapOutputValueClass(MovieRating.class);
	     conf.setOutputKeyClass(LongWritable.class);
	     conf.setOutputValueClass(MovieRatingArrayWritable.class);
	     conf.setMapperClass(CollectUserMap.class);
	     //conf.setCombinerClass(CountUserInfoReduce.class);
	     conf.setReducerClass(CollectUserReduce.class);
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(SequenceFileOutputFormat.class);
	     //conf.setOutputFormat(SequenceFileOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileSystem.get(conf).delete(new Path("Collect"), true);
	     FileOutputFormat.setOutputPath(conf, new Path("Collect"));
	     MainDriver.run(conf);
	}
	
}
