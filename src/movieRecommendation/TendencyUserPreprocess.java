import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class TendencyUserPreprocess {
	public static class CountMovieInfoMap extends MapReduceBase implements Mapper<LongWritable, MovieRatingArrayWritable, LongWritable, PostingMovie>{
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
	
	public static class CountMovieInfoReduce extends MapReduceBase implements Reducer<LongWritable, PostingMovie, LongWritable, PostingMovieArrayWritable>{
		public void reduce(LongWritable userID, Iterator<PostingMovie> nextMovie, OutputCollector<LongWritable, PostingMovieArrayWritable> output, Reporter reporter) throws IOException{
			ArrayList<PostingMovie> movies = new ArrayList<PostingMovie>();
			while(nextMovie.hasNext()){
				PostingMovie hold = nextMovie.next();
				movies.add(new PostingMovie(hold.movieID, hold.avgRating, hold.rate));
			}
			PostingMovie[] arrayMovies = new PostingMovie[movies.size()];
			arrayMovies = movies.toArray(new PostingMovie[movies.size()]);
			Arrays.sort(arrayMovies);// sorting checked
			output.collect(userID, new PostingMovieArrayWritable(arrayMovies));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TendencyUserPreprocess.class);
	    conf.setJobName("TendencyUserPreprocess");
	    conf.setMapOutputKeyClass(LongWritable.class);
	    conf.setMapOutputValueClass(PostingMovie.class);
	    conf.setOutputKeyClass(LongWritable.class);
	    conf.setOutputValueClass(PostingMovieArrayWritable.class);
	    conf.setMapperClass(CountMovieInfoMap.class);
	    //conf.setCombinerClass(CountUserInfoReduce.class);
	    conf.setReducerClass(CountMovieInfoReduce.class);
	    conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    //conf.setOutputFormat(SequenceFileOutputFormat.class);
	    FileInputFormat.setInputPaths(conf, new Path("Collect"));
	    //each time remove the outpt first
	    FileSystem.get(conf).delete(new Path("TendencyUserPreprocess"), true);
	    FileOutputFormat.setOutputPath(conf, new Path("TendencyUserPreprocess"));
	    MainDriver.run(conf);
	}
	
}
