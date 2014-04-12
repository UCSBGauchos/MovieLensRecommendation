import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.JobConf;

public class TendencyQuery {
	public static void main(String [] args) throws IOException {
		
		
		//movie1 u1 98 u2 100  
		//movie2 u1 97 u3 100
		HashMap<String, HashMap<String, Integer>> movieInfo = new HashMap<String, HashMap<String, Integer>>();
		//user1 movie1 98 movie2 97
		//user2 movie1 100
		//user3 movie 100
		HashMap<String, HashMap<String, Integer>> userInfo = new HashMap<String, HashMap<String, Integer>>();
		
		
		JobConf job = new JobConf();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter movie userid: ");
		String userID = br.readLine();
		System.out.print("Enter movie id of the movie you want its predicted rate: ");
		String movieID = br.readLine();
		

		Path TendencyItemPreprocessResult = new Path("TendencyItemPreprocess/part-00000");
		FileSystem hadoopFS = TendencyItemPreprocessResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("TendencyItemPreprocess"))){
			throw new UnsupportedEncodingException("TendencyItemPreprocess is not set");
		}
		FSDataInputStream TendencyItemPreprocessData = hadoopFS.open(TendencyItemPreprocessResult);
		
		Path TendencyUserPreprocessResult = new Path("TendencyUserPreprocess/part-00000");
		hadoopFS = TendencyUserPreprocessResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("TendencyUserPreprocess"))){
			throw new UnsupportedEncodingException("TendencyUserPreprocess is not set");
		}
		FSDataInputStream TendencyUserPreprocessData = hadoopFS.open(TendencyUserPreprocessResult);
		
		String itemLine;
		String userLine;
		
		while ((itemLine = TendencyItemPreprocessData.readLine()) != null){
			StringTokenizer token = new StringTokenizer(itemLine.toString(), " |\t,");
			String mid = token.nextToken();
			while(token.hasMoreTokens()){
				String uid = token.nextToken();
				float avg = Float.parseFloat(token.nextToken());
				Integer rating = Integer.parseInt(token.nextToken());
				HashMap<String, Integer> value = new HashMap<String, Integer>();
				value.put(uid, rating);
				if(!movieInfo.containsKey(mid)){
					movieInfo.put(mid, value);
				}else{
					movieInfo.get(mid).put(uid, rating);
				}
			}
		}
		
		while ((userLine = TendencyUserPreprocessData.readLine()) != null){
			StringTokenizer token = new StringTokenizer(userLine.toString(), " |\t,");
			String uid = token.nextToken();
			while(token.hasMoreTokens()){
				String mid = token.nextToken();
				float avg = Float.parseFloat(token.nextToken());
				Integer rating = Integer.parseInt(token.nextToken());
				HashMap<String, Integer> value = new HashMap<String, Integer>();
				value.put(mid, rating);
				if(!userInfo.containsKey(uid)){
					userInfo.put(uid, value);
				}else{
					userInfo.get(uid).put(mid, rating);
				}
			}
		}
		
		
		
		HashMap<String, Integer> userRating = userInfo.get(userID);
		if(userRating==null){
			System.out.println("User "+userID+" doesn't exist in the current MovieLens database, cannot be predicted! ");
			return;
		}
		float userTop = 0;
		float userTopSum = 0;
		//for each movie the user has rated
		for(String mid: userRating.keySet()){
			float itemAvgRating = 0;
			int itemSum = 0;
			int rating = userRating.get(mid);
			HashMap<String, Integer> movieUsers = movieInfo.get(mid);
			for(String uid: movieUsers.keySet()){
				itemSum+=movieUsers.get(uid);
			}
			itemAvgRating = itemSum/movieUsers.size();
			//System.out.println("avg of this movie is "+itemAvgRating+" user's rating is "+rating);
			userTop = (rating-itemAvgRating);
			userTopSum += userTop;
		}
		float tendencyUser = userTopSum/userRating.size();
		
		System.out.println("Tendency of "+userID+" is "+tendencyUser);
		
		HashMap<String, Integer> movieRating = movieInfo.get(movieID);
		if(movieRating==null){
			System.out.println("Movie "+movieID+" doesn't exist in the current MovieLens database, cannot be predicted! ");
			return;
		}
		float itemTop = 0;
		float itemTopSum = 0;
		//for each user who rated this movie
		for(String uid: movieRating.keySet()){
			float userAvgRating = 0;
			int userSum = 0;
			int rating = movieRating.get(uid);
			HashMap<String, Integer> userMovies = userInfo.get(uid);
			for(String mid: userMovies.keySet()){
				userSum+=userMovies.get(mid);
			}
			userAvgRating = userSum/userMovies.size();
			//System.out.println("avg of this user is "+userAvgRating+" to this movie rating is "+rating);
			itemTop = (rating-userAvgRating);
			itemTopSum += itemTop;
		}
		float tendencyItem = itemTopSum/movieRating.size();
		
		System.out.println("Tendency of "+movieID+" is "+tendencyItem);
		
		int chosenUserSum = 0;
		float chosenUserAvgRating = 0;
		HashMap<String, Integer> chosenUser = userInfo.get(userID);
		for(String mid: chosenUser.keySet()){
			chosenUserSum+=chosenUser.get(mid);
		}
		chosenUserAvgRating = chosenUserSum/chosenUser.size();
		System.out.println("The avg of chosen user "+userID+" is "+chosenUserAvgRating);
		
		int chosenMovieSum = 0;
		float chosenMovieAvgRating = 0;
		HashMap<String, Integer> chosenMovie = movieInfo.get(movieID);
		for(String uid: chosenMovie.keySet()){
			chosenMovieSum+=chosenMovie.get(uid);
		}
		chosenMovieAvgRating = chosenMovieSum/chosenMovie.size();
		System.out.println("The avg of chosen movie "+movieID+" is "+chosenMovieAvgRating);
		
		if(userRating.containsKey(movieID)){
			System.out.println("User "+userID+" has rated "+userRating.get(movieID)+" to movie "+movieID+" ,no prediction ");
			return;
		}
		
		//both positive
		if(tendencyItem>0&&tendencyUser>0){
			System.out.println("Both positive, the user should give the good rating");
			System.out.println("The prediction of user "+userID+" to movie "+movieID+" is "+Math.max(chosenUserAvgRating+tendencyItem, chosenMovieAvgRating+tendencyUser));
		}else if(tendencyItem<0&&tendencyUser<0){
			System.out.println("Both negative, the user should give the bad rating");
			System.out.println("The prediction of user "+userID+" to movie "+movieID+" is "+Math.min(chosenUserAvgRating+tendencyItem, chosenMovieAvgRating+tendencyUser));
		}else if(tendencyItem>0&&tendencyUser<0){
			float contribute = (float) 0.5;
			if(chosenMovieAvgRating>=3&&chosenUserAvgRating<=2){
				System.out.println("The mean matches the tendency");
				float predictResult = Math.min(Math.max(chosenUserAvgRating, (chosenMovieAvgRating+tendencyUser)*contribute+(chosenUserAvgRating+tendencyItem)*(1-contribute)), chosenMovieAvgRating);
				System.out.println("The prediction of user "+userID+" to movie "+movieID+" is "+predictResult);
			}else{
				System.out.println("The mean doesn't match the tendency");
				float predictResult = chosenMovieAvgRating*contribute+chosenUserAvgRating*(1-contribute);
				System.out.println("The prediction of user "+userID+" to movie "+movieID+" is "+predictResult);			
			}
		}else if(tendencyItem<0&&tendencyUser>0){
			float contribute = (float) 0.5;
			if(chosenMovieAvgRating<=2&&chosenUserAvgRating>=3){
				System.out.println("The mean matches the tendency");
				float predictResult = Math.min(Math.max(chosenUserAvgRating, (chosenMovieAvgRating+tendencyUser)*contribute+(chosenUserAvgRating+tendencyItem)*(1-contribute)), chosenMovieAvgRating);
				System.out.println("The prediction of user "+userID+" to movie "+movieID+" is "+predictResult);
			}else{
				System.out.println("The mean doesn't match the tendency");
				float predictResult = chosenMovieAvgRating*contribute+chosenUserAvgRating*(1-contribute);
				System.out.println("The prediction of user "+userID+" to movie "+movieID+" is "+predictResult);	
			}
		}
		
		
		
		
	}
}
