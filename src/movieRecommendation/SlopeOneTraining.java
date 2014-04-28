import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.JobConf;

public class SlopeOneTraining {
	
	public float getResult(String userID, String movieID) throws IOException{
		JobConf job = new JobConf();
		//user1 movie1 98 movie2 97
		//user2 movie1 100
		//user3 movie 100
		//only choose those users which rate both i and j
		HashMap<String, Integer> hasRatedMovie = new HashMap<String, Integer>();
		HashMap<String, HashMap<String, Integer>> localUserRating = new HashMap<String, HashMap<String, Integer>>();
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		System.out.print("Enter movie userid: ");
//		String userID = br.readLine();
//		System.out.print("Enter movie id of the movie you want its predicted rate: ");
//		String movieID = br.readLine();	
//		//String tmp = "20";//tmp chose i as 10;
		
		
		Path SlopeOnePreprocessResult = new Path("SlopeOnePreprocess/part-00000");
		FileSystem hadoopFS = SlopeOnePreprocessResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("SlopeOnePreprocess"))){
			throw new UnsupportedEncodingException("SlopeOnePreprocess is not set");
		}
		FSDataInputStream SlopeOnePreprocessData1 = hadoopFS.open(SlopeOnePreprocessResult);
		FSDataInputStream SlopeOnePreprocessData2 = hadoopFS.open(SlopeOnePreprocessResult);
		
		//hasRatedMovie store all the movies which the chosen user has rated
		String UserLine;
		int ratingSum = 0;
		while((UserLine = SlopeOnePreprocessData1.readLine()) != null){
			StringTokenizer token = new StringTokenizer(UserLine.toString(), " |\t,");//for tracking outside loop
			String uid = token.nextToken();
			while(token.hasMoreTokens()){
				String mid = token.nextToken();
				Integer rating = Integer.parseInt(token.nextToken());
				if(uid.equals(userID)){
					hasRatedMovie.put(mid, rating);
					ratingSum+=rating;
				}
			}
		}
		SlopeOnePreprocessData1.close();
		
		//localUserRating store all the user rating infos from the input file
		while((UserLine = SlopeOnePreprocessData2.readLine()) != null){
			StringTokenizer token = new StringTokenizer(UserLine.toString(), " |\t,");
			String uid = token.nextToken();
			while(token.hasMoreTokens()){
				String mid = token.nextToken();
				Integer rating = Integer.parseInt(token.nextToken());
				if(!localUserRating.containsKey(uid)){
					HashMap<String, Integer> value = new HashMap<String, Integer>();
					value.put(mid, rating);
					localUserRating.put(uid, value);
				}else{
					localUserRating.get(uid).put(mid, rating);
				}
			}
		}
		SlopeOnePreprocessData2.close();
		
		
		int bigSum = 0;
		int index = 0;
		
		if(hasRatedMovie.size() == 0){
			System.out.println("this user has not rated any movie, cannot predict");
			return -1;
		}
		
		if(localUserRating.size() == 0){
			System.out.println("No record");
			return -1;
		}
		
		for(String movie: hasRatedMovie.keySet()){
			//chosenUserInfo store all the users which is chosen, because this user has rated both current movie and the predict
			//movie
			HashMap<String, HashMap<String, Integer>> chosenUserInfo = new HashMap<String, HashMap<String, Integer>>();
			for(String uid: localUserRating.keySet()){
				//this user has rated both movie and current movie
				if(localUserRating.get(uid).keySet().contains(movie)&&localUserRating.get(uid).keySet().contains(movieID)){
					chosenUserInfo.put(uid, localUserRating.get(uid));
				}
			}
			if(chosenUserInfo.size()!=0){
				int length = chosenUserInfo.keySet().size();
				float sum = 0;
				for(String user: chosenUserInfo.keySet()){
					HashMap<String, Integer> chosenValue = chosenUserInfo.get(user);
					sum += (chosenValue.get(movieID)-chosenValue.get(movie));
				}
				float dev = (float) sum/length;
				bigSum+=dev;
				index++;
				//System.out.println("Dev for "+movieID+" and "+movie+" equals to "+dev);
			}
		}
		if(index == 0){
			return -1;
		}
		float constantModelValue = (float) bigSum/index;
		System.out.println("The constant value for user "+userID+" and movie "+movieID+" is "+constantModelValue);
		
		float userAvgRating = (float)ratingSum/hasRatedMovie.size();
		
		System.out.println("User "+userID+" average rating is "+userAvgRating);
		
		//model: f(x) = x+b
		float result = userAvgRating+constantModelValue;
		
		System.out.println("The predicted rating which user "+userID+" may give to movie "+movieID+" is "+result);
		return result;
	}
	
	public static void main(String [] args) throws IOException {
		SlopeOneTraining s = new SlopeOneTraining();
		float sum = 0;
		float diff = 0;
		int num = 0;
		float MAEResult = 0;
		FileReader Reader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/Evulation");
		BufferedReader br = new BufferedReader(Reader);
		FileWriter writer = new FileWriter("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/90SlopeOneLog");
	    BufferedWriter bw = new BufferedWriter(writer);
		String str = null;
		while((str = br.readLine())!=null){
			StringTokenizer token = new StringTokenizer(str.toString(), " \t");
			while(token.hasMoreTokens()){
				String userID = token.nextToken();
				String movieID = token.nextToken();
				String rating = token.nextToken();
				String date = token.nextToken();
				float pred = s.getResult(userID, movieID);
				if(pred!=-1){
					diff = Math.abs(pred - Integer.parseInt(rating));
					String log = "User: "+userID+" Movie: "+movieID+" pred is "+pred+" rating is "+Integer.parseInt(rating)+" diff is "+diff;
					System.out.println(log);
					bw.write(log);
					bw.newLine();
					sum+=diff;
					num++;
				}
			}
		}
		MAEResult = (float)sum/num;
		String logResult = "MAE is "+MAEResult;
		bw.write(logResult);
		System.out.println(logResult);
		Reader.close();
		br.close();
		bw.close();
		writer.close();
	}
	
}
