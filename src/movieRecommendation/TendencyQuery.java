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
		float itemAvgRating = 0;
		int itemSum = 0;

		float top = 0;
		float topSum = 0;
		//for each movie the user has rated
		for(String mid: userRating.keySet()){
			int rating = userRating.get(mid);
			HashMap<String, Integer> movieUsers = movieInfo.get(mid);
			for(String uid: movieUsers.keySet()){
				itemSum+=movieUsers.get(uid);
			}
			itemAvgRating = itemSum/movieUsers.size();
			top = (rating-itemAvgRating);
			topSum += top;
		}
		float tendencyUser = topSum/userRating.size();
		
		System.out.println(tendencyUser);
		
		
	}
}
