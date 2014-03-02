import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.JobConf;

public class Query {
	public static void main(String [] args) throws IOException {
		boolean isRating = false;
		HashMap<String, Float> localHash = new HashMap<String, Float>();
		HashMap<String, Integer> localUserRating = new HashMap<String, Integer>();
		JobConf job = new JobConf();
//		Path queryPath = new Path("query");
//		FileSystem hadoopFS = queryPath.getFileSystem(job);
//		if(hadoopFS.exists(queryPath)){
//			throw new UnsupportedEncodingException("Query is not set");
//		}
//		
//		FSDataInputStream in = hadoopFS.open(queryPath);
//		String line;
		
		//input the id of the movie
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter movie userid: ");
		String userID = br.readLine();
		System.out.print("Enter movie id of the movie you want its predicted rate: ");
		String movieID = br.readLine();
		
		

		//neighbour: read from KNNData
		//user: read From UserData
		
		Path KNNResult=new Path("KNN/part-00000");
		FileSystem hadoopFS = KNNResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("KNN"))){
			throw new UnsupportedEncodingException("KNN is not set");
		}
		FSDataInputStream KNNData = hadoopFS.open(KNNResult);
		
		Path UserResult=new Path("/input/u1.test");
//		if(!hadoopFS.exists(new Path("/input"))){
//			throw new UnsupportedEncodingException("KNN is not set");
//		}
		FSDataInputStream UserData = hadoopFS.open(UserResult);
		
		String neighbourtLine;
		while ((neighbourtLine = KNNData.readLine()) != null){
			StringTokenizer token = new StringTokenizer(neighbourtLine.toString(), " |\t,");
			String mid = token.nextToken();
			if(mid.equals(movieID)){
				while(token.hasMoreTokens()){
					String neighbourmid = token.nextToken();
					float wij = Float.parseFloat(token.nextToken());
					localHash.put(neighbourmid, wij);
				}
			}
		}
		//System.out.println(localHash);

		String userLine;
		while ((userLine = UserData.readLine()) != null){
			StringTokenizer token = new StringTokenizer(userLine.toString(), " |\t");
			while(token.hasMoreTokens()){
				String uid = token.nextToken();
				String mid = token.nextToken();
				Integer rating = Integer.parseInt(token.nextToken());
				String time = token.nextToken();
				if(uid.equals(userID)){
					localUserRating.put(mid, rating);
				}
			}
		}
		float predictedRateUp = 0f;
		float predictedRateDown = 0f;
		//System.out.println(localUserRating);
		if(localUserRating.keySet().contains(movieID)){
			System.out.println("User "+userID+" has rated "+localUserRating.get(movieID)+" to the movie "+movieID+" no need to predict");
			isRating = true;		
		}else{
			System.out.println("Need to predict the rating which user "+userID+" gives to the movie "+movieID);
			if(localHash.size()==0){
				System.out.println("No record for "+movieID);
			}else{
				for(String neighboutMovieID: localHash.keySet()){
					if(localUserRating.keySet().contains(neighboutMovieID)){
						//System.out.println("User has rated the neighbour "+neighboutMovieID+" "+localUserRating.get(neighboutMovieID)+" ,the weight is "+localHash.get(neighboutMovieID));
						predictedRateUp += localUserRating.get(neighboutMovieID)*localHash.get(neighboutMovieID);
						predictedRateDown += Math.abs(localHash.get(neighboutMovieID));
					}
				}
			}
		}
		if(!isRating){
			System.out.println("The predicted rating is user "+userID+" may give to the movie "+movieID+" is "+(predictedRateUp / predictedRateDown));
		}
	}
}
