import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.JobConf;

public class ItemQuery {
	
	//all invalid cases return -1
	
	public float getResult(String userID, String movieID) throws IOException{
		float predResult = 0;
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
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		System.out.print("Enter movie userid: ");
//		String userID = br.readLine();
//		System.out.print("Enter movie id of the movie you want its predicted rate: ");
//		String movieID = br.readLine();
		
		

		//neighbour: read from KNNData
		//user: read From UserData
		
		Path KNNResult=new Path("KNN/part-00000");
		FileSystem hadoopFS = KNNResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("KNN"))){
			throw new UnsupportedEncodingException("KNN is not set");
		}
		FSDataInputStream KNNData = hadoopFS.open(KNNResult);
		
		Path UserResult=new Path("/input/10");
//		if(!hadoopFS.exists(new Path("/input"))){
//			throw new UnsupportedEncodingException("KNN is not set");
//		}
		FSDataInputStream UserData = hadoopFS.open(UserResult);
		
		int threshold = 10;
		int index = 0;
		
		//if there are more than 10 neighbours, then choose top 10 of them, else choose all
		String neighbourtLine;
		while ((neighbourtLine = KNNData.readLine()) != null){
			StringTokenizer token = new StringTokenizer(neighbourtLine.toString(), " |\t,");
			String mid = token.nextToken();
			if(mid.equals(movieID)){
				while(token.hasMoreTokens()){
					String neighbourmid = token.nextToken();
					float wij = Float.parseFloat(token.nextToken());
					localHash.put(neighbourmid, wij);
					//index++;
				}
			}
		}
		//System.out.println(localHash);
		KNNData.close();

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
		UserData.close();
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
				return -1;
			}else if(localUserRating.size()==0){
				System.out.println("No record for "+userID);
				return -1;
			}else{
				for(String neighboutMovieID: localHash.keySet()){
					if(localUserRating.keySet().contains(neighboutMovieID)&&index<threshold){
						//System.out.println("User has rated the neighbour "+neighboutMovieID+" "+localUserRating.get(neighboutMovieID)+" ,the weight is "+localHash.get(neighboutMovieID));
						predictedRateUp += localUserRating.get(neighboutMovieID)*localHash.get(neighboutMovieID);
						predictedRateDown += Math.abs(localHash.get(neighboutMovieID));
						index++;
					}
				}
			}
		}
		if(predictedRateDown == 0){
			System.out.println("The neighbour of "+movieID+" doesn't contain any movie whihc user "+userID+" has rated");
			return -1;
		}
		if(!isRating){
			predResult = (float)predictedRateUp / predictedRateDown;
			System.out.println("The predicted rating is user "+userID+" may give to the movie "+movieID+" is "+predResult);	
			return predResult;
		}
		return -1;
	}
	
	public static void main(String [] args) throws IOException {
		ItemQuery i = new ItemQuery();
		float sum = 0;
		float diff = 0;
		int num = 0;
		float MAEResult = 0;
		FileReader Reader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/Evulation");
		BufferedReader br = new BufferedReader(Reader);
		FileWriter writer = new FileWriter("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/10ItemBasedLog");
	    BufferedWriter bw = new BufferedWriter(writer);
		String str = null;
		while((str = br.readLine())!=null){
			StringTokenizer token = new StringTokenizer(str.toString(), " \t");
			while(token.hasMoreTokens()){
				String userID = token.nextToken();
				String movieID = token.nextToken();
				String rating = token.nextToken();
				String date = token.nextToken();
				float pred = i.getResult(userID, movieID);
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
