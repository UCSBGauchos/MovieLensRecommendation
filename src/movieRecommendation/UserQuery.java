import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.JobConf;

public class UserQuery {
	
	public float getResult(String userID, String movieID) throws IOException{
		float predResult = 0;
		boolean isRating = false;
		ArrayList<String> localHash = new ArrayList<String>();
		JobConf job = new JobConf();

//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		System.out.print("Enter movie userid: ");
//		String userID = br.readLine();
//		System.out.print("Enter movie id of the movie you want its predicted rate: ");
//		String movieID = br.readLine();

		Path KNNResult=new Path("UserKNN/part-00000");
		FileSystem hadoopFS = KNNResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("UserKNN"))){
			throw new UnsupportedEncodingException("UserKNN is not set");
		}
		FSDataInputStream KNNData = hadoopFS.open(KNNResult);
		
		Path UserResult=new Path("/input/90");
		FSDataInputStream UserData1 = hadoopFS.open(UserResult);
		
		//int threshold = 10;
		//int i = 0;
		
		String neighbourtLine;
		while ((neighbourtLine = KNNData.readLine()) != null){
			StringTokenizer token = new StringTokenizer(neighbourtLine.toString(), " |\t,");
			String uid = token.nextToken();
			if(uid.equals(userID)){
				while(token.hasMoreTokens()){
					String neighbourmid = token.nextToken();
					float wij = Float.parseFloat(token.nextToken());
					localHash.add(neighbourmid);
				}
			}
		}
		
		//System.out.println(localHash);
		
		
		String userLine;
		while ((userLine = UserData1.readLine()) != null){
			StringTokenizer token = new StringTokenizer(userLine.toString(), " |\t");
			while(token.hasMoreTokens()){
				String uid = token.nextToken();
				String mid = token.nextToken();
				Integer rating = Integer.parseInt(token.nextToken());
				String time = token.nextToken();
				if(uid.equals(userID)&&mid.equals(movieID)){
					System.out.println("User "+userID+" has rated "+rating+" to the movie "+movieID+" no need to predict");
					return -1;
				}
			}
		}
		System.out.println("Need to predict the rating which user "+userID+" gives to the movie "+movieID);
		int threshold = 10;
		int i=0;
		int simSum = 0;
		if(localHash.size()==0){
			System.out.println("No record for "+movieID);
			return -1;
		}else{
			int index = 0;
			while(index<localHash.size()){
				String guessUser = localHash.get(index);
				FSDataInputStream UserData2 = hadoopFS.open(UserResult);
				while ((userLine = UserData2.readLine()) != null){
					StringTokenizer token = new StringTokenizer(userLine.toString(), " |\t");
					while(token.hasMoreTokens()){
						String uid = token.nextToken();
						String mid = token.nextToken();
						Integer rating = Integer.parseInt(token.nextToken());
						String time = token.nextToken();
						if(uid.equals(guessUser)&&mid.equals(movieID)&&i<threshold){
							//System.out.println("Use the guess user "+guessUser);
							//System.out.println("The predicted rating is user "+userID+" may give to the movie "+movieID+" is "+rating);
							i++;
							simSum+=rating;
							//return;
						}
					}
				}
				index++;
				UserData2.close();
			}
			if(i==0){
				System.out.println("All the neighbour user of the given user has not rated to the given movie, cannot predict");
				return -1;
			}
			predResult = (float)simSum/i;
			System.out.println("The predict result = "+predResult);
			KNNData.close();
			UserData1.close();
			return predResult;
		}
	}
	
	public static void main(String [] args) throws IOException {
		UserQuery u = new UserQuery();
		float sum = 0;
		float diff = 0;
		int num = 0;
		float MAEResult = 0;
		FileReader Reader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/Evulation");
		BufferedReader br = new BufferedReader(Reader);
		FileWriter writer = new FileWriter("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/90UserBasedLog");
	    BufferedWriter bw = new BufferedWriter(writer);
		String str = null;
		while((str = br.readLine())!=null){
			StringTokenizer token = new StringTokenizer(str.toString(), " \t");
			while(token.hasMoreTokens()){
				String userID = token.nextToken();
				String movieID = token.nextToken();
				String rating = token.nextToken();
				String date = token.nextToken();
				float pred = u.getResult(userID, movieID);
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

