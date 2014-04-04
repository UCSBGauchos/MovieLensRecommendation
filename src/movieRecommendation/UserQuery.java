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
	public static void main(String [] args) throws IOException {
		boolean isRating = false;
		ArrayList<String> localHash = new ArrayList<String>();
		JobConf job = new JobConf();

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter movie userid: ");
		String userID = br.readLine();
		System.out.print("Enter movie id of the movie you want its predicted rate: ");
		String movieID = br.readLine();

		Path KNNResult=new Path("UserKNN/part-00000");
		FileSystem hadoopFS = KNNResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("UserKNN"))){
			throw new UnsupportedEncodingException("UserKNN is not set");
		}
		FSDataInputStream KNNData = hadoopFS.open(KNNResult);
		
		Path UserResult=new Path("/input/u1.test");
		FSDataInputStream UserData1 = hadoopFS.open(UserResult);
		
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
					return;
				}
			}
		}
		System.out.println("Need to predict the rating which user "+userID+" gives to the movie "+movieID);
		if(localHash.size()==0){
			System.out.println("No record for "+movieID);
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
						if(uid.equals(guessUser)&&mid.equals(movieID)){
							System.out.println("Use the guess user "+guessUser);
							System.out.println("The predicted rating is user "+userID+" may give to the movie "+movieID+" is "+rating);
							return;
						}
					}
				}
				index++;
			}
		}
	}
}

