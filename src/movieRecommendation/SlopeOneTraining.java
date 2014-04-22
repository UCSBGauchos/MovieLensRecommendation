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
	
	public static void main(String [] args) throws IOException {
		JobConf job = new JobConf();
		//user1 movie1 98 movie2 97
		//user2 movie1 100
		//user3 movie 100
		//only choose those users which rate both i and j
		HashMap<String, HashMap<String, Integer>> chosenUserInfo = new HashMap<String, HashMap<String, Integer>>();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		//System.out.print("Enter movie userid: ");
		//String userID = br.readLine();
		System.out.print("Enter movie id of the movie you want its predicted rate: ");
		String movieID = br.readLine();	
		String tmp = "20";//tmp chose i as 10;
		
		
		Path SlopeOnePreprocessResult = new Path("SlopeOnePreprocess/part-00000");
		FileSystem hadoopFS = SlopeOnePreprocessResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("SlopeOnePreprocess"))){
			throw new UnsupportedEncodingException("SlopeOnePreprocess is not set");
		}
		FSDataInputStream SlopeOnePreprocessData = hadoopFS.open(SlopeOnePreprocessResult);
		
		String itemLine;
		
		while((itemLine = SlopeOnePreprocessData.readLine()) != null){
			StringTokenizer token = new StringTokenizer(itemLine.toString(), " |\t,");
			StringTokenizer token2 = new StringTokenizer(itemLine.toString(), " |\t,");
			String uid = token.nextToken();
			boolean containsI = false;
			boolean containsJ = false;
			while(token.hasMoreTokens()){
				String mid = token.nextToken();
				Integer rating = Integer.parseInt(token.nextToken());
				if(mid.equals(movieID)){
					containsJ = true;
				}
				if(mid.equals(tmp)){
					containsI = true;
				}
			}
			//if this line contains both movie i and j, then it will be written to the list
			if(containsI == true && containsJ == true){
				String uid2 = token2.nextToken();
				while(token2.hasMoreTokens()){
					String mid2 = token2.nextToken();
					Integer rating2 = Integer.parseInt(token2.nextToken());
					HashMap<String, Integer> value = new HashMap<String, Integer>();
					value.put(mid2, rating2);
					if(!chosenUserInfo.containsKey(uid2)){
						chosenUserInfo.put(uid2, value);
					}else{
						chosenUserInfo.get(uid2).put(mid2, rating2);
					}
				}
			}
		}
		System.out.println(chosenUserInfo.keySet());
	}
	
}
