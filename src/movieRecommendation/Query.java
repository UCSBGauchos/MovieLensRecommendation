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
		
		Path UserResult=new Path("/input/u1.test");
		if(!hadoopFS.exists(new Path("/input"))){
			throw new UnsupportedEncodingException("KNN is not set");
		}
		FSDataInputStream UserData = hadoopFS.open(UserResult);
		
		
		
	}
}
