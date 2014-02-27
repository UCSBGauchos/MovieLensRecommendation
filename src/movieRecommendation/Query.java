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
		Path queryPath = new Path("query");
		FileSystem hadoopFS = queryPath.getFileSystem(job);
		if(hadoopFS.exists(queryPath)){
			throw new UnsupportedEncodingException("Query is not set");
		}
		
		FSDataInputStream in = hdfs.open(queryPath);
		String line;
		
		//input the id of the movie
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter movie id of the movie you want its predicted rate: ");
		String movieID = br.readLine();

		Path KNNResult=new Path("/user/yangbo/KNN/part-00000");
		System.out.println(KNNResult.toString());
		
		//use reader to get the key and value: [mi neighbour(mj wij)] 
		KNNResult.Reader reader = new KNNResult.Reader(hadoopFS, KNNResult.toString(). new Configuration());
		LongWritable midKey = new LongWritable(Integer.parseInt(movieID));
		NeighbourArrayWritable neighborhoodValue = new NeighbourArrayWritable();
		reader.get(midKey, neighborhoodValue);
		System.out.println("The movie "+movieID+"'s neighbour is "+neighborhoodValue.toString());
		
		//here also need to process each user from the input dataset, read each line from the input dataset
		while ((line = in.readLine()) != null) {
			StringTokenizer token = new StringTokenizer(line.toString(), " |\t");
			long userID  = Long.parseLong(token.nextToken());
			
		}
		
		
	}
}
