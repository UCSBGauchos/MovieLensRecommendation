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
		
		Path SlopeOnePreprocessResult = new Path("SlopeOnePreprocess/part-00000");
		FileSystem hadoopFS = SlopeOnePreprocessResult.getFileSystem(job);
		if(!hadoopFS.exists(new Path("SlopeOnePreprocess"))){
			throw new UnsupportedEncodingException("SlopeOnePreprocess is not set");
		}
		FSDataInputStream SlopeOnePreprocessData = hadoopFS.open(SlopeOnePreprocessResult);
		
		String readLine;
		
		while((readLine = SlopeOnePreprocessData.readLine()) != null){
			System.out.println(readLine);
		}
	}
	
}
