import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class KNNMapRunner extends MapRunner<LongWritable, PostingUserArrayWritable, LongWritable, NeighbourArray>{
	public KNNMapper mapper = new KNNMapper();
	public HashMap<Long, PostingUser[]> MovieUsers = new HashMap<Long, PostingUser[]>();
	public FileSystem hadoopFS;
	public JobConf job;
	public Path path;
	
	public void configure(JobConf job){
		this.job = job;
		path = new Path(job.get("map.input.file"));
		try{
			hadoopFS = FileSystem.get(job);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	//input key is longwritable, input value is postingUser array, read it drom the input file,
	//save them in the local hashmap
	public void run(RecordReader input, OutputCollector output, Reporter reporter) throws IOException {
		LongWritable key = new LongWritable();
		PostingUserArrayWritable value = new PostingUserArrayWritable();
		while (input.next(key, value)){
			MovieUsers.put(key.get(), value.getPosting());
		}
		mapper.movieUsers = MovieUsers;
		
		//here call compareOwn to get neighbour in my own file
		mapper.compareOwn();
		
		//get all the other preprocessed file from hadoop file system, get get neighbour
		FileStatus[] fStatus = hadoopFS.listStatus(path.getParent());
		long t = System.nanoTime();
		for (int currentFile = 1; currentFile < fStatus.length; currentFile++){
			Path currentPath = fStatus[currentFile].getPath();
			if(hadoopFS.isFile(currentPath)&&(!currentPath.equals(path))){
				System.err.println("Reading:" + currentPath.getName());
				Reader reader = new SequenceFile.Reader(hadoopFS, currentPath, job);
				mapper.compareWithOthers(reader);
			}
		}
		System.out.println("Similarity comparison time in millisec:" + (System.nanoTime() - t)
				/ 1000000.0);
		//call the map function
		mapper.map(key, value, output, reporter);
		mapper.close();
	}
	
}
