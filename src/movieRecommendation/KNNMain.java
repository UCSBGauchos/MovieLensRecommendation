import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.ArrayFile;


//Based on the paper "Efficient Multicore Collaborative Filtering", it will calculate top k similar neighbour songs of the given
//song
//No Reduce process for this KNN step
public class KNNMain {
	public static final int K = 10;
	public static void main(String [] args) throws Exception{
		JobConf job = new JobConf();
		job.setJobName(KNNMain.class.getSimpleName());
		job.setJarByClass(KNNMain.class);
	
		
		job.setMapRunnerClass(KNNMapRunner.class);
		job.setMapperClass(KNNMapper.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NeighbourArrayWritable.class);
		job.setNumReduceTasks(0);//no reduce for this step
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NeighbourArrayWritable.class);
		
		
		job.setInputFormat(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path("Preprocess"));
		//each time remove the output folder first!
		FileSystem.get(job).delete(new Path("KNN"), true);
		job.setOutputFormat(TextOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path("KNN"));
		MainDriver.run(job);

	}
}
