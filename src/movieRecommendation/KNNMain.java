package movieRecommendation;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.ArrayFile;


//Based on the paper "Efficient Multicore Collaborative Filtering", it will calculate top k similar neighbour songs of the given
//song
//No Reduce process for this KNN step
public class KNNMain {
	public static final int K = 10;
	public static void main(String [] args) throws Excpetion{
		JobConf job = new JobConf();
		job.setJobName(KNNMain.class.getSimpleName());
		job.setJarByClass(KNNMain.class);
		
		job.setMapRunnerClass(KNNMapRunner.class);
		job.setMapperClass(KNNMapper.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NeighboursArrayWritable.class);
		job.setNumReduceTasks(0);//no reduce for this step
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NeighboursArrayWritable.class);
		
		job.setInputFormat(SequenceFileInputFormat.class);
		String inputPath = "input";
		SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
		//each time remove the output folder first!
		Path outputPath = new Path("output"));
		FileSystem.get(job).delete(outputPath, true);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		MainDriver.run(job);

	}
}
