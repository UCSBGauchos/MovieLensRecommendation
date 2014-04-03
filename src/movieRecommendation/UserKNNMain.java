
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.ArrayFile;

//user-based knn algorithm
public class UserKNNMain {
	public static void main(String [] args) throws Exception{
		 JobConf conf = new JobConf(UserKNNMain.class);
	     conf.setJobName("knn");
	     conf.setMapOutputKeyClass(LongWritable.class);
	     conf.setMapOutputValueClass(UserNeighbourArrayWritable.class);
	     conf.setNumMapTasks(1);
	     conf.setNumReduceTasks(0);
	     //conf.setOutputKeyClass(LongWritable.class);
	     //conf.setOutputValueClass(PostingUserArrayWritable.class);
	     conf.setMapRunnerClass(UserKNNMapRunner.class);
	     conf.setMapperClass(UserKNNMapper.class);
	     //conf.setCombinerClass(CountUserInfoReduce.class);
	     //conf.setReducerClass(CountUserInfoReduce.class);
	     conf.setInputFormat(SequenceFileInputFormat.class);
	     //conf.setOutputFormat(TextOutputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path("UserPreprocess"));
	     //each time remove the outpt first
	     FileSystem.get(conf).delete(new Path("UserKNN"), true);
	     FileOutputFormat.setOutputPath(conf, new Path("UserKNN"));
	     MainDriver.run(conf);

	}
}
