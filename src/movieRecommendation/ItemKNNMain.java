
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.ArrayFile;


//Based on the paper "Efficient Multicore Collaborative Filtering", it will calculate top k similar neighbour songs of the given
//song
//No Reduce process for this KNN step
public class ItemKNNMain {
	public static void main(String [] args) throws Exception{
		 JobConf conf = new JobConf(ItemKNNMain.class);
	     conf.setJobName("knn");
	     conf.setMapOutputKeyClass(LongWritable.class);
	     conf.setMapOutputValueClass(NeighbourArrayWritable.class);
	     conf.setNumMapTasks(1);
	     conf.setNumReduceTasks(0);
	     //conf.setOutputKeyClass(LongWritable.class);
	     //conf.setOutputValueClass(PostingUserArrayWritable.class);
	     conf.setMapRunnerClass(ItemKNNMapRunner.class);
	     conf.setMapperClass(ItemKNNMapper.class);
	     //conf.setCombinerClass(CountUserInfoReduce.class);
	     //conf.setReducerClass(CountUserInfoReduce.class);
	     conf.setInputFormat(SequenceFileInputFormat.class);
	     //conf.setOutputFormat(TextOutputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path("ItemPreprocess"));
	     //each time remove the outpt first
	     FileSystem.get(conf).delete(new Path("KNN"), true);
	     FileOutputFormat.setOutputPath(conf, new Path("KNN"));
	     MainDriver.run(conf);

	}
}
