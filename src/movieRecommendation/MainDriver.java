import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;

//the main driver should be call at first, user need to run preprocess first 

//preprpcess - preprpcess the data
//KNN - get the neighbour based on the preprocessed data
//result-TODO
public class MainDriver {
	public static void main(String [] argv){
		int exitCode = -1;
		ProgramDriver driver = new ProgramDriver();
		try{
			driver.addClass("itempreprocess", ItemPreprocess.class, "Get the item-based preprocess result from the given dataset");
			driver.addClass("itemknn", ItemKNNMain.class, "Do item-based KNN on the preprocessed input data");
			driver.addClass("helloworld", HelloWorld.class, "Hello World");
			driver.addClass("collectuser", CollectUser.class, "Collect user infomation from the input dataset");
			driver.addClass("startitemquery", ItemQuery.class, "Start item-based collaborative algorithm to predict the rating. Make sure you have run collect, preprocess and knn command");
			driver.driver(argv);
			exitCode = 0;
		}catch(Throwable e){
			e.printStackTrace();
		}
		System.exit(exitCode); 
	}
	
	
	//each job defined in this system will call this run function, and the job will be submit to the jobtracker
	//and it will run by job tracker of hadoop
	public static void run(JobConf job) throws IOException{
		job.setJarByClass(MainDriver.class);
		JobClient.runJob(job);
	}
	
}
