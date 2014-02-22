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

public class MainDriver {
	public static void main(String [] argv){
		int exitCode = -1;
		ProgramDriver driver = new ProgramDriver();
		try{
			driver.addClass("preprocess", CountUserMain.class, "Get the preprocess result from the given dataset");
			driver.addclass("KNN", KNNMain.class, "Do KNN Based on the preprocessed input data");
			driver.driver(argv);
			exitCode = 0;
		}catch(Throwable e){
			e.printStackTrace();
		}
		System.exit(exitCode); 
	}
	
	
	//each job defined in this system will call this run function, and the job will be submit to the jobtracker
	//and it will run by job tracker of hadoop
	public static void run(JobConf job) throws IOExecption{
		job.setJarByClass(MainDriver.class);
		JobClient.reunJob(job)
	}
	
}