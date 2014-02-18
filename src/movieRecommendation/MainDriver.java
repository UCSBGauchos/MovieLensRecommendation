import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;



public class MainDriver {
	public static void main(String [] argv){
		int exitCode = -1;
		ProgramDriver driver = new ProgramDriver();
		try{
			System.out.println("Begin!");
			driver.addClass("preprocess", CountUserMain.class, "Get the preprocess result from the given dataset");
			driver.driver(argv);
			exitCode = 0;
		}catch(Throwable e){
			e.printStackTrace();
		}
		System.exit(exitCode); 
	}
}
