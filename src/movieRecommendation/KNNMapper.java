import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KNNMapper extends MapReduceBase implements Mapper<LongWritable, PostingUserArrayWritable, LongWritable, NeighboursArray>{
	
	//movieID <userID, uverACG, rate>
	public HashMap<Long, PostingUser[]> movieUsers;
	public HashMap<Long, SortedArrayList<Neighbour>> similarityNieghbour = new HashMap<Long, SortedArrayList<Neighbour>>();
	
	public void compareOwn(){
		int movieNum = movieUsers.size();
		Long[] movieIDs = new Long[movieNum];
		movieUsers.keySet().toArray(movieIDs);
		
		//get the usetPostingInfo for each pair movie i and j
		for(int i=0; i<movieNum-1; i++){
			PostingUser[] usersForMoviei = movieUsers.get(movieIDs[i]);
			for(int j= i+1; j<movieNum; j++){
				PostingUser[] usersForMoviej = movieUsers.get(movieIDs[j]);
				
			}
		}
		
		
	}
}
