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
				//here use movieI, movieJ, userInfoForMovieI, userInfoForMovieJ to compute. 
				compute(movieIDs[i], movieIDs[j], usersForMoviei, usersForMoviej, true);
			}
		}
		
	}
	
	public void compareWithOthers(Reader reader) throws IOException{
		int movieNum = movieUsers.size();
		LongWritable key = new LongWritable;
		PostingUserArrayWritable value = new PostingUserArrayWritable();
		Long [] movieIDs = new Long[movieNum];
		movieUsers.keySet().toArray(movieIDs);
		for(int i=0; i<movieNum-1; i++){
			PostingUser[] usersForMoviei = movieUsers.get(movieIDs[i]);
			while(reader.next(key, value)){
				if(movieIDs[i]<key.get()){
					compute(movieIDs[i], movieIDs[j], usersForMoviei, value.getPosting(), false);
				}
			}
		}
	}
	
	public void dumpNeighbours(OutputCollector output) throws IOException{
		Iterator<Long> itr = similarityNieghbour.keySet().iterator();
		while(itr.hasNext()){
			long movieID = itr.next();
			SortedArrayList<Neighbour> neighbourhood = similarityNieghbour.get(movieID);
			Neighbour[] toArray = new Neighbour[neighbourhood.size()];
			neighbourhood.toArray(toArray);// debug this
			output.collect(new LongWritable(movieID), new NeighbourArray(toArray));
		}
	}
	
	//four paras
	public void compute(long iID, long jID, PostingUser[] usersForMoviei, PostingUser[] usersForMoviej, Boolean own){
		int indexForI = 0;
		int indexForJ = 0;
		double [] calculationResult = new double[3];
		while((indexForI<usersForMoviei.length)&&(indexForJ<usersForMoviej.length)){
			if(usersForMoviei[indexForI].userID<usersForMoviej[indexForJ].userID){
				indexForI++;
			}else if(usersForMoviei[indexForI].userID>usersForMoviej[indexForJ].userID){
				indexForJ++;
			}else{
				// + songjUsers[jPoint].avgRating + ")");
				calculationResult[0] += ((usersForMoviei[indexForI].rate - usersForMoviej[indexForJ].avgRating) * (usersForMoviej[indexForJ].rate - usersForMoviej[indexForJ].avgRating));

				calculationResult[1] += Math.pow((usersForMoviei[indexForI].rate - usersForMoviej[indexForJ].avgRating),
						2);
				calculationResult[2] += Math.pow((usersForMoviei[indexForI].rate - usersForMoviej[indexForJ].avgRating),
						2);
			}
		}
		float weightIJ = (float) (calculationResult[0] / Math.sqrt(calculationResult[1] * calculationResult[2]));
		if(calculationResult[0]!=0){
			if(own){
				addNeighbour(iID, jID, weightIJ);
				addNeighbour(indexForJ, jID, weightIJ);
			}else{
				addNeighbour(iID, jID, weightIJ);
			}
		}	
	}
	public void addNeighbour(long iId, long jId, float weightIJ) {
		Neighbour n = new Neighbour(jId, weightIJ);
		if (similarityNieghbour.containsKey(iId)) {
			SortedArrayList<Neighbour> iNeighbourhood = similarityNieghbour.get(iId);
			iNeighbourhood.add(new Neighbour(jId, weightIJ));// do we need to put
			// again?
		} else {
			SortedArrayList<Neighbour> iNeighbourhood = new SortedArrayList<Neighbour>();
			iNeighbourhood.add(new Neighbour(jId, weightIJ));
			similarityNieghbour.put(iId, iNeighbourhood);
		}
	}
	
	//map, usr dump to add the result to the output
	public void map(LongWritable arg0, PostingUserArrayWritable arg1,
			OutputCollector<LongWritable, NeighbourArray> out, Reporter arg3)
			throws IOException {
		dumpNeighbours(out);
	}

}
