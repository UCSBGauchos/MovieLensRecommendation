import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UserKNNMapper extends MapReduceBase implements Mapper<LongWritable, PostingMovieArrayWritable, LongWritable, UserNeighbourArrayWritable>{
	
	//movieID <userID, uverACG, rate>
	public HashMap<Long, PostingMovie[]> userMovies;
	public HashMap<Long, ArrayList<UserNeighbour>> similarityNieghbour = new HashMap<Long, ArrayList<UserNeighbour>>();
	
	public void configure(JobConf job) {
		
	}
	
	public void map(LongWritable unusedKey, PostingMovieArrayWritable unusedValue, OutputCollector<LongWritable, UserNeighbourArrayWritable> output, Reporter reporter) throws IOException {
		Iterator<Long> itr = similarityNieghbour.keySet().iterator();
		while(itr.hasNext()){
			long userID = itr.next();
			ArrayList<UserNeighbour> neighbourhood = similarityNieghbour.get(userID);
			UserNeighbour[] toArray = new UserNeighbour[neighbourhood.size()];
			neighbourhood.toArray(toArray);// debug this
			Arrays.sort(toArray);// sorting checked
			output.collect(new LongWritable(userID), new UserNeighbourArrayWritable(toArray));
		}
	}
	
	public void compareOwn(){
		int userNum = userMovies.keySet().size();
		Long[] userIDs = new Long[userNum];
		userMovies.keySet().toArray(userIDs);
		
		//get the usetPostingInfo for each pair movie i and j
		for(int i=0; i<userNum; i++){
			PostingMovie[] moviesForUseri = userMovies.get(userIDs[i]);
			for(int j= 0; j<userNum; j++){
				if(i!=j){
					PostingMovie[] moviesForUserj = userMovies.get(userIDs[j]);
					//here use movieI, movieJ, userInfoForMovieI, userInfoForMovieJ to compute. 
					compute(userIDs[i], userIDs[j], moviesForUseri, moviesForUserj, true);
				}
			}
		}
		
	}
	
//	public void compareWithOthers(Reader reader) throws IOException{
//		int movieNum = movieUsers.keySet().size();
//		LongWritable key = new LongWritable();
//		PostingUserArrayWritable value = new PostingUserArrayWritable();
//		
//		Long [] movieIDs = new Long[movieNum];
//		movieUsers.keySet().toArray(movieIDs);
//		for(int i=0; i<movieNum-1; i++){
//			PostingUser[] usersForMoviei = movieUsers.get(movieIDs[i]);
//			while(reader.next(key, value)){
//				if(movieIDs[i]<key.get()){
//					compute(movieIDs[i], key.get(), usersForMoviei, value.getPosting(), false);
//				}
//			}
//		}
//	}
	
	
	
	//four paras
	public void compute(long iID, long jID, PostingMovie[] moviesForUseri, PostingMovie[] moviesForUserj, Boolean own){
		int indexForI = 0;
		int indexForJ = 0;
		float [] calculationResult = new float[3];
		while((indexForI<moviesForUseri.length)&&(indexForJ<moviesForUserj.length)){
			if(moviesForUseri[indexForI].movieID<moviesForUserj[indexForJ].movieID){
				indexForI++;
			}else if(moviesForUseri[indexForI].movieID>moviesForUserj[indexForJ].movieID){
				indexForJ++;
			}else{
				// + songjUsers[jPoint].avgRating + ")");
				calculationResult[0] += ((moviesForUseri[indexForI].rate - moviesForUseri[indexForI].avgRating) * (moviesForUserj[indexForJ].rate - moviesForUserj[indexForJ].avgRating));

				calculationResult[1] += Math.pow((moviesForUserj[indexForJ].rate - moviesForUserj[indexForJ].avgRating),
						2);
				calculationResult[2] += Math.pow((moviesForUseri[indexForI].rate - moviesForUseri[indexForI].avgRating),
						2);
				indexForI++;
				indexForJ++;
				
			}
		}
		float weightIJ = (float) (calculationResult[0] / Math.sqrt(calculationResult[1] * calculationResult[2]));
		if(calculationResult[0]!=0){
//			if(own){
//				addNeighbour(iID, jID, weightIJ);
//				addNeighbour(jID, iID, weightIJ);
//			}else{
//				addNeighbour(iID, jID, weightIJ);
//			}
			//get wij
			if(similarityNieghbour.containsKey(iID)){
				ArrayList<UserNeighbour> iNeighbourhood = similarityNieghbour.get(iID);
				iNeighbourhood.add(new UserNeighbour(jID, weightIJ));
			}else{
				ArrayList<UserNeighbour> iNeighbourhood = new ArrayList<UserNeighbour>();
				iNeighbourhood.add(new UserNeighbour(jID, weightIJ));
				similarityNieghbour.put(iID, iNeighbourhood);
			}
		}	
	}
	
	
//	public void addNeighbour(long iId, long jId, float weightIJ) {
//		Neighbour n = new Neighbour(jId, weightIJ);
//		if (similarityNieghbour.containsKey(iId)) {
//			SortedArrayList<Neighbour> iNeighbourhood = similarityNieghbour.get(iId);
//			iNeighbourhood.add(new Neighbour(jId, weightIJ));// do we need to put
//			// again?
//		} else {
//			SortedArrayList<Neighbour> iNeighbourhood = new SortedArrayList<Neighbour>();
//			iNeighbourhood.add(new Neighbour(jId, weightIJ));
//			similarityNieghbour.put(iId, iNeighbourhood);
//		}
//	}
	
	
	

}

