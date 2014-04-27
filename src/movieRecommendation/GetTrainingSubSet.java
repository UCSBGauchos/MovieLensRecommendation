import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

public class GetTrainingSubSet {
	public static void main(String [] args) throws IOException{
		HashMap<String, Integer> cache = new HashMap<String, Integer>();
		FileReader preReader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/database");
		BufferedReader preBr = new BufferedReader(preReader);
		FileReader Reader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/database");
		BufferedReader br = new BufferedReader(Reader);
		String preStr = null;
		String str = null;
		
		String prevUser = "";
		int count = 0;
		boolean hasBegin = false;
		//each line in the input file
		while((preStr = preBr.readLine())!=null){
			StringTokenizer token = new StringTokenizer(preStr.toString(), " \t");
			String user = token.nextToken();
			if(!prevUser.equals(user)&&hasBegin == true){
				//System.out.println("User "+prevUser+" has "+count);
				cache.put(prevUser, count);
				count = 1;
			}else if(prevUser.equals(user)){
				hasBegin = true;
				count++;
			}else if(!prevUser.equals(user)&&hasBegin == false){
				hasBegin = true;
				count++;
			}
			prevUser = user;
			while(token.hasMoreTokens()){
				String movie = token.nextToken();
				String rating = token.nextToken();
				String date = token.nextToken();
				//bw.write(str.toString());
			}
			//bw.write("\n");
		}
		//System.out.println("User "+prevUser+" has "+count);
		cache.put(prevUser, count);
		//System.out.println(cache);
		//Now we know how many movies each user has rated, then we can get different percent training set
		//from the input dataset
		
		System.out.println("Please input the percentage whihc you want to input");
		BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
		String number = buffer.readLine();
		int n = Integer.parseInt(number);
		
		
		//dest of the output file
        FileWriter writer = new FileWriter("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/"+number);
        BufferedWriter bw = new BufferedWriter(writer);
		
		String prevUser2 = "";
		float percent = (float) n/100;
		int index = 0;
		while((str = br.readLine())!=null){
			StringTokenizer token = new StringTokenizer(str.toString(), " \t");
			String user = token.nextToken();
			int num = (int) (percent*cache.get(user));
			//System.out.println("num is "+num);
			if(!prevUser2.equals(user)){
				index = 0;
				prevUser2 = user;
			}
			while(token.hasMoreTokens()){
				String movie = token.nextToken();
				String rating = token.nextToken();
				String date = token.nextToken();
				//bw.write(str.toString());
			}
			if(index<num){
				bw.write(str.toString());
				bw.write("\n");
				index++;
			}
		}	
		preBr.close();
		preReader.close();
		Reader.close();
		br.close();
        bw.close();
        writer.close();

	}
}
