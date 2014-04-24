import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

public class GetTrainingSubSet {
	public static void main(String [] args) throws IOException{
		FileReader reader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/u1.test");
		BufferedReader br = new BufferedReader(reader);
		String str = null;
		//dest of the output file
        FileWriter writer = new FileWriter("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/90");
        BufferedWriter bw = new BufferedWriter(writer);
		
		//read from the input file
		while((str = br.readLine())!=null){
			StringTokenizer token = new StringTokenizer(str.toString(), " \t");
			while(token.hasMoreTokens()){
				String user = token.nextToken();
				String movie = token.nextToken();
				String rating = token.nextToken();
				String date = token.nextToken();
				bw.write(str.toString());
			}
			bw.write("\n");
		}
		br.close();
        reader.close();
        bw.close();
        writer.close();
        
     
        
        
	}
}
