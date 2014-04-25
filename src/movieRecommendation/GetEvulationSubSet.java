import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class GetEvulationSubSet {
	public static void main(String [] args) throws IOException{
		FileReader originalReader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/u1.test");
		BufferedReader obr = new BufferedReader(originalReader);
		FileReader Reader = new FileReader("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/90");
		BufferedReader br = new BufferedReader(Reader);
		
		ArrayList<String> traings = new ArrayList<String>();
		
		String originalStr = null;
		String str = null;
		
		while((str = br.readLine())!=null){
			traings.add(str);
		}
		
		FileWriter writer = new FileWriter("/Users/yangbo/Workspaces/MyEclipse 10/MovieLensRecommendation/src/movieRecommendation/evulation");
	    BufferedWriter bw = new BufferedWriter(writer);
	    
	    while((originalStr = obr.readLine())!=null){
			if(!traings.contains(originalStr)){
				bw.write(originalStr.toString());
				bw.newLine();
			}
	    	//System.out.println(originalStr);
		}
	    
	    originalReader.close();
	    obr.close();
	    Reader.close();
	    br.close();
	    bw.close();
	    writer.close();
	    
	}
}
