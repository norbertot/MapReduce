package bs.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class TestMapReduce {
	
	public static void main(String [] args){
		File folder = new File("/Users/norberto/Downloads/articulos_exam/txt");
		MapReduce<String, String, String, Integer> mapReduce = new MapReduce<String, String, String, Integer>(2, 2,new MapperImplementation(), new ReducerImplementation());
		//TODO test if this can be done parallel or not(That is if it is faster, this of course for one disk implementation)
		SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" );
		System.out.println(format.format(System.currentTimeMillis()));
		for(File file: folder.listFiles()){
			try (BufferedReader reader = new BufferedReader(new FileReader(file))){
				StringBuffer sb = new StringBuffer();
				String line = null;
				while((line = reader.readLine()) != null){
					sb.append(line);
				}
				mapReduce.addInput(file.getName(), sb.toString());
				sb = null;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(format.format(System.currentTimeMillis()));
		mapReduce.runMapReduce();
		System.out.println(format.format(System.currentTimeMillis()));
		int i = 0;
		for(ListElement<String, Integer> element: mapReduce.getOutput()){
			System.out.println("Key: " + element.getKey());
			System.out.println("Value: " + element.getValue());
			i++;
			if(i >= 11){
				break;
			}
		}
	}

}
