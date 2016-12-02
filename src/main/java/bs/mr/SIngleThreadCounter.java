package bs.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;

public class SIngleThreadCounter {
	public static void main(String[] args) {
		File folder = new File("/Users/norberto/Downloads/texto");
		SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" );
		ArrayList<ListElement<String,String>> list = new ArrayList<ListElement<String,String>>();
		System.out.println(format.format(System.currentTimeMillis()));
		Hashtable<String, Integer> output = new Hashtable<String, Integer>();
		for(File file: folder.listFiles()){
			try (BufferedReader reader = new BufferedReader(new FileReader(file))){
				StringBuffer sb = new StringBuffer();
				String line = null;
				while((line = reader.readLine()) != null){
					sb.append(line);
				}
				list.add(new ListElement<String,String>(file.getName(), sb.toString()));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(format.format(System.currentTimeMillis()));
		for(ListElement<String,String> element: list){
			String text = element.getValue();
			String[] split = text.split(" ");
			for(String word:split){
				Integer integer = output.get(word);
				if(integer == null){
					output.put(word, 1);
				}else{
					integer++;
					output.put(word, integer);
				}
			}
			
		}
		
		System.out.println(format.format(System.currentTimeMillis()));
		int i = 0;
		for(String key: output.keySet()){
			i++;
			System.out.println("Key: "+ key);
			System.out.println("Value: "+ output.get(key));
			if(i >= 11){
				break;
			}
		}

	}

}
