package BD.Clase01;

import java.util.HashMap;

public class ReduceFuncion {
	public HashMap<String, Integer> reduced;
	
	public ReduceFuncion(){
		reduced = new HashMap<String, Integer>();
	}
	
	public void reduce(String [] pc){
		for(String wordInPC: pc){
			Integer integer = reduced.get(wordInPC);
			if(integer == null){
				reduced.put(wordInPC, 1);
			}else{
				Integer cuenta = reduced.get(wordInPC);
				//TODO check if reference increments cuenta
				cuenta++;
				reduced.put(wordInPC, cuenta);
				
			}
		}
	}

	public void reduceGnome(String [] pc){
		for(String wordInPC: pc){
			if(wordInPC.equals("genome")){
				Integer integer = reduced.get(wordInPC);
				if(integer == null){
					reduced.put(wordInPC, 1);
				}else{
					Integer cuenta = reduced.get(wordInPC);
					//TODO check if reference increments cuenta
					cuenta++;
					reduced.put(wordInPC, cuenta);
					
				}				
			}
		}
	}

	public void reduceBioinformatics(String [] pc){
		for(String wordInPC: pc){
			if(wordInPC.equals("bioinformatics")){
				Integer integer = reduced.get(wordInPC);
				if(integer == null){
					reduced.put(wordInPC, 1);
				}else{
					Integer cuenta = reduced.get(wordInPC);
					//TODO check if reference increments cuenta
					cuenta++;
					reduced.put(wordInPC, cuenta);
					
				}				
			}
		}
	}
	

}
