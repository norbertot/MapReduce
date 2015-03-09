package BD.Clase01;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

public class TestMapReduce {

	public static void main(String args[]) throws IOException {
		BufferedReader lector = new BufferedReader(new FileReader(
				"data.txt"));
		ArrayList<String> data = new ArrayList<String>();
		while (lector.ready()) {
			String s = lector.readLine();
			data.add(s);
			// System.out.println(s);
		}
		lector.close();
		MapFuncion mf = new MapFuncion(data);
		MapFuncion mf2 = new MapFuncion(data);
		ReduceFuncion rf = new ReduceFuncion();
		ReduceFuncion rg = new ReduceFuncion();
		ReduceFuncion rb = new ReduceFuncion();
		int i = 0;
		for (String documentContent : data) {
			// Mapper: crea tu objeto de MAP y prueba
			String[] map = mf.map(i, documentContent);
			// Reducer: crea tu objeto de Reduce, cunto se repite cada palabra
			rf.reduce(map);
			rg.reduceGnome(map);
			rb.reduceBioinformatics(map);
			i++;
			if(i>= data.size()/2){
				break;
			}
		}

		for (; i < data.size(); i++) {
			// Mapper: crea tu objeto de MAP y prueba
			String[] map = mf2.map(i, null);
			// Reducer: crea tu objeto de Reduce, cunto se repite cada palabra
			rf.reduce(map);
			rg.reduceGnome(map);
			rb.reduceBioinformatics(map);
		}

		// Tarea: 2 mappers , 1 reducer (cunto se repite cada palabra), 2
		// reducers
		System.out.println("Primer reducer");
		Set<String> keySet = rf.reduced.keySet();
		for (String key : keySet) {
			System.out.println(key + ":" + rf.reduced.get(key));
		}
		// segundo reducer, debe mostrar cuntas veces aparece la palabra genome
		System.out.println("Segundo reducer");
		keySet = rg.reduced.keySet();
		for (String key : keySet) {
			System.out.println(key + ":" + rf.reduced.get(key));
		}
		// tercer reducer, debe mostrar cuntas veces aparece la palabra
		// bioinformatics
		System.out.println("Tercer reducer");
		keySet = rb.reduced.keySet();
		for (String key : keySet) {
			System.out.println(key + ":" + rf.reduced.get(key));
		}
	}

}