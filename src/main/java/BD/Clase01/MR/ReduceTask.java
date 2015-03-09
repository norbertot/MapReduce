package BD.Clase01.MR;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class ReduceTask<K1 extends Comparable<? super K1>, V1 extends Comparable<? super V1>, K2 extends Comparable<? super K2>, V2>
		extends Worker<K1,V1,K2,V2> implements Runnable {

	private Reducer<K1, V1, K2, V2> reduceFunction = null;
	private Hashtable<K1, List<V1>> input = null;

	public ReduceTask(Reducer<K1, V1, K2, V2> reduceFunction,
			Hashtable<K1, List<V1>> input) {
		this.reduceFunction = reduceFunction;
		this.input = input;
	}

	public ReduceTask(Reducer<K1, V1, K2, V2> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	public void run() {
		for (K1 key : input.keySet()) {
			reduceFunction.reduceFunction(key, input.get(key));
		}
	}

	public void addInput(ListElement<K1, V1> element) {
		this.addInput(element.getKey(), element.getValue());

	}

	public void addInput(K1 key, V1 value) {
		if (input == null) {
			input = new Hashtable<K1, List<V1>>();
		}
		List<V1> list = input.get(key);
		if (list == null) {
			list = new ArrayList<V1>();
			list.add(value);
			input.put(key, list);
		} else {
			list.add(value);
		}
	}
}