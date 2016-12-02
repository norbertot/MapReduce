package bs.mr;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class ReduceTask<K1 extends Comparable<? super K1>, V1 extends Comparable<? super V1>, K2 extends Comparable<? super K2>, V2>
		extends Worker<K1,V1,K2,V2> implements Runnable {

	private Reducer<K1, V1, K2, V2> reduceFunction = null;
	private Hashtable<K2, List<V2>> input = null;

	public ReduceTask(Reducer<K1, V1, K2, V2> reduceFunction,
			Hashtable<K2, List<V2>> input) {
		this.reduceFunction = reduceFunction;
		this.input = input;
	}

	public ReduceTask(Reducer<K1, V1, K2, V2> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	public void run() {
		for (K2 key : input.keySet()) {
			reduceFunction.reduceFunction(key, input.get(key));
		}
	}

	public void addInput(ListElement<K2, V2> element) {
		this.addInput(element.getKey(), element.getValue());

	}

	public void addInput(K2 key, V2 value) {
		if (input == null) {
			input = new Hashtable<K2, List<V2>>();
		}
		List<V2> list = input.get(key);
		if (list == null) {
			list = new ArrayList<V2>();
			list.add(value);
			input.put(key, list);
		} else {
			list.add(value);
		}
	}

	public void addInput(K2 key, List<V2> value) {
		if (input == null) {
			input = new Hashtable<K2, List<V2>>();
		}
		input.put(key, value);
	}
}