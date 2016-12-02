package bs.mr;

import java.util.ArrayList;

public class MapTask<K1 extends Comparable<? super K1>, V1 extends Comparable<? super V1>, K2 extends Comparable<? super K2>, V2 extends Comparable<? super V2>>
		extends Worker<K1, V1, K2, V2> implements Runnable {
	private Mapper<K1, V1, K2, V2> mapper = null;
	private ArrayList<ListElement<K1, V1>> input = null;

	public MapTask(Mapper<K1, V1, K2, V2> mapper,
			ArrayList<ListElement<K1, V1>> input) {
		this.mapper = mapper;
		this.input = input;
	}

	public MapTask(Mapper<K1, V1, K2, V2> mapper) {
		this.mapper = mapper;
	}

	public void run() {
		for (ListElement<K1, V1> element : input) {
			mapper.mapFunction(element.getKey(), element.getValue());
		}

	}

	public void addInput(K1 key, V1 value) {
		this.addInput(new ListElement<K1, V1>(key, value));
	}

	public void addInput(ListElement<K1, V1> element) {
		if (input == null) {
			input = new ArrayList<ListElement<K1, V1>>();
		}
		input.add(element);
	}

}
