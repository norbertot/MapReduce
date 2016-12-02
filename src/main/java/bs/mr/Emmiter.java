package bs.mr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Emmiter<K1, V1 extends Comparable<? super V1>, K2 extends Comparable<? super K2>, V2> {
	protected List<ListElement<K2, V2>> intermediateMemory;

	// TODO test if this is memory efficient
	public void emit(K2 key, V2 value) {
		if (intermediateMemory == null) {
			intermediateMemory = Collections
					.synchronizedList(new ArrayList<ListElement<K2, V2>>());
		}
		intermediateMemory.add(new ListElement<K2, V2>(key, value));
	}

	public void setIntermediateMemory(
			List<ListElement<K2, V2>> intermediateMemory) {
		this.intermediateMemory = intermediateMemory;
	}

}
