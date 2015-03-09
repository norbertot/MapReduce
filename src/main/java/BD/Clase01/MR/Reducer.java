package BD.Clase01.MR;

import java.util.List;

public abstract class Reducer<K1, V1 extends Comparable<? super V1>, K2 extends Comparable<? super K2>, V2>
		extends Emmiter<K1, V1, K2, V2> {
	public Reducer() {
		super();
	}

	public abstract Reducer<K1, V1, K2, V2> reduceFunction(K1 key, List<V1> values);
}
