package bs.mr;


public abstract class Mapper<K1, V1 extends Comparable<? super V1>, K2 extends Comparable<? super K2>, V2> extends Emmiter<K1, V1, K2, V2>{
	public Mapper() {
		super();
	}

	public abstract Mapper<K1, V1, K2, V2> mapFunction(K1 key, V1 value);

}
