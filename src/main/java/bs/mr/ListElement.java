package bs.mr;

public class ListElement<K extends Comparable<? super K>, V> implements
		Comparable<ListElement<K, V>> {
	private K key = null;
	private V value = null;

	public ListElement() {
		super();
	}

	public ListElement(K key, V value) {
		super();
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public int compareTo(ListElement<K, V> o) {
		return key.compareTo(o.getKey());
	}

}
