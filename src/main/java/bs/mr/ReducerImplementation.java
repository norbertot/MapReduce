package bs.mr;

import java.util.List;

public class ReducerImplementation extends
		Reducer<String, Integer, String, Integer> {

	@Override
	public Reducer<String, Integer, String, Integer> reduceFunction(String key,
			List<Integer> values) {
		Integer sum = 0;
		for (Integer value : values) {
			sum += value;
		}
		emit(key,sum);
		return this;
	}

}
