package bs.mr;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

public class MapReduce<K1 extends Comparable<? super K1>, V1 extends Comparable<? super V1>, K2 extends Comparable<? super K2>, V2 extends Comparable<? super V2>> {

	private Mapper<K1, V1, K2, V2> mapperFunction = null;
	private Reducer<K2, V2, K2, V2> reducerFunction = null;
	private List<MapTask<K1, V1, K2, V2>> mappers;
	private List<Thread> mapThreads;
	private List<ReduceTask<K2, V2, K2, V2>> reducers;
	private List<Thread> reduceThreads;
	private List<ListElement<K2, V2>> sharedMemory;
	private List<ListElement<K2, V2>> output;
	private List<ListElement<K1, V1>> input;
	private boolean finished = false;

	public MapReduce(Integer numberOfMappers, Integer numberOfReducers,
			Mapper<K1, V1, K2, V2> mapperFunction,
			Reducer<K2, V2, K2, V2> reducerFunction,
			List<ListElement<K1, V1>> input) {
		this.sharedMemory = Collections
				.synchronizedList(new ArrayList<ListElement<K2, V2>>());
		this.output = Collections
				.synchronizedList(new ArrayList<ListElement<K2, V2>>());
		this.mapperFunction = mapperFunction;
		this.mapperFunction.setIntermediateMemory(sharedMemory);
		this.reducerFunction = reducerFunction;
		this.reducerFunction.setIntermediateMemory(output);
		;
		createProcesses(numberOfMappers, numberOfReducers);
		this.input = input;
	}

	public MapReduce(Integer numberOfMappers, Integer numberOfReducers,
			Mapper<K1, V1, K2, V2> mapperFunction,
			Reducer<K2, V2, K2, V2> reducerFunction) {
		this.sharedMemory = Collections
				.synchronizedList(new ArrayList<ListElement<K2, V2>>());
		this.output = Collections
				.synchronizedList(new ArrayList<ListElement<K2, V2>>());
		this.mapperFunction = mapperFunction;
		this.mapperFunction.setIntermediateMemory(sharedMemory);
		this.reducerFunction = reducerFunction;
		this.reducerFunction.setIntermediateMemory(output);
		;
		createProcesses(numberOfMappers, numberOfReducers);
	}

	public void addInput(K1 key, V1 value) {
		if (input == null) {
			input = new ArrayList<ListElement<K1, V1>>();
		}
		input.add(new ListElement<K1, V1>(key, value));
	}

	private void createProcesses(Integer numberOfMappers,
			Integer numberOfReducers) {
		setMappers(new ArrayList<MapTask<K1, V1, K2, V2>>());
		setReducers(new ArrayList<ReduceTask<K2, V2, K2, V2>>());
		for (int i = 0; i < numberOfMappers; i++) {
			getMappers().add(new MapTask<K1, V1, K2, V2>(mapperFunction));
		}
		for (int i = 0; i < numberOfReducers; i++) {
			getReducers().add(new ReduceTask<K2, V2, K2, V2>(reducerFunction));
		}
	}

	public void runMapReduce() {
		SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

		System.out.println("Mapping: "
				+ format.format(System.currentTimeMillis()));
		runMap();
		System.out.println("Reducing: "
				+ format.format(System.currentTimeMillis()));
		runReduce();
		System.out.println("Finished: "
				+ format.format(System.currentTimeMillis()));
		finished = true;
	}

	private void runReduce() {
		assignInputReducers();
		reduceThreads = new ArrayList<Thread>();
		for (ReduceTask<K2, V2, K2, V2> reduceTask : reducers) {
			Thread thread = new Thread(reduceTask);
			reduceThreads.add(thread);
			thread.start();
		}
		for (Thread reduceThread : reduceThreads) {
			try {
				reduceThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//TODO Join the map and reduce thread array
		reduceThreads.clear();
		reduceThreads = null;
		reducers.clear();
		reducers = null;
	}

	private void runMap() {
		assignInputMappers(mappers);
		mapThreads = new ArrayList<Thread>();
		for (MapTask<K1, V1, K2, V2> mapTask : mappers) {
			Thread thread = new Thread(mapTask);
			mapThreads.add(thread);
			thread.start();
		}

		for (Thread mapThread : mapThreads) {
			try {
				mapThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		mapThreads.clear();
		mapThreads = null;
		mappers.clear();
		mappers = null;
	}

	private void assignInputReducers(List<ReduceTask<K2, V2, K2, V2>> reducers2) {
		int i = 0;
		K2 currentKey = sharedMemory.get(0).getKey();
		if (sharedMemory != null && !sharedMemory.isEmpty()) {
			currentKey = sharedMemory.get(0).getKey();
		}
		for (ListElement<K2, V2> element : sharedMemory) {
			if (!currentKey.equals(element.getKey())) {
				i++;
				currentKey = element.getKey();
			}
			reducers.get(i % reducers.size()).addInput(element);
		}
		this.sharedMemory = null;
	}

	private void assignInputReducers() {
		Hashtable<K2, List<V2>> inputReducers = new Hashtable<K2, List<V2>>();
		for (ListElement<K2, V2> element : sharedMemory) {
			List<V2> list = inputReducers.get(element.getKey());
			if (list == null) {
				list = new ArrayList<V2>();
			}
			list.add(element.getValue());
			inputReducers.put(element.getKey(), list);
		}
		this.sharedMemory = null;
		int i = 0;
		for(K2 key: inputReducers.keySet()){
			reducers.get(i % reducers.size()).addInput(key, inputReducers.get(key));
			i++;
		}
		inputReducers.clear();
		inputReducers = null;
	}

	private void assignInputMappers(List<MapTask<K1, V1, K2, V2>> lMappers) {
		int i = 0;
		int j = 0;
		for (ListElement<K1, V1> element : input) {
			mappers.get(i % mappers.size()).addInput(element);
			j++;
			if (j % 2 == 0) {
				i++;
			}
		}
		input.clear();
		input = null;
	}

	private void sortSharedMemory() {
		Collections.sort(sharedMemory);
	}

	public List<ListElement<K2, V2>> getSharedMemory() {
		return this.sharedMemory;
	}

	public List<ReduceTask<K2, V2, K2, V2>> getReducers() {
		return reducers;
	}

	public void setReducers(List<ReduceTask<K2, V2, K2, V2>> reducers) {
		this.reducers = reducers;
	}

	public List<MapTask<K1, V1, K2, V2>> getMappers() {
		return mappers;
	}

	public void setMappers(List<MapTask<K1, V1, K2, V2>> mappers) {
		this.mappers = mappers;
	}

	public List<ListElement<K2, V2>> getOutput() {
		if (finished) {
			return output;
		} else {
			return null;
		}
	}
}
