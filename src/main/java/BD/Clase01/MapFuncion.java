package BD.Clase01;

import java.util.ArrayList;

public class MapFuncion implements Runnable{
	private ArrayList<String> srcDir = null;
	private Integer docs = null;

	public MapFuncion(ArrayList<String> dir) {
		this.srcDir = dir;
	}

	public String[] map(Integer documentName, String documentContent) {
		if (documentContent == null) {
			documentContent = srcDir.get(documentName);
		}
		String[] split = documentContent.split(" ");
		return split;
	}

	public void run() {
		map(this.docs,null);
		
		
	}

	public Integer getDoc() {
		return docs;
	}

	public void setDoc(Integer doc) {
		this.docs = doc;
	}
}
