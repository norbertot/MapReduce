package bs.mr;

import org.apache.commons.lang3.StringUtils;

public class MapperImplementation extends
		Mapper<String, String, String, Integer> {

	private static final String[] stopSymbols = { "(", ")", ",", ".", ";", ":",
			"-", "+", "_", "{", "}", "[", "]" };
	private static final String[] stopWords = { "the", "" };

	@Override
	public Mapper<String, String, String, Integer> mapFunction(String key,
			String value) {
		value = cleanText(value);
		String[] split = value.split(" ");
		value = null;
		for (String word : split) {
			word = cleanWord(word);
			if (word == null) {
				continue;
			}
			emit(word, 1);
		}
		return this;
	}

	private String cleanText(String value) {
		value = StringUtils.lowerCase(value);
		value = StringUtils.stripAccents(value);
		return value;
	}

	private String cleanWord(String word) {
		word = StringUtils.stripToNull(word);
		if (word != null) {
			for (String stopSymbol : stopSymbols) {
				word = StringUtils.replaceChars(word, stopSymbol, "");
			}
			for (String stopWord : stopWords) {
				if (stopWord.equals(word)) {
					word = null;
				}
			}
		}
		return word;
	}

}
