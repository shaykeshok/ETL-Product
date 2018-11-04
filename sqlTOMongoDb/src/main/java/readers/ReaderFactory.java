package readers;
import java.util.HashMap;

public class ReaderFactory {

	@SuppressWarnings("unchecked")
	public static Reader create(HashMap<String, Object> conf) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		HashMap<String, Object> source = (HashMap<String, Object>) conf.get("source");
		String readerType = (String) source.get("class");
		System.out.println(source);
		Reader r = (Reader) Class.forName("readers." + readerType).newInstance();
		return r;
	}
}
