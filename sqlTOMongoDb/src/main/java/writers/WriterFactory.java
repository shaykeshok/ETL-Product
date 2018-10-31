package writers;

import java.util.HashMap;

public class WriterFactory {

	@SuppressWarnings("unchecked")
	public static Writer create(HashMap<String, Object> conf) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		HashMap<String, Object> target = (HashMap<String, Object>) conf.get("target");
		String writerType = (String) target.get("class");
		System.out.println(writerType);
		Writer w= (Writer) Class.forName("sqlTOMongoDb."+writerType).newInstance();
		return w;
	}
	

}
