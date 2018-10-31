package transformer;

import java.util.HashMap;

public class TransformersFactory {

	@SuppressWarnings("unchecked")
	public static Transformer create(HashMap<String, Object> conf) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		HashMap<String, Object> transforms = (HashMap<String, Object>) conf.get("transforms");
		String TransformerType = (String) transforms.get("class");
		System.out.println(TransformerType);
		Transformer t= (Transformer) Class.forName("sqlTOMongoDb."+TransformerType).newInstance();
		return t;
	}
	
}
