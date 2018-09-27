package sqlTOMongoDb;

import java.util.HashMap;
import java.util.List;

public class AddFieldTrans implements Transformer {

	@SuppressWarnings({ "unchecked"})
	public List<HashMap<String, Object>> transform(List<HashMap<String, Object>> data, HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("transforms");
		String fieldName=(String) conf.get("fieldName");
		String fieldValue=(String)conf.get("fieldValue");
		for (int i = 0; i < data.size(); i++) {
			data.get(i).put(fieldName,fieldValue );
		}
		return data;
	}

}
