package sqlTOMongoDb;

import java.util.HashMap;
import java.util.List;

public class RemoveFieldTrans implements Transformer {

	@SuppressWarnings("unchecked")
	@Override
	public List<HashMap<String, Object>> transform(List<HashMap<String, Object>> data, HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("transforms");
		for (int i = 1; i < data.size(); i++) {
			data.get(i).remove(conf.get("fieldToDelete"));
		}
		return data;
	}

}
