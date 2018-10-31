package transformer;

import java.util.HashMap;
import java.util.List;

public class RenameFieldTrans implements Transformer {

	@SuppressWarnings({ "unchecked" })
	@Override
	public List<HashMap<String, Object>> transform(List<HashMap<String, Object>> data, HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("transforms");
		for (int i = 0; i < data.size(); i++) {
			data.get(i).put((String) conf.get("newfieldName"), data.get(i).remove(conf.get("fieldToRename")));
		}
		return data;
	}

}
