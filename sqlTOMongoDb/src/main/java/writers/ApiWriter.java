package writers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sqlTOMongoDb.Fields;

public class ApiWriter implements Writer {
	Map<String, Object> params = new HashMap<String, Object>();
	List<Map<String, Object>> lst = new ArrayList<Map<String, Object>>();

	@Override
	public void init(HashMap<String, Object> conf) {
	}

	@Override
	public void write(List<HashMap<String, Object>> data) {
		lst.addAll(data);
	}

	@Override
	public void close() {
		params.put(Fields.result, lst);
	}

}
