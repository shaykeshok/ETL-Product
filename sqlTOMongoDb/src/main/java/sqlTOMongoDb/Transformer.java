package sqlTOMongoDb;

import java.util.HashMap;
import java.util.List;

public interface Transformer {

	List<HashMap<String, Object>> transform(List<HashMap<String, Object>> data, HashMap<String,Object> conf);

}
