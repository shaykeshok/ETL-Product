package sqlTOMongoDb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public interface Reader extends Iterator<List<HashMap<String, Object>>> {

	void init(HashMap<String, Object> conf);

	void close();

}
