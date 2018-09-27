package sqlTOMongoDb;

import java.util.HashMap;
import java.util.List;

public interface Writer {


	void write(List<HashMap<String, Object>> data);

	void close();

	void init(HashMap<String, Object> conf);

}
