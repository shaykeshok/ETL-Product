package readers;

import java.util.HashMap;
import java.util.List;

public class SqlIncrementalReader implements Reader{

	@Override
	public void init(HashMap<String, Object> conf) {
		
	}
	
	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public List<HashMap<String, Object>> next() {
		return null;
	}


	@Override
	public void close() {
		
	}

}
