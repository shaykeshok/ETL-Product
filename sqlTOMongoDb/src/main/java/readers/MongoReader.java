package readers;

import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class MongoReader implements Reader {

	private int batchSize;
	private MongoClient mongo;
	private DB db;

	@SuppressWarnings("unchecked")
	public void init(HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("source");
		String driverClass = (String) conf.get("driverClass");
		String connectionString = (String) conf.get("connectionString");
		int port = (int) conf.get("port");
		//String dbName=(String) conf.get("db");
		batchSize = (int) conf.get("batchSize");
		String query = (String) conf.get("query");
		
		mongo = new MongoClient(connectionString, port);
		db = mongo.getDB((String) conf.get("db"));
		BasicDBObject searchQuery = new BasicDBObject();
		//searchQuery.put("_id", taskName);
		DBCursor cursor = table.find(searchQuery);
		
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
		
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
