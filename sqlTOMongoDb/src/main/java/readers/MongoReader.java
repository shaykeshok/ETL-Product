package readers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoReader implements Reader {

	private int batchSize;
	private MongoClient mongo;
	private DB db;
	private DBCollection table;
	private DBCursor cursor;

	@SuppressWarnings("unchecked")
	public void init(HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("source");
		String connectionString = (String) conf.get("connectionString");
		int port = (int) conf.get("port");
		batchSize = (int) conf.get("batchSize");
		DBObject query = (DBObject) conf.get("query");

		mongo = new MongoClient(connectionString, port);
		db = mongo.getDB((String) conf.get("db"));
		table = db.getCollection(((String) conf.get("table")));
		cursor = table.find(query);

	}

	@Override
	public boolean hasNext() {
		if (cursor.hasNext())
			return true;
		return false;
	}

	@Override
	public List<HashMap<String, Object>> next() {
		List<HashMap<String, Object>> lst = new ArrayList<HashMap<String, Object>>();
		int count = 0;
		while (cursor.hasNext() && count < batchSize) {
			lst.add((HashMap<String, Object>) cursor.next());
			count++;
		}
		return lst;
	}

	@Override
	public void close() {
		return;
	}

}
