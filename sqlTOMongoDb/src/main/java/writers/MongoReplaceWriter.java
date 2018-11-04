package writers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import readers.Reader;
import readers.ReaderFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoReplaceWriter extends Mongo {
	private MongoClient mongo;
	private DB db;
	private DBCollection table;
	private DBCollection tempTable;

	@SuppressWarnings({ "deprecation", "unchecked" })
	public void init(HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("target");
		String collection = (String) conf.get("collection");
		String dbName = (String) conf.get("db");

		mongo = new MongoClient("localhost", 27017);
		db = mongo.getDB(dbName);
		table = db.getCollection(collection);
		tempTable = db.getCollection("temp" + collection);
	}

	public void write(List<HashMap<String, Object>> data) {
		for (int i = 0; i < data.size(); i++) {
			BasicDBObject document = new BasicDBObject();
			for (Map.Entry<String, Object> entry : data.get(i).entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				document.put(key, value);
			}
			tempTable.insert(document);

		}
	}
	

	public void close() {
		String tableName = table.getFullName();
		if (table.count() > 0) {
			table.rename("old" + table.getFullName(), true);
			tempTable.rename(tableName);
		}
		mongo.close();

	}

}
