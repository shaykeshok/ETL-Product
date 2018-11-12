package readers;

import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class MongoLineReader {
	@SuppressWarnings({ "deprecation", "resource" })
	public static HashMap<String, Object> readFromMongo(String tblName, 
			String queryField,String queryValue, String dbName) {
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB(dbName);
		DBCollection table = db.getCollection(tblName);
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(queryField, queryValue);
		DBCursor cursor = table.find(searchQuery);
		BasicDBObject next = null;
		if (cursor.hasNext()) {
			next = (BasicDBObject) cursor.next();
		}
		return next;
	}
}
