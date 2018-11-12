package readers;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import sqlTOMongoDb.Fields;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class SqlIncrementalReader extends SqlReader {
	private int batchSize;
	private boolean backupResultSet;
	private ResultSet rs;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	private Object max;
	private Object oldValue;

	public void init(HashMap<String, Object> conf) {
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("bdika");
		DBCollection table = db.getCollection("config");
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(Fields.id, "SqlIncremental");
		DBCursor cursor = table.find(searchQuery);
		BasicDBObject next = null;
		if (cursor.hasNext()) {
			next = (BasicDBObject) cursor.next();
		}
		max = next.get("lastModified");
		oldValue = next.get("lastModified");
		super.init(conf);
	}

	public List<HashMap<String, Object>> next() {
		ResultSetMetaData rsmd = null;
		int columnsNumber;
		int countRow = batchSize;
		List<HashMap<String, Object>> lst = new ArrayList<HashMap<String, Object>>();
		try {
			rsmd = rs.getMetaData();
			columnsNumber = rsmd.getColumnCount();
			while (backupResultSet && countRow > 0) {
				if (rs.next()) {
					HashMap<String, Object> innerMap = new HashMap<>();
					for (int i = 1; i <= columnsNumber; i++) {
						innerMap.put(rsmd.getColumnName(i), rs.getObject(i));
						if (rsmd.getColumnName(i).equals("lastModified"))
							maxValue(innerMap.get(i));
					}
					lst.add(innerMap);
					countRow--;
				} else
					backupResultSet = false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return lst;

	}

	@Override
	public void close() {
		super.close();
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("bdika");
		DBCollection table = db.getCollection("config");
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(Fields.id, "SqlIncremental");

		DBCursor cursor = table.find(searchQuery);
		BasicDBObject next = null;
		while (cursor.hasNext()) {
			next = (BasicDBObject) cursor.next();
		}
		/*
		 * BasicDBObject newDocument = new BasicDBObject();
		 * newDocument.put("lastModified", max); table.remove(searchQuery);
		 * table.insert(newDocument);
		 */
		BasicDBObject query = new BasicDBObject();
		query.put("lastModified", oldValue);

		BasicDBObject newDocument = new BasicDBObject();
		newDocument.put("lastModified", max);

		BasicDBObject updateObj = new BasicDBObject();
		updateObj.put("$set", newDocument);

		table.update(query, updateObj);
	}

	public void maxValue(Object value) {

		if (value instanceof Number)
			if (max == null || ((Number) max).intValue() < ((Number) value).intValue())
				max = ((Number) value).intValue();
		if (value instanceof Date) {
			if (max == null || ((Date) max).before((Date) value))
				max = value;
		} else {
			max = value;
		}
	}
}
