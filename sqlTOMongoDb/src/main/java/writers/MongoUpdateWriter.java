package writers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;

public class MongoUpdateWriter extends Mongo {
	private MongoClient mongo;
	private MongoDatabase db;
	private MongoCollection<Document> table;
	private String sourceJoinField;
	private String targetJoinField;

	@SuppressWarnings({ "unchecked", "deprecation" })
	@Override
	public void init(HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("target");
		String collection = (String) conf.get("collection");
		String dbName = (String) conf.get("db");
		sourceJoinField = (String) conf.get("sourceJoinField");
		targetJoinField = (String) conf.get("targetJoinField");

		mongo = new MongoClient("localhost", 27017);
		db = mongo.getDatabase(dbName);
		table = db.getCollection(collection);
	}

	@Override
	public void write(List<HashMap<String, Object>> data) {
		List<UpdateOneModel<Document>> requests=new ArrayList<>();
		for (int i = 0; i < data.size(); i++) {
			BasicDBObject document = new BasicDBObject(data.get(i));
			Object remove = document.remove(sourceJoinField);

			UpdateOneModel<Document> updateOneModel = new UpdateOneModel<Document>(new Document(targetJoinField, remove), new Document("$set", data.get(i)));
			requests.add(updateOneModel);
			//			table.update(new BasicDBObject(targetJoinField, remove), new BasicDBObject("$set", data.get(i)));
			
		}
		table.bulkWrite(requests);
	}

}
