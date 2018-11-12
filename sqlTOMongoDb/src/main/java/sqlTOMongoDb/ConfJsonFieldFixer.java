package sqlTOMongoDb;

import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

@SuppressWarnings("unused")
public class ConfJsonFieldFixer implements JsonFieldFixer {

	private Map<String, Object> params;

	public ConfJsonFieldFixer(Map<String, Object> params) {
		this.params = params;
	}

	@Override
	public boolean needToFixValue(String key) {
		return false;
	}

	@Override
	public Object fixValue(Object object) {
		String par = "+param.";
		if (object instanceof String) {
			String str = (String) object;
			if (str.startsWith(par))
				return DBObjectUtil.getInnerField(str.substring(par.length()), params);

			int indexOf = str.indexOf(par);
			while (indexOf >= 0) {
				int end = str.indexOf("+", indexOf + 1);
				String key = str.substring(indexOf + 8, end);
				Object globalProperty = DBObjectUtil.getInnerField(key, params);
				if (str.length() == end && indexOf == 0)
					return globalProperty;
				str = str.substring(0, indexOf) + globalProperty + str.substring(end + 1);
				indexOf = str.indexOf(par);

			}

			indexOf = str.indexOf("+config.");
			while (indexOf >= 0) {
				int end = str.indexOf("+", indexOf + 1);
				String key = str.substring(indexOf + 8, end);
				Object globalProperty = ConfigurationLoader.getInstance().getGlobalProperty(key);
				if (str.length() == end && indexOf == 0)
					return globalProperty;
				str = str.substring(0, indexOf) + globalProperty + str.substring(end + 1);
				indexOf = str.indexOf("+config.");
			}

			return str;
		}
		return object;
	}

	@Override
	public boolean needToFixKey(String string) {
		return false;
	}

	@Override
	public String fixKey(String string) {
		return null;
	}
	
	public Object getValue(String key){
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("bdika");
		DBCollection table = db.getCollection("config");
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(Fields.id, key);

		DBCursor cursor = table.find(searchQuery);
		BasicDBObject next = null;
		if (cursor.hasNext()) {
			next = (BasicDBObject) cursor.next();
		}
		
		return next.get(1); 
	}
}
