package writers;

import com.mongodb.MongoClient;

public abstract class Mongo implements Writer {
	private MongoClient mongo;

	public void close() {
		mongo.close();
	}
}
