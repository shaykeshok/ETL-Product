package connections;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoConnection {
	private MongoClient client;
	private static MongoConnection instance = new MongoConnection();
	
	private MongoConnection() {
		try {
			client = createClient();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	public static MongoConnection getInstance() {
		return instance;
	}

	private MongoClient createClient() throws UnknownHostException {
		String[] split = (Conf.get("mongo.host")).split(";");
		List<ServerAddress> ips = new ArrayList<>();
		for (int i = 0; i < split.length; i++) {
			ips.add(new ServerAddress(split[i]));
		}
		if ("true".equals(Conf.get("mongo.auth"))) {
			MongoCredential credential = MongoCredential.createMongoCRCredential("" + Conf.get("mongo.auth.user"),
					"" + Conf.get("mongo.db.name"), ("" + Conf.get("mongo.auth.password")).toCharArray());

			client = new MongoClient(ips, Arrays.asList(credential));
		} else
			client = new MongoClient(ips);
		return client;
	}

	public MongoDatabase getDb() {
		return client.getDatabase("" + Conf.get("mongo.db.name"));
	}

	public MongoCollection<Document> getCollection(String collectionName) {
		return client.getDatabase("" + Conf.get("mongo.db.name")).getCollection(collectionName);
	}

	public MongoCollection<Document> getCol(String table) {
		return client.getDatabase("" + Conf.get("mongo.db.name")).getCollection(table);
	}
}
