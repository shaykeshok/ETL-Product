package sqlTOMongoDb;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import readers.Reader;
import readers.ReaderFactory;
import transformer.Transformer;
import transformer.TransformersFactory;
import writers.Writer;
import writers.WriterFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class Task implements Runnable {

	private String taskName;

	public Task(String task) {
		this.taskName = task;
	}

	public void run() {
		try {
			if (taskName.equals("timer"))
				timer();
			HashMap<String, Object> conf = readFromMongo(taskName);
			System.out.println(conf);
			Reader reader = ReaderFactory.create(conf);
			Transformer trans = TransformersFactory.create(conf);
			Writer writer = WriterFactory.create(conf);

			reader.init(conf);
			writer.init(conf);
			while (reader.hasNext()) {
				List<HashMap<String, Object>> data = reader.next();
				System.out.println(data.size());
				if (trans != null)
					data = trans.transform(data, conf);
				writer.write(data);
			}
				
			reader.close();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("failed to do task:" + taskName);
		}
	}

	//1) with what you know.
	//2) Scheduler
	private void timer() throws InterruptedException {
		wait(1800000);
		new Thread(new Task("copyDeltaPolicies")).start();
		timer();
	}
	
	private void timer2(){
		 final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	      scheduler.scheduleAtFixedRate(new Task("copyDeltaPolicies"), 0, 30, TimeUnit.MINUTES);
	        
	}
	

	@SuppressWarnings({ "deprecation", "resource" })
	private HashMap<String, Object> readFromMongo(String taskName) {
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("bdika");
		DBCollection table = db.getCollection("tasks");
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("_id", taskName);

		DBCursor cursor = table.find(searchQuery);
		BasicDBObject next = null;
		if (cursor.hasNext()) {
			next = (BasicDBObject) cursor.next();
		}
		return next;
	}

	public static void main(String[] args) {
		 new Task("mongoStreetsToMongo").run();
		// new Task("sqlUpdateToMongo").run();
		//new Thread(new Task("timer")).start();	
	
		
		
		
	}
}
