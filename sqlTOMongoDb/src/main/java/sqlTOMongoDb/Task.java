package sqlTOMongoDb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import readers.MongoLineReader;
import readers.Reader;
import readers.ReaderFactory;
import transformer.Transformer;
import transformer.TransformersFactory;
import writers.Writer;
import writers.WriterFactory;

public class Task implements Runnable {
	private Map<String, Object> params;
	private String taskName;

	public Task(String task) {
		this.taskName = task;
	}

	public Task(String taskName, Map<String, Object> parameters) {
		this.taskName = taskName;
		this.params = parameters;
	}

	public void run() {
		try {
			//HashMap<String, Object> conf = readFromMongo(taskName);
			HashMap<String, Object> conf = MongoLineReader.readFromMongo(Fields.tasks, Fields.id, taskName, "bdika");
			System.out.println(conf);
			DBObjectUtil.fixJson(conf, new ConfJsonFieldFixer(params), "");
			conf.put("params", params);
			Reader reader = ReaderFactory.create(conf);
			//Transformer trans = TransformersFactory.create(conf);
			Writer writer = WriterFactory.create(conf);

			reader.init(conf);
			writer.init(conf);
			while (reader.hasNext()) {
				List<HashMap<String, Object>> data = reader.next();
				System.out.println(data.size());
				//if (trans != null)
				//	data = trans.transform(data, conf);
				writer.write(data);
			}

			reader.close();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("failed to do task:" + taskName);
		}
	}

	private void timer(long delay,String task) throws InterruptedException {
		while(true) {
		Thread.sleep(delay);
		new Thread(new Task(task)).start();
		}
	}
	
	private void timer2(long delay,String task){
		 final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	      scheduler.scheduleAtFixedRate(new Task("copyDeltaPolicies"), 0, 30, TimeUnit.MINUTES);
	        
	}

	public static void main(String[] args) {
		new Task("SqlIncremental").run();
		// new Task("sqlPoliciesToMongo").run();
		// new Thread(new Task("timer")).start();

	}
}
