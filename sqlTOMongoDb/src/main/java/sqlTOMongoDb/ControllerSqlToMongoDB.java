package sqlTOMongoDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

@RestController
public class ControllerSqlToMongoDB {

	@SuppressWarnings("unused")
	private final AtomicLong counter = new AtomicLong();
	public static final MongoClient mongo = new MongoClient("localhost", 27017);;
	public DB db;
	public DBCollection table;
	private static final String CONNECTIONURL = "jdbc:sqlserver://192.168.57.31:1433;database=bnsgvia;user=epr;password=epr10";

	@SuppressWarnings("deprecation")
	public ControllerSqlToMongoDB() {
		this.db = mongo.getDB("bdika");
	}

	// insert data to MongoDB
	private void insertMongoDB(List<HashMap<String, Object>> lst, String tblName) {

		this.table = db.getCollection(tblName);
		for (int i = 0; i < lst.size(); i++) {
			BasicDBObject document = new BasicDBObject();
			for (Map.Entry<String, Object> entry : lst.get(i).entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				document.put(key, value);
			}
			table.insert(document);

		}
	}

	@RequestMapping("/doTask")
	public String doTask(@RequestParam(value = "taskName") String taskName) {
		new Thread(new Task(taskName)).start();	
		return "Task:"+taskName+" is run";
		 
	}
	
	@RequestMapping("/getFromSQL")
	public String getFromSQL(
			@RequestParam(value = "tablename", defaultValue = "msp") String tablename) {

		HashMap<String, Object> innerMap = new HashMap<>();
		List<HashMap<String, Object>> lst = new ArrayList<HashMap<String, Object>>();
		String ans = "success";

		try (Connection con = DriverManager.getConnection(CONNECTIONURL);
				Statement stmt = con.createStatement();) {

			String SQL = "SELECT * FROM " + tablename;
			ResultSet rs = stmt.executeQuery(SQL);
			ResultSetMetaData rsmd = rs.getMetaData();
			int countrow = 0;
			int columnsNumber = rsmd.getColumnCount();

			while (rs.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
					innerMap.put(rsmd.getColumnName(i), rs.getObject(i));
				}
				lst.add(innerMap);

				innerMap = new HashMap<>();
				countrow++;
				if (countrow % 100 == 0){
					insertMongoDB(lst, tablename);
					lst = new ArrayList<HashMap<String, Object>>();
				}
			}
			if (countrow % 100 != 0)
				insertMongoDB(lst, tablename);

		}
		// Handle any errors that may have occurred.
		catch (SQLException e) {
			e.printStackTrace();
		}
		return ans;
	}

}