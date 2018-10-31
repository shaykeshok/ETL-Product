package readers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SqlReader implements Reader {

	private Connection con;
	private Statement stmt;
	private ResultSet rs;
	private boolean backupResultSet;
	private int batchSize;

	@SuppressWarnings("unchecked")
	@Override
	public void init(HashMap<String, Object> conf) {
		conf = (HashMap<String, Object>) conf.get("source");
		String driverClass = (String) conf.get("driverClass");
		String connectionString = (String) conf.get("connectionString");
		batchSize = (int) conf.get("batchSize");
		String query = (String) conf.get("query");
		backupResultSet=true;
		try {
			// Class.forName(driverClass);
			con = DriverManager.getConnection(connectionString);
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
	//	try {
			if (backupResultSet)
				return true;
	//	} catch (SQLException e) {
	//		e.printStackTrace();
	//	}

		return false;
	}

	@Override
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
						}
						lst.add(innerMap);
						countRow--;
					}
					else
						backupResultSet = false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return lst;

	}

	@Override
	public void close() {
		try {
			stmt.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
