package readers;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SqlIncrementalReader extends SqlReader{
	private int batchSize;
	private boolean backupResultSet;
	private Object rs;
/*
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

	}*/
}
