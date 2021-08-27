package tc.yigit.m2p.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;

import com.google.common.collect.Lists;

import tc.yigit.m2p.Mysql2Postgre;
import tc.yigit.m2p.enums.TableType;
import tc.yigit.m2p.utils.Utils;

public class TableCreator {
	
	public static LinkedHashMap<String, String> check(String from, String to){
		Utils.log("Table ("+from+") checking....");
		
		SQLServer toSQL = Mysql2Postgre.getPostgreServer();
		
		LinkedHashMap<String, String> columns = getColumns(from);
		columns.entrySet().forEach(entry -> {
			Utils.log("Column found: " + entry.getKey() + " (" + entry.getValue() + ")");
		});
		
		// Delete old table
		if(toSQL.tableExists(to)){
			Utils.log("PostgreSQL's ["+to+"] deleted, new one is creating...");
			toSQL.tableDelete(to);
		}
		
		createTable(to, columns);
		
		return columns;
	}
	
	private static void createTable(String table, LinkedHashMap<String, String> columns){
		String query = "CREATE TABLE "+table+" ( %lines% );";
		
		List<String> lines = Lists.newArrayList();
		
		if(columns.containsKey("id")){
			lines.add("id SERIAL NOT NULL,");
		}
		
		columns.entrySet().forEach(entry -> {
			String column = entry.getKey();
			TableType type = TableType.getTableType(entry.getValue());
			if(column.equals("id")) return;
			
			lines.add(column + " " + type.getColumnType() + " NULL,");
		});
		
		if(columns.containsKey("id")){
			lines.add("CONSTRAINT "+table+"_pkey PRIMARY KEY (id)");
		}
		
		StringBuilder lines_string = new StringBuilder();
		lines.forEach(line -> lines_string.append(line));
		
		query = query.replace("%lines%", lines_string.toString());
		
		Mysql2Postgre.getPostgreServer().update(query);
		
		Utils.log("Table created ["+table+"]");
	}
	
	private static LinkedHashMap<String, String> getColumns(String table){
		LinkedHashMap<String, String> columns = new LinkedHashMap<>();
		
		Statement ps = null;
		try{
			ps = Mysql2Postgre.getSQLServer().createStatement();
			ResultSet results = ps.executeQuery("SELECT * FROM " + table + " LIMIT 1");			
			ResultSetMetaData metadata = results.getMetaData();
			 
			int columnCount = metadata.getColumnCount();
			for(int i=1; i<=columnCount; i++){
				columns.put(metadata.getColumnName(i), metadata.getColumnClassName(i));
			}
		}catch(SQLException ex){
            ex.printStackTrace();
        }finally{
            try{
            	ps.close();
            }catch(SQLException ex){
            	//ex.printStackTrace();
            }
        }
		
		return columns;
	}

}
