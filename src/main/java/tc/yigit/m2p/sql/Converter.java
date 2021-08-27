package tc.yigit.m2p.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import tc.yigit.m2p.Mysql2Postgre;
import tc.yigit.m2p.enums.TableType;
import tc.yigit.m2p.utils.Utils;

public class Converter {
	
	private static long TOTAL_SIZE = 0;
	private static long COPIED_SIZE = 0;
	
	public static void convert(String from, String to){
		final String prefix = "SQL ["+from+"] <-TO-> POSTGRE ["+to+"] --> ";		
		Utils.log(prefix + "Convert starting...");
				
		Utils.log(prefix + "Table cheking...");
		LinkedHashMap<String, String> columns = TableCreator.check(from, to);
		TOTAL_SIZE = Mysql2Postgre.getSQLServer().getRowCount(from);
		
		Utils.log(prefix + "Convert checked.");
		
		Utils.log(prefix + "Copying data...");
		copyDatas(from, to, columns);
		Utils.log(prefix + "Copying is completed.");
	}
	
	private static void copyDatas(String from, String to, LinkedHashMap<String, String> columns){
		Integer last_id = null;
		while((last_id = copyData(from, to, last_id, columns)) == null){
			break;
		}
	}
	private static Integer copyData(String from, String to, Integer last_id, LinkedHashMap<String, String> columns){
		SQLServer fromSQL = Mysql2Postgre.getSQLServer();
		
		final int limit = Mysql2Postgre.getConfig().getLimit_copy_per_task();
        int completed_limit = 0;
        
        int new_last_id = 0; 
		
		PreparedStatement ps = null;
        try{
            ResultSet set = null;            
            if(last_id == null){
            	ps = fromSQL.prepare("SELECT * FROM "+from+" ORDER BY id DESC LIMIT ?");
            	ps.setInt(1, limit);
            }else{
            	ps = fromSQL.prepare("SELECT * FROM "+from+" WHERE id < ? ORDER BY id DESC LIMIT ?");
            	ps.setInt(1, last_id.intValue());
            	ps.setInt(2, limit);
            }
            
            set = ps.executeQuery();
            
            while(set.next()){
            	completed_limit++;
            	
            	try{
            		writeData(set, to, columns);
            	}catch(Throwable ex){
            		ex.printStackTrace();
            	}
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
        
        COPIED_SIZE += completed_limit;
        Utils.log("Total row count: " + TOTAL_SIZE + " | " + " Copied row count: " + COPIED_SIZE);
        
        if(completed_limit < limit){
        	return null;
        }
        
        return new_last_id;
	}
	
	private static boolean writeData(ResultSet set, String table, LinkedHashMap<String, String> columns){
		String keys 	= Joiner.on(", ").join(generateList(columns.size(), new LinkedList<>(columns.keySet())));
		String values	= Joiner.on(", ").join(generateList(columns.size(), null));
		
		SQLServer toSQL = Mysql2Postgre.getPostgreServer();
		boolean isSaved = false;
		
        PreparedStatement ps = null;
        try{
        	String query = "INSERT INTO %table% (%keys%) VALUES (%values%)";
        	query = query.replace("%table%", table);
        	query = query.replace("%keys%", keys);
        	query = query.replace("%values%", values);
        	
        	ps = toSQL.prepare(query);
        	
        	int i = 1;
    		for(Entry<String, String> entry : columns.entrySet()){
    			String column = entry.getKey();
    			TableType type = TableType.getTableType(entry.getValue());
    			
    			if(type == TableType.INTEGER){
    				ps.setInt(i, set.getInt(column));
    			}else if(type == TableType.LONG){
    				ps.setLong(i, set.getLong(column));
    			}else if(type == TableType.STRING){
    				ps.setString(i, set.getString(column));
    			}else if(type == TableType.TIMESTAMP){
    				ps.setTimestamp(i, set.getTimestamp(column));
    			}else{
    				ps.setObject(i, set.getObject(column));
    			}
    			
    			i++;
    		};
    		
            int success = ps.executeUpdate();
            if(success > 0){
            	isSaved = true;
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
        
        return isSaved;
	}
	
	private static List<String> generateList(int key, LinkedList<String> columns){
		List<String> keys = Lists.newArrayList();
		
		for(int i = 0; i < key; i++){
			keys.add(columns != null ? "\"" + columns.get(i).toLowerCase(Locale.ENGLISH) + "\"" : "?");
		}
		
		return keys;
	}
 	
}
