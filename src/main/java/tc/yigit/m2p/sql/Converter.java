package tc.yigit.m2p.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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

@SuppressWarnings("deprecation")
public class Converter {
	
	private static long TOTAL_SIZE = 0;
	private static long COPIED_SIZE = 0;
	
	public static void convert(String from, String to){
		final String prefix = "SQL ["+from+"] <-TO-> POSTGRE ["+to+"] --> ";		
		Utils.log(prefix + "Convert starting...");
				
		Utils.log(prefix + "Table cheking...");
		LinkedHashMap<String, TableType> columns = TableCreator.check(from, to);
		
		Utils.log(prefix + "Counts cheking...");
		TOTAL_SIZE = Mysql2Postgre.getSQLServer().getRowCount(from);
		Utils.log(prefix + "Counts in table: " + TOTAL_SIZE);
		
		Utils.log(prefix + "Convert checked.");
		
		Utils.log(prefix + "Copying data...");
		copyDatas(from, to, columns);
		Utils.log(prefix + "Copying is completed.");
	}
	
	private static void copyDatas(String from, String to, LinkedHashMap<String, TableType> columns){
		int last_id = -1;
		while(true){
			last_id = copyData(from, to, last_id, columns);
			if(last_id == -2){
				break;
			}
		}
	}
	private static int copyData(String from, String to, int last_id, LinkedHashMap<String, TableType> columns){
		SQLServer fromSQL 	= Mysql2Postgre.getSQLServer();
		SQLServer toSQL 	= Mysql2Postgre.getPostgreServer();
		
		final int limit = Mysql2Postgre.getConfig().getLimit_copy_per_task();
        int completed_limit = 0;
        
        int new_last_id = 0; 

        PreparedStatement psBatch = null;
		String keys 	= Joiner.on(", ").join(generateList(columns.size(), new LinkedList<>(columns.keySet())));
		String values	= Joiner.on(", ").join(generateList(columns.size(), null));	

    	String query = "INSERT INTO %table% (%keys%) VALUES (%values%)";
    	query = query.replace("%table%", to);
    	query = query.replace("%keys%", keys);
    	query = query.replace("%values%", values);
    	    	
		PreparedStatement ps = null;	
        try{
			toSQL.getConnection().setAutoCommit(true);
			
            ResultSet set = null;            
            if(last_id == -1){
            	ps = fromSQL.prepare("SELECT * FROM "+from+" ORDER BY id DESC LIMIT ?");
            	ps.setInt(1, limit);
            }else{
            	ps = fromSQL.prepare("SELECT * FROM "+from+" WHERE id < ? ORDER BY id DESC LIMIT ?");
            	ps.setInt(1, last_id);
            	ps.setInt(2, limit);
            }
            
            set = ps.executeQuery();
        	psBatch = toSQL.prepare(query);
            
            while(set.next()){
            	completed_limit++;
            	writeData(psBatch, set, columns);
				new_last_id = set.getInt("id");
            }
            
            psBatch.executeBatch();
        }catch(SQLException ex){
            ex.printStackTrace();
        }finally{
            try{
                ps.close();
            }catch(SQLException ex){
            	//ex.printStackTrace();
            }
            try{
            	psBatch.close();
            }catch(SQLException ex){
            	//ex.printStackTrace();
            }
        }
        
        COPIED_SIZE += completed_limit;
        Utils.log("Total row count: " + TOTAL_SIZE + " | " + " Copied row count: " + COPIED_SIZE);
        
        if(completed_limit < limit){
        	return -2;
        }
        
        return new_last_id;
	}
	
	private static void writeData(PreparedStatement ps, ResultSet set, LinkedHashMap<String, TableType> columns){
        try{
        	int i = 1;
    		for(Entry<String, TableType> entry : columns.entrySet()){
    			String column = entry.getKey();
    			TableType type = entry.getValue();
    			
    			if(type == TableType.TIMESTAMP){
    				try{
        				ps.setTimestamp(i, set.getTimestamp(column));   
    				}catch(Throwable ex){
    					ps.setTimestamp(i, new Timestamp(1970, 1, 1, 0, 0, 0, 0));
    				} 				
    			}else{
    				ps.setObject(i, set.getObject(column));
    			}
    			
    			i++;
    		};
    		
    		ps.addBatch();
        }catch(SQLException ex){
            ex.printStackTrace();
        }
	}
	
	private static List<String> generateList(int key, LinkedList<String> columns){
		List<String> keys = Lists.newArrayList();
		
		for(int i = 0; i < key; i++){
			keys.add(columns != null ? "\"" + columns.get(i).toLowerCase(Locale.ENGLISH) + "\"" : "?");
		}
		
		return keys;
	}
 	
}
