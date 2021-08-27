package tc.yigit.m2p.sql;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

public class SQLAddData {
	
	private SQLServer SQLServer;
	private List<String> keys;
	private List<Object> values;
	private String table;
	
	public SQLAddData(SQLServer SQLServer){
		this.SQLServer = SQLServer;
		this.keys = new LinkedList<String>();
		this.values = new LinkedList<Object>();
	}
	
	public List<String> getKeyList(){
		return this.keys;
	}
	
	public List<Object> getValueList(){
		return this.values;
	}
	
	public void addKey(String key){
		if(getKeyList().contains(key)){
			return;
		}
		getKeyList().add(key);
	}
	public void removeKey(String key){
		if(!getKeyList().contains(key)){
			return;
		}
		getKeyList().remove(key);
	}
	
	public void addValue(Object value){
		getValueList().add(value);
	}
	public void removeValue(Object value){
		if(!getValueList().contains(value)){
			return;
		}
		getValueList().remove(value);
	}
	
	public void setTable(String table){
		this.table = table;
	}
	
	public String getTable(){
		return this.table;
	}
	
	public String getKeys(){
		if(getKeyList().size() > 1){
			String output = "";
			int i = 0;
			for(String key : getKeyList()){
				boolean addComma = !(i == getKeyList().size() - 1);
				output += StringEscapeUtils.escapeSql(key) + (addComma ? ", " : "");					
				i++;
			}
			return output;
		}else{
			return getKeyList().get(0);
		}
	}
	
	public String getValues(){
		if(getValueList().size() > 1){
			String output = "";
			int i = 0;
			for(Object key : getValueList()){
				boolean addComma = !(i == getValueList().size() - 1);
				output += "'" + StringEscapeUtils.escapeSql(key.toString()) + "'" + (addComma ? ", " : "");					
				i++;
			}
			return output;
		}else{
			return (String) getValueList().get(0);
		}
	}
	
	public void send(){
		SQLServer.dataInsert(this.getTable(), this.getKeys(), this.getValues());
	}
	
}
