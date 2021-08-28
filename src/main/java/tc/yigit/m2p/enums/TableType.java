package tc.yigit.m2p.enums;

import java.util.Arrays;

import lombok.Getter;

@Getter
public enum TableType {

	INTEGER			("java.lang.Integer", 		 "int4"),
	STRING			("java.lang.String",		 "text"),
	LONG			("java.lang.Long",			 "int8"),
	BOOLEAN			("java.lang.Boolean",		 "bool"),
	TIMESTAMP		("java.sql.Timestamp",		 "timestamp");
	
	private String className;
	private String columnType;
	
	TableType(String className, String columnType){
		this.className = className;
		this.columnType = columnType;
	}
	
	public static TableType getTableType(String type){
		return Arrays.asList(TableType.values()).stream().filter(table -> table.className.equals(type)).findFirst().orElse(null);
	}
	
}
