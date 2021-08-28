package tc.yigit.m2p.enums;

import java.util.Arrays;

import lombok.Getter;

@Getter
public enum TableType {

	VARCHAR			(new Integer[]{12},						"java.lang.String",			"varchar(1000)"),
	STRING			(new Integer[]{-1},						"java.lang.String",			"text"),	
	INTEGER			(new Integer[]{4,5,3,2,-6}, 			"java.lang.Integer",		"int4"),
	LONG			(new Integer[]{-5}, 					"java.lang.Long",			"int8"),
	DOUBLE          (new Integer[]{8,6}, 					"java.lang.Double",			"float8"),	
	BOOLEAN			(new Integer[]{-7}, 					"java.lang.Boolean",		"bool"),	
	TIMESTAMP		(new Integer[]{93,92,91}, 				"java.sql.Timestamp",		"timestamp");
	
	private Integer[] ids;
	private String className;
	private String columnType;
	
	TableType(Integer[] ids, String className, String columnType){
		this.ids = ids;
		this.className = className;
		this.columnType = columnType;
	}
	
	public static TableType getTableType(int id){
		return Arrays.asList(TableType.values()).stream().filter(table -> Arrays.asList(table.ids).contains(Integer.valueOf(id))).findFirst().orElse(null);		
	}
	
}
