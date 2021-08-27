package tc.yigit.m2p;


import tc.yigit.m2p.config.Configuration;
import tc.yigit.m2p.sql.Converter;
import tc.yigit.m2p.sql.SQLServer;
import tc.yigit.m2p.sql.SQLServer.SQLType;
import tc.yigit.m2p.utils.Utils;

public class Mysql2Postgre {
	
	public static final String CONFIG_NAME = "m2p.yml";
	public static Configuration CONFIGURATION;
	
	private static SQLServer MYSQL_SERVER;
	private static SQLServer POSTGRE_SERVER;
	
	public static void main(String[] args){
		if(!Configuration.loadConfiguration()){
			Utils.log("Please add a correct config file.");
			return;
		}
		
		Utils.log("Config loaded.");
		
		MYSQL_SERVER = new SQLServer(
				SQLType.MYSQL,
				CONFIGURATION.getMysql_host(),
				CONFIGURATION.getMysql_port(),
				
				CONFIGURATION.getMysql_username(),
				CONFIGURATION.getMysql_password(),
				CONFIGURATION.getPostgre_database()
		);
		POSTGRE_SERVER = new SQLServer(
				SQLType.POSTGRE,
				CONFIGURATION.getPostgre_host(),
				CONFIGURATION.getPostgre_port(),
				
				CONFIGURATION.getPostgre_username(),
				CONFIGURATION.getPostgre_password(),
				CONFIGURATION.getPostgre_database()
		);
		
		if(!MYSQL_SERVER.isConnected()){
			Utils.log("MySQL connection failed.");			
			return;
		}
		if(!POSTGRE_SERVER.isConnected()){
			Utils.log("PostgreSQL connection failed.");			
			return;
		}
		
		Utils.log("All connections active.");
		Converter.convert(CONFIGURATION.getMysql_table(), CONFIGURATION.getPostgre_table());
	}
	
}
