package tc.yigit.m2p;


import lombok.Getter;
import lombok.Setter;
import tc.yigit.m2p.config.Configuration;
import tc.yigit.m2p.sql.Converter;
import tc.yigit.m2p.sql.SQLServer;
import tc.yigit.m2p.sql.SQLServer.SQLType;
import tc.yigit.m2p.utils.Utils;

public class Mysql2Postgre {
	
	public static final String CONFIG_NAME = "m2p.yml";

	@Getter
	@Setter
	private static Configuration configuration;

	@Getter
	private static SQLServer mysqlServer;
	@Getter
	private static SQLServer postgreServer;
	
	public static void main(String[] args){
		if(!Configuration.loadConfiguration()){
			Utils.log("Please add a correct config file.");
			return;
		}
		
		Utils.log("Config loaded.");

		mysqlServer = new SQLServer(
				SQLType.MYSQL,
				configuration.getMysqlHost(),
				configuration.getMysqlPort(),

				configuration.getMysqlUsername(),
				configuration.getMysqlPassword(),
				configuration.getMysqlDatabase()
		);
		postgreServer = new SQLServer(
				SQLType.POSTGRE,
				configuration.getPostgreHost(),
				configuration.getPostgrePort(),
				
				configuration.getPostgreUsername(),
				configuration.getPostgrePassword(),
				configuration.getPostgreDatabase()
		);
		
		if(!mysqlServer.isConnected()){
			Utils.log("MySQL connection failed.");			
			return;
		}
		if(!postgreServer.isConnected()){
			Utils.log("PostgreSQL connection failed.");			
			return;
		}
		
		Utils.log("All connections active.");
		Converter.convert(configuration.getMysqlTable(), configuration.getPostgreTable());
	}
	
}
