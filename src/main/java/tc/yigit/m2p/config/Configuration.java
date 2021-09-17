package tc.yigit.m2p.config;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.yaml.snakeyaml.Yaml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import tc.yigit.m2p.Mysql2Postgre;

@AllArgsConstructor(staticName = "settings")
@NoArgsConstructor
@Getter
@Setter
public final class Configuration {
	
	private String mysqlUsername;
	private String mysqlHost;
	private String mysqlPassword;
	private int mysqlPort;
	private String mysqlDatabase;
	private String mysqlTable;

	private String postgreUsername;
	private String postgreHost;
	private String postgrePassword;
	private int postgrePort;
	private String postgreDatabase;
	private String postgreTable;
	
	private int limitCopyPerTask;
	
	public static Path getPath(){
		try{
			File clientFile = Paths.get(
					Mysql2Postgre.class.getProtectionDomain().getCodeSource().getLocation().toURI()
			).toFile().getParentFile();
			
			return Paths.get(clientFile.getAbsolutePath() + File.separator + Mysql2Postgre.CONFIG_NAME);
		}catch(Throwable ex){
			ex.printStackTrace();			
			return null;
		}
	}
	
	public static boolean loadConfiguration(){
		try{
			Yaml yaml = new Yaml();		
	        try(InputStream in = Files.newInputStream(Configuration.getPath())){
				Mysql2Postgre.setConfiguration(yaml.loadAs(in,Configuration.class));
	        }
	        
	        return true;
		}catch(Throwable ex){
			ex.printStackTrace();
			return false;
		}
	}
	
}
