package tc.yigit.m2p.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import tc.yigit.m2p.utils.Utils;

public class SQLServer {
	
	private SQLType sqlType;
	
    private String ip;
    private int port;
    
    private String user;
    private String pass;
	
    private String database;	
    private Connection con;
	private ScheduledExecutorService executor;
	
	public static enum SQLType {
		POSTGRE("postgresql", "org.postgresql.Driver"),
		MYSQL("mysql", "com.mysql.jdbc.Driver");
		
		private String connectionName;
		private String className;
		
		SQLType(String connectionName, String className){
			this.connectionName = connectionName;
			this.className = className;
		}
		
		public String getConnectionName(){
			return this.connectionName;
		}
		
		public String getClassName(){
			return this.className;
		}
	}
	
	public SQLServer(SQLType sqlType, String ip, int port, String user, String pass, String database){
		this.sqlType = sqlType;
		this.ip = ip;
		this.port = port;
		this.user = user;
		this.pass = pass;
		this.database = database;
		this.executor = Executors.newSingleThreadScheduledExecutor();
		
		for(SQLType type : SQLType.values()){
			try{
				Class.forName(type.getClassName());
			}catch(Throwable ex){
				ex.printStackTrace();
			}
		}
		
		connect();
		
		this.executor.scheduleWithFixedDelay(() -> {
			try{
				if(!this.isConnected()) throw new SQLException("No connection!");
				
				Statement sta = this.con.createStatement();	
				sta.execute("SELECT 1");
				sta.close();
			}catch(SQLException ex){
				connect();
			}catch(Exception ex){
				// don't break the task.
			}
		}, 0L, 1000L, TimeUnit.MILLISECONDS);
	}
	
	private void connect(){ 
		disconnect();
		try{
			this.con = DriverManager.getConnection("jdbc:"+this.getSQLType().getConnectionName()+"://" + getIP() + ":" + getPort() + "/" + getDatabase()+"?prepareThreshold=0&preparedStatementCache=0&preparedStatementCacheQueries=0&useSSL=false&autoReconnect=true&useUnicode=true&characterEncoding=utf-8", getUser(), getPass());
		}catch(Exception ex){
			ex.printStackTrace();
            Utils.log("[" + getIP() + ":" + getPort() + "] SQL connection failed: " + ex.getMessage());
		}
	}	
	
	public SQLServer newInstance(){
		return new SQLServer(
				getSQLType(),
				getIP(), 
				getPort(), 
				getUser(), 
				getPass(),
				getDatabase()
		);
	}
	
	public SQLType getSQLType(){
		return this.sqlType;
	}
	
	public String getIP(){
		return this.ip;
	}
	
	public void destroy(){
		this.disconnect();
		
		try{
			this.executor.shutdownNow();		
		}catch(Throwable ex){
			// don't break the task.
		}
	}
	
	public int getPort(){
		return this.port;
	}
	
	public String getUser(){
		return this.user; 
	}
	
	public String getPass(){
		return this.pass;
	}
	
	public String getDatabase(){
		return this.database;
	}
	
	public ExecutorService getExecutor(){
		return executor;
	}
	
	public Connection getConnection(){
		return this.con;
	}
	
	public boolean isConnected(){
		try{
			return getConnection() != null && !getConnection().isClosed();
		}catch(SQLException e){
			return false;
		}
	}
	
	public void reConnect(){
		disconnect();
		connect();
	}
	
	private void disconnect(){
		if(isConnected()){
			try{
				getConnection().close();
			}catch(SQLException e){
				e.printStackTrace();
			}
		}
		
	    this.con = null;
	}	
	
	public Statement createStatement() throws SQLException {
		return this.getConnection().createStatement();
	}
	public PreparedStatement prepare(String command) throws SQLException {
		return this.getConnection().prepareStatement(command);
	}
	
}
