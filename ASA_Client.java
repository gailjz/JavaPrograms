package AsaClient;

//SQL 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//Microsoft JDBC Driver 
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

//Java Utilities 
import java.util.Properties;


public class AsaClientMain {

	public static void main(String[] args) {
		
		// Connecting to Azure Synapse SQL Server using JDBC 
		//https://docs.microsoft.com/en-us/sql/connect/jdbc/parsing-the-results?view=sql-server-ver15
	    String server = "svrName.database.windows.net";
	    String port =  "1433";
	    String dbName = "dwDbName";
	    String user = "userName";
	    String pass = "PassWord";
	   
	    String connectionUrl = "jdbc:sqlserver://" + server + ":" + port + ";database=" + dbName + ";user=" + user+";password=" + pass;
	    
	    //Establish and reuse the connection 
	    Connection con = null;
		try {
			con = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	   
	    // Try a simple select query 
        try (Statement stmt = con.createStatement();) {
            String SQL = "SELECT TOP 10 * FROM dbo.DimAccount";
            ResultSet rs = stmt.executeQuery(SQL);

            // Iterate through the data in the result set and display it.
            while (rs.next()) {
            	System.out.println("Hello");
            	System.out.println(rs.getString("AccountKey") + " " + rs.getString("AccountType") + " " + rs.getString("ValueType"));
            }
            stmt.close();
        }
        // Handle any errors that may have occurred.
        catch (SQLException e) {
            e.printStackTrace();
        }
        
     // Try update with join 
        try (Statement stmt = con.createStatement();) {
        	String SQL = new StringBuilder()
        			.append("update [dbo].[DimAccountTar] \n")
        			.append("set dbo.DimAccountTar.CustomMembers = stg.CustomMembers, \n")
					.append("dbo.DimAccountTar.CustomMemberOptions = stg.CustomMemberOptions \n")
					.append("From [dbo].[DimAccountTar] \n")
					.append("Join [dbo].[DimAccountStg] stg \n")
					.append("on dbo.DimAccountTar.AccountKey = stg.AccountKey \n")
					.toString();
        	
        	System.out.println(SQL);
        	stmt.executeUpdate(SQL);
        	//stmt.executeQuery(SQL);	
            stmt.close();
            
        }
        // Handle any errors that may have occurred.
        catch (SQLException e) {
            e.printStackTrace();
        }
        // Now close the connection 
        try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("Done");
	}
}
	
	
