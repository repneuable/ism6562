import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.file.Paths;

// below DataStax drivers added by self
//import com.datastax.oss.driver.api.core.metadata.Metadata;
//import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
//import com.datastax.oss.driver.api.core.cql.BoundStatement;
//import com.datastax.oss.driver.api.core.cql.PreparedStatement;
//import com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
//import com.datastax.oss.driver.api.core.cql.RegularInsert;
//import com.datastax.oss.driver.api.core.cql.SimpleStatement;

//import com.datastax.oss.driver.api.core.CqlIdentifier;
//import com.datastax.oss.driver.api.core.type.DataTypes;
//import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
//import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;

//import java.util.UUID;

public class ConnectDatabase {

   public static void main(String[] args) { 
	   
	   // Create the CqlSession object:
         try (CqlSession session = CqlSession.builder()
            .withCloudSecureConnectBundle(Paths.get("/Users/kevinhitt1/Desktop/ISM_6562_Big_Data/secure-connect-hitt-demo.zip"))
            .withAuthCredentials("jGNYGuelDuJbMRHLagPWnqrT","ZDlyqZZLWaMo0lf9l5m7jIcyHy9Rp2fjtC+9CPNhoOb0mfC+mNbE,SRG1qLMSEN4eZiMsMicUZLEy_5CaGft+NpSIG7nc9rR4vkdE-F8,5vsuDhxBrfXYScub4OZ1Gmj")
            .withKeyspace("hitt_schema")
            .build()) {
        	 
        	 // CONFIRM CONNECTION (have database respond with version number via system.local table)
            ResultSet rs = session.execute("select release_version from system.local");
            Row row_vcheck = rs.one();
            if (row_vcheck != null) {
               System.out.println(row_vcheck.getString("release_version"));
            } else {
               System.out.println("No connection :,(");
            }
            
            // Read the table from the database: 
            System.out.println("Connection successful ... proceeding with operations:");
            
            
            // Create the keyspace if it doesn't exist
    	    String keyspaceName = "hitt_schema";
    	    //String replicationStrategy = "{'class': 'SimpleStrategy', 'replication_factor': 1}";
    	    //String createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName +
    	    //    " WITH replication = " + replicationStrategy + ";";

    	    //session.execute(createKeyspaceQuery);
    	    //System.out.println("Keyspace created as " + keyspaceName);
    	    // Keyspaces only able to be created in web interface: 
    	    //    	    Missing correct permission on hitt_schema2.: Keyspace management is currently only supported at 
    	    //    	    https://astra.datastax.com/org/95dd66a1-d091-4374-88d2-fb7fa3f50590/database/f5c5a0a5-cf44-43df-abbe-2c8a345710d8
    	    
    	    //session.execute("DESCRIBE KEYSPACE " + keyspaceName);
    	    
    	    // Create the column family if it doesn't exist
    	    String columnFamilyName = "hitt_cfam";
    	    String createColumnFamilyQuery = "CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + columnFamilyName + 
    	        " (item_id int PRIMARY KEY, item_name text, item_desc text);";
    	    session.execute(createColumnFamilyQuery);
    	    System.out.println("Column Family created in " + keyspaceName + " as " + columnFamilyName);
    	    //session.execute("DESCRIBE TABLE " + keyspaceName + "." + columnFamilyName);
    	    
    	    // Alter the column family
    	    String alterColumnFamilyQuery = "ALTER TABLE " + keyspaceName + "." + columnFamilyName + 
    	        " ADD item_price double;";
    	    session.execute(alterColumnFamilyQuery);
    	    System.out.println("Column Family altered in " + keyspaceName + " as " + columnFamilyName);
    	    //session.execute("DESCRIBE TABLES");

    	    // Insert data into the column family
    	    String insertQuery = "INSERT INTO " + keyspaceName + "." + columnFamilyName + 
    	        " (item_id, item_name, item_desc, item_price) VALUES (?, ?, ?, ?);";
    	    // '?' is a placeholder for values that will be inserted at runtime
    	    session.execute(insertQuery, 1, "Item1", "Deluxe Package", 99.99);
    	    session.execute(insertQuery, 2, "Item2", "Standard Package", 89.99);
    	    session.execute(insertQuery, 3, "Item2", "\"Dr. Dutta's Big Data\" Package", 999999.99);
    	    System.out.println("Rows added to " + keyspaceName + "." + columnFamilyName);
    	    //session.execute("SELECT * FROM hitt_schema.hitt_cfam");
    	    
    	    // Delete the column family
    	    String deleteColumnFamilyQuery = "DROP TABLE IF EXISTS " + keyspaceName + "." + columnFamilyName + ";";
    	    session.execute(deleteColumnFamilyQuery);
    	    System.out.println(keyspaceName + "." + columnFamilyName + "DELETED");
    	    //session.execute("SELECT * FROM hitt_schema.hitt_cfam");
    	    
    	    // Delete the keyspace
//    	    String deleteKeyspaceQuery = "DROP KEYSPACE IF EXISTS " + keyspaceName + ";";
//    	    session.execute(deleteKeyspaceQuery);
//    	    System.out.println(keyspaceName + "DELETED");
    	    // Keyspaces only able to be deleted in web interface: 
    	    //    	    Keyspace management is currently only supported on hitt_schema2.: Keyspace management is currently only supported at 
    	    //    	    https://astra.datastax.com/org/95dd66a1-d091-4374-88d2-fb7fa3f50590/database/f5c5a0a5-cf44-43df-abbe-2c8a345710d8

    	    
    	    // Close the session
    	    session.close();

            	
         }
         System.exit(0); // "catch" statement not needed as System.exit(0) always executes
   }
   
   
}

