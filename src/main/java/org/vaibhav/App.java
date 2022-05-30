package org.vaibhav;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


// This app assumed that all the updates are going through on Yugabyte already
public class App {
  private long iterations = 0;
  private boolean firstTime = true;
  private long startKey = 1;

  private static String TABLE_NAME = "";

  // These flags are there to ensure that if the ybEndpoint the app is connected to, if it goes down
  // and the app can continue connection to the other node then it resumes insertion from the point 
  // where it threw the error
  private boolean insertCompleted = false;
  private boolean updateCompleted = false;
  private boolean deleteCompleted = false;

  private HikariDataSource ybDataSource = new HikariDataSource();
  private HikariDataSource mysqlDataSource = new HikariDataSource();

  private void initializeYugabyteDataSource(String ybEndpoint) throws Exception {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl("jdbc:yugabytedb://" + ybEndpoint + ":5433/yugabyte?user=yugabyte&password=yugabyte");
    config.setMaximumPoolSize(2);
    
    if (!ybDataSource.isClosed()) {
      ybDataSource.close();
    }

    ybDataSource = new HikariDataSource(config);
  }

  private void initializeMySqlDataSource(String mysqlEndpoint) throws Exception {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl("jdbc:mysql://" + mysqlEndpoint + ":3306/test_api?user=mysqluser&password=mysqlpw&sslMode=required");
    config.setMaximumPoolSize(2);

    if (!mysqlDataSource.isClosed()) {
      mysqlDataSource.close();
    }

    mysqlDataSource = new HikariDataSource(config);
  }

  private void addBatchesToInsertStatement(Statement st, long start, long end) throws Exception {
    long i = start;
    while (i <= end) {
      // INSERT INTO test_cdc_app VALUES (i);
      st.addBatch("INSERT INTO " + TABLE_NAME + " VALUES (" + i + ");");
      
      ++i;
    }
  }

  private void addBatchesToUpdateStatement(Statement st, long start, long end) throws Exception {
    long i = start;
    while (i <= end) {
      st.addBatch("UPDATE " + TABLE_NAME + " SET name='VKVK' where id = " + i + ";");

      ++i;
    }
  }

  private void addBatchesToDeleteStatement(Statement st, long start, long end) throws Exception {
    long i = start;
    while (i <= end) {
      st.addBatch("DELETE FROM " + TABLE_NAME + " where id = " + i + ";");

      ++i;
    }
  }
  
  private long getCountOnYugabyte(String ybEndpoint) throws Exception {
    // Connection conn = DriverManager.getConnection("jdbc:yugabytedb://" + ybEndpoint + ":5433/yugabyte?user=yugabyte&password=yugabyte");
    try (Connection conn = ybDataSource.getConnection()) {
      Statement st = conn.createStatement();
      
      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + ";");
      rs.next();
      
      return rs.getLong(1);
    } catch (Exception e) {
      throw e;
    }
  }

  private void verifyCountOnMySql(String mysqlEndpoint, long countInYugabyte) throws Exception {
    // Create connection
    // Connection conn = DriverManager.getConnection("jdbc:mysql://" + mysqlEndpoint + ":3306/test_api?user=mysqluser&password=mysqlpw&sslMode=required");
    try (Connection conn = mysqlDataSource.getConnection()) {
      Statement st = conn.createStatement();
      
      // Do a select count(*)
      long countInMysql = 0;
      long start = System.currentTimeMillis();

      // Continue for a minute if count is not the same
      while ((System.currentTimeMillis() - start) < 60000 && countInMysql != countInYugabyte) {
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + ";");
        rs.next();

        countInMysql = rs.getLong(1);
      }
      
      if (countInMysql != countInYugabyte) {
        System.out.println("Exiting the app because count is not equal");
        System.out.println("Yugabyte: " + countInYugabyte + " MySql: " + countInMysql);

        System.exit(-11);
      } else {
        System.out.println("Count in both source and sink equal");
      }
    }
  }

  private void verifyCountOnMySqlAfterUpdate(String mysqlEndpoint, long countInYugabyte) throws Exception {
    // Create connection
    // Connection conn = DriverManager.getConnection("jdbc:mysql://" + mysqlEndpoint + ":3306/test_api?user=mysqluser&password=mysqlpw&sslMode=required");
    try (Connection conn = mysqlDataSource.getConnection()) {
      Statement st = conn.createStatement();
      
      // Do a select count(*)
      long countOfRowsWithOldName = -1;
      long start = System.currentTimeMillis();

      // Continue for a minute if count is not the same
      while ((System.currentTimeMillis() - start) < 60000 && countOfRowsWithOldName != 0) {
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE name='Vaibhav';");
        rs.next();

        countOfRowsWithOldName = rs.getLong(1);
      }

      if (countOfRowsWithOldName != 0) {
        System.out.println("Exiting the app because count of rows with old name is not zero");
        System.out.println("Count of rows with old name: " + countOfRowsWithOldName);

        System.exit(-11);
      } else {
        System.out.println("Row count with old name zero in both source and sink");
      }

      long countOfRowsWithNewName = 0;
      long newStart = System.currentTimeMillis();
      while ((System.currentTimeMillis() - newStart) < 60000 && countOfRowsWithNewName != countInYugabyte) {
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE name='VKVK';");
        rs.next();

        countOfRowsWithNewName = rs.getLong(1);
      }
      
      if (countOfRowsWithNewName != countInYugabyte) {
        System.out.println("Exiting the app because count of rows with new name is not equal");
        System.out.println("Yugabyte: " + countInYugabyte + " MySql: " + countOfRowsWithNewName);

        System.exit(-11);
      } else {
        System.out.println("Count of rows with new name in both source and sink equal");
      }
    } catch(Exception e) {
      throw e;
    }
  }

  private void runWorkload(String endpoint, String mysqlEndpoint, String tableName) throws Exception {
    initializeYugabyteDataSource(endpoint);
    initializeMySqlDataSource(mysqlEndpoint);

    long endKey = startKey + 511;

    // String ybUrl = "jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
    //   "user=yugabyte&password=yugabyte";
    try (Connection conn = ybDataSource.getConnection()) {
      Statement st = conn.createStatement();
      TABLE_NAME = tableName;
      // set up the table if it doesn't exist
      boolean res = st.execute("create table if not exists " + TABLE_NAME + " (id int primary key, " +
        "name text default 'Vaibhav', a bigint default 12, b float default 12.34, vrchr varchar(20) default 'varchar_column'," +
        "dp double precision default 567.89, user_id text default '1234abcde') split into 10 tablets;");
      if (!res && firstTime) {
        // this means that the table is created
        System.out.println("Table created for the first time, waiting for 10s to let the " +
          "deployment happen");
        firstTime = false;
        Thread.sleep(10000);
      }

      while(true) {
        System.out.println("Start key: " + startKey + " End key: " + endKey);

        long countInYb = 0;
        // insert rows first
        if (!insertCompleted){
          if (true) { // Do not update i anywhere
            addBatchesToInsertStatement(st, startKey, endKey);

            int[] insertBatchCount = st.executeBatch();
            int resInsert = 0;
            for (int cnt : insertBatchCount) {
              resInsert += cnt;
            }

            // int resInsert = st.executeUpdate("insert into test_cdc_app(id) values (generate_series(" + startKey + "," + endKey + "));");
            if (resInsert != 512) {
              throw new RuntimeException("Unable to insert more rows, trying from scratch again...");
            }
          }
          System.out.println("Inserts completed...");
          insertCompleted = true;
          Thread.sleep(200);

          countInYb = getCountOnYugabyte(endpoint);

          verifyCountOnMySql(mysqlEndpoint, countInYb);
        }

        // Clear the batch after the inserts
        st.clearBatch();

        // update the inserted rows
        if (!updateCompleted) {
          if (true) {
            addBatchesToUpdateStatement(st, startKey, endKey);

            int[] batchUpdateCount = st.executeBatch();
            int resUpdate = 0;
            for (int cnt : batchUpdateCount) {
              resUpdate += cnt;
            }

            // int resUpdate = st.executeUpdate("update test_cdc_app set name = 'VKVK' where id >= " + startKey + " and id <= " + endKey + ";");
            if (resUpdate != 512) {
              throw new RuntimeException("Not all the rows are updated");
            }
          }
          System.out.println("Update complete...");
          updateCompleted = true;
          verifyCountOnMySqlAfterUpdate(mysqlEndpoint, countInYb);
          Thread.sleep(200);
        }

        // clear the batch after updates
        st.clearBatch();

        // delete the inserted rows
        
        if (!deleteCompleted) {
          if (true) {
            addBatchesToDeleteStatement(st, startKey, endKey);

            int[] batchDeleteCount = st.executeBatch();
            int resDelete = 0;
            for (int cnt : batchDeleteCount) {
              resDelete += cnt;
            }

            if (resDelete != 512) {
              throw new RuntimeException("Not all the rows are deleted");
            }
          }
          System.out.println("Delete complete...");
          deleteCompleted = true;
          // todo: add a function to verify that the deletes are taking place
          Thread.sleep(200);
        }

        ++iterations;
        System.out.println("Iteration count: " + iterations);
        Thread.sleep(5000);

        // mark the flags as false so that next iteration can take place
        insertCompleted = false;
        updateCompleted = false;
        deleteCompleted = false;

        // update the keys to be inserted
        startKey = endKey + 1;
        endKey = startKey + 511;
      }
    } catch(Exception e) {
      e.printStackTrace();
      endKey = startKey + 511;
      throw e;
    }
  }

  public static void main(String[] args) {
    App cdcApp = new App();
    // args will contain the endpoints
    if (args.length == 0) {
      System.out.println("No endpoints specified to connect to, exiting...");
      System.exit(-1);
    }

    int index = 1;
    while (true) {
      try {
        // We are assuming that the last index being passed is mysql's connection point
        // and the first is a table name
        cdcApp.runWorkload(args[index], args[args.length - 1], args[0]);
      } catch (Exception e) {
        System.out.println("Exception caught: " + e);
        System.out.println("Trying again...");
        ++index;
        if (index >= args.length - 1) {
          index = 1;
        }
      }
    }
  }
}
