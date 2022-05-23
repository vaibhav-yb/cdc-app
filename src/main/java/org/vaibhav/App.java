package org.vaibhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


// This app assumed that all the updates are going through on Yugabyte already
public class App {
  private static long iterations = 0;
  private static boolean firstTime = true;
  private static long startMarker = 1;

  // private static long UPDATE_BATCH_SIZE = 3;
  // private static long INSERT_BATCH_SIZE = 3;

  // These flags are there to ensure that if the ybEndpoint the app is connected to, if it goes down
  // and the app can continue connection to the other node then it resumes insertion from the point 
  // where it threw the error
  private static boolean insertCompleted = false;
  private static boolean updateCompleted = false;
  private static boolean deleteCompleted = false;

  private static void addBatchesToInsertStatement(Statement st, long startKey, long endKey) throws Exception {
    long i = startKey;
    while (i <= endKey) {
      // INSERT INTO test_cdc_app VALUES (i);
      st.addBatch("INSERT INTO test_cdc_app VALUES (" + i + ");");
      
      ++i;
    }
  }

  private static void addBatchesToUpdateStatement(Statement st, long startKey, long endKey) throws Exception {
    long i = startKey;
    while (i <= endKey) {
      st.addBatch("UPDATE test_cdc_app SET name='VKVK' where id = " + i + ";");

      ++i;
    }
  }

  private static void addBatchesToDeleteStatement(Statement st, long startKey, long endKey) throws Exception {
    long i = startKey;
    while (i <= endKey) {
      st.addBatch("DELETE FROM test_cdc_app where id = " + i + ";");

      ++i;
    }
  }
  
  private static long getCountOnYugabyte(String ybEndpoint) throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:yugabytedb://" + ybEndpoint + ":5433/yugabyte?user=yugabyte&password=yugabyte");
    Statement st = conn.createStatement();
    
    ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM test_cdc_app;");
    rs.next();

    // st.close();
    // conn.close();
    return rs.getLong(1);
  }

  private static void verifyCountOnMySql(String mysqlEndpoint, long countInYugabyte) throws Exception {
    // Create connection
    Connection conn = DriverManager.getConnection("jdbc:mysql://" + mysqlEndpoint + ":3306/test_api?user=mysqluser&password=mysqlpw&sslMode=required");
    Statement st = conn.createStatement();
    
    // Do a select count(*)
    long countInMysql = 0;
    long start = System.currentTimeMillis();

    // Continue for a minute if count is not the same
    while ((System.currentTimeMillis() - start) < 60000 && countInMysql != countInYugabyte) {
      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM test_cdc_app;");
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

    // st.close();
    // conn.close();
  }

  private static void verifyCountOnMySqlAfterUpdate(String mysqlEndpoint, long countInYugabyte) throws Exception {
    // Create connection
    Connection conn = DriverManager.getConnection("jdbc:mysql://" + mysqlEndpoint + ":3306/test_api?user=mysqluser&password=mysqlpw&sslMode=required");
    Statement st = conn.createStatement();
    
    // Do a select count(*)
    long countOfRowsWithOldName = -1;
    long start = System.currentTimeMillis();

    // Continue for a minute if count is not the same
    while ((System.currentTimeMillis() - start) < 60000 && countOfRowsWithOldName != 0) {
      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM test_cdc_app WHERE name='Vaibhav';");
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
      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM test_cdc_app WHERE name='VKVK';");
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

    // st.close();
    // conn.close();
  }

  private static void runWorkload(String endpoint, String mysqlEndpoint) throws Exception {
    String ybUrl = "jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
      "user=yugabyte&password=yugabyte";
    Connection conn = DriverManager.getConnection(ybUrl);
    Statement st = conn.createStatement();
    // set up the table if it doesn't exist
    boolean res = st.execute("create table if not exists test_cdc_app (id int primary key, " +
      "name text default 'Vaibhav', a bigint default 12, b float default 12.34, vrchr varchar(20) default 'varchar_column'," +
      "dp double precision default 567.89, user_id text default '1234abcde') split into 10 tablets;");
    if (!res && firstTime) {
      // this means that the table is created
      System.out.println("Table created for the first time, waiting for 10s to let the " +
        "deployment happen");
      firstTime = false;
      Thread.sleep(10000);
    }

    // make sure the table doesn't contain anything
    // st.execute("delete from test_cdc_app;");

    long startKey = startMarker;
    long endKey;
    while(true) {
      endKey = startKey + 511; // Total batch size would be 512

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
      startMarker = startKey;
    }
  }

  public static void main(String[] args) {
    // args will contain the endpoints
    if (args.length == 0) {
      System.out.println("No endpoints specified to connect to, exiting...");
      System.exit(-1);
    }

    int index = 0;
    while (true) {
      try {
        // We are assuming that the last index being passed is mysql's connection point
        runWorkload(args[index], args[args.length - 1]);
      } catch (Exception e) {
        System.out.println("Exception caught: " + e);
        System.out.println("Trying again...");
        ++index;
        if (index >= args.length - 1) {
          index = 0;
        }
      }
    }
  }
}
