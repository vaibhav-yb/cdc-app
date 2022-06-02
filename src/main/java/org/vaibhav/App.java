package org.vaibhav;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

// This app assumed that all the updates are going through on Yugabyte already
public class App {
  private final long BATCH_SIZE = 2048;
  private long iterations = 0;
  private boolean firstTime = true;
  private long startKey = 1;

  private static String TABLE_NAME = "";

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
  
  private long getCountOnYugabyte(String ybEndpoint) throws Exception {
    // Connection conn = DriverManager.getConnection("jdbc:yugabytedb://" + ybEndpoint + ":5433/yugabyte?user=yugabyte&password=yugabyte");
    try (Connection conn = ybDataSource.getConnection()){
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
    } catch (Exception e) {
      throw e;
    }
  }

  private void runWorkload(String endpoint, String mysqlEndpoint, String tableName) throws Exception {
    initializeYugabyteDataSource(endpoint /* yugabyte endpoint */);
    initializeMySqlDataSource(mysqlEndpoint);

    long endKey = startKey + BATCH_SIZE - 1;
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
        long countInYb = 0;
        // insert rows first

        System.out.println("Start Key: " + startKey + " End key: " + endKey);
        addBatchesToInsertStatement(st, startKey, endKey);

        int[] insertBatchCount = st.executeBatch();
        int resInsert = 0;
        for (int cnt : insertBatchCount) {
          resInsert += cnt;
        }

        // int resInsert = st.executeUpdate("insert into test_cdc_app(id) values (generate_series(" + startKey + "," + endKey + "));");
        if (resInsert != BATCH_SIZE) {
          throw new RuntimeException("Unable to insert more rows, trying from scratch again...");
        }
        System.out.println("Inserts completed...");
        Thread.sleep(200);

        countInYb = getCountOnYugabyte(endpoint);

        verifyCountOnMySql(mysqlEndpoint, countInYb);

        // Clear the batch after the inserts
        st.clearBatch();

        ++iterations;
        System.out.println("Iteration count: " + iterations);
        Thread.sleep(5000);

        // update the keys to be inserted
        startKey = endKey + 1;
        endKey = startKey + BATCH_SIZE - 1;
      }
    } catch (Exception e) {
      e.printStackTrace();
      startKey = endKey + 1;
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
