package org.vaibhav;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class App {
  private long iterations = 0;
  private boolean firstTime = true;
  private long startKey = 1;

  private static String TABLE_NAME = "";

  private HikariDataSource mysql1DataSource = new HikariDataSource();
  private HikariDataSource mysql2DataSource = new HikariDataSource();

  private void initializeMySql1DataSource(String mysql1Endpoint) throws Exception {
    HikariConfig config = new HikariConfig();

    System.out.println("Initializing mysql1 pool with endpoint: " + mysql1Endpoint);
    config.setJdbcUrl("jdbc:mysql://" + mysql1Endpoint + ":3306/api_db_domestic?sslMode=required");
    config.setUsername("replicant");
    config.setPassword("replicant#123");
    config.setMaximumPoolSize(2);

    if (!mysql1DataSource.isClosed()) {
      mysql1DataSource.close();    
    }

    mysql1DataSource = new HikariDataSource(config);
  }

  private void initializeMySql2DataSource(String mysql2Endpoint) throws Exception {
    HikariConfig config = new HikariConfig();

    System.out.println("Initializing mysql2 pool with endpoint: " + mysql2Endpoint);

    config.setJdbcUrl("jdbc:mysql://" + mysql2Endpoint + ":3306/test_api?user=mysqluser&password=mysqlpw&sslMode=required");
    config.setMaximumPoolSize(2);

    if (!mysql2DataSource.isClosed()) {
      mysql2DataSource.close();
    }

    mysql2DataSource = new HikariDataSource(config);
  }

  private void addBatchesToInsertStatement(Statement st, long start, long end) throws Exception {
    long i = start;
    while (i <= end) {
      // INSERT INTO test_cdc_app VALUES (i);
      st.addBatch("INSERT INTO " + TABLE_NAME + " VALUES (" + i + ");");
      
      ++i;
    }
  }
  
  private long getCountOnMysql1(String mysql1Endpoint) throws Exception {
    try (Connection conn = mysql1DataSource.getConnection()){
      Statement st = conn.createStatement();
      
      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + ";");
      rs.next();

      return rs.getLong(1);
    } catch (Exception e) {
      throw e;
    }
  }

  private void verifyCountOnMySql2(String mysql2Endpoint, long countInMysql1) throws Exception {
    try (Connection conn = mysql2DataSource.getConnection()) {
      Statement st = conn.createStatement();
      
      // Do a select count(*)
      long countInMysql2 = 0;
      long start = System.currentTimeMillis();

      long WAIT_TIME_MS = 120000; // 2 minutes
      // Continue for a minute if count is not the same
      while ((System.currentTimeMillis() - start) < WAIT_TIME_MS && countInMysql2 != countInMysql1) {
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + ";");
        rs.next();

        countInMysql2 = rs.getLong(1);
      }
      
      if (countInMysql1 != countInMysql2) {
        System.out.println("Exiting the app because count is not equal");
        System.out.println("MySql1: " + countInMysql1 + " MySql2: " + countInMysql2);

        System.exit(-11);
      } else {
        System.out.println("Count in both mysql1 and mysql2 equal");
      }
    } catch (Exception e) {
      throw e;
    }
  }

  private void runWorkload(String mysql1Endpoint, String mysql2Endpoint, String tableName) throws Exception {
    initializeMySql1DataSource(mysql1Endpoint /* mysql1 endpoint */);
    initializeMySql2DataSource(mysql2Endpoint);

    final long BATCH_SIZE = 1024;

    long endKey = startKey + BATCH_SIZE - 1;
    try (Connection conn = mysql1DataSource.getConnection()) {
      Statement st = conn.createStatement();
      TABLE_NAME = tableName;
      // set up the table if it doesn't exist
      boolean res = st.execute("create table if not exists " + TABLE_NAME + " (id int primary key, " +
        "name varchar(20) default 'Vaibhav', a bigint default 12, b double default 12.34, vrchr varchar(20) default 'varchar_column'," +
        "dp double default 567.89, user_id varchar(25) default '1234abcde');");
      if (!res && firstTime) {
        // this means that the table is created
        System.out.println("Table created for the first time, waiting for 10s to let the " +
          "deployment happen");
        firstTime = false;
        Thread.sleep(10000);
      }

      System.out.println("Starting workload...");

      while(true) {
        long countInMysql1 = 0;
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

        countInMysql1 = getCountOnMysql1(mysql1Endpoint);

        verifyCountOnMySql2(mysql2Endpoint, countInMysql1);
        // Clear the batch after the inserts
        st.clearBatch();

        ++iterations;
        System.out.println("Iteration count: " + iterations);
        Thread.sleep(5000);

        // update the keys to be inserted
        startKey = endKey + 1;
        endKey = startKey + 511;
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
        // We are assuming that the last index being passed is mysql 2's connection point
        // and the first is a table name, the second will be the mysql1's endpoint

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
