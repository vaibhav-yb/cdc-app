package org.vaibhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {
  private static long iterations = 0;
  private static boolean firstTime = true;

  private static long getCountOnYugabyte(String ybEndpoint) throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:yugabytedb://" + ybEndpoint + ":5433/yugabyte?user=yugabyte&password=yugabyte");
    Statement st = conn.createStatement();
    
    ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM test_cdc_app;");

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
      System.out.println("Table created for the first time, waiting for 20s to let the " +
        "deployment happen");
      firstTime = false;
      Thread.sleep(20000);
    }

    // make sure the table doesn't contain anything
    st.execute("delete from test_cdc_app;");

    long startKey = 1;
    long endKey;
    while(true) {
      endKey = startKey + 511; // Total batch size would be 512
      // insert rows first
      if (true) { // Do not update i anywhere
        int resInsert = st.executeUpdate("insert into test_cdc_app(id) values (generate_series(" + startKey + "," + endKey + "));");
        if (resInsert != 512) {
          throw new RuntimeException("Unable to insert more rows, trying from scratch again...");
        }
      }
      System.out.println("Inserts completed...");
      Thread.sleep(200);

      long countInYb = getCountOnYugabyte(endpoint);

      verifyCountOnMySql(mysqlEndpoint, countInYb);

      // update the inserted rows
      // if (true) {
      //   int resUpdate = st.executeUpdate("update test_cdc_app set name = 'VKVK' where id >= " + startKey + " and id <= " + endKey + ";");
      //   if (resUpdate != 512) {
      //     throw new RuntimeException("Unable to update rows, throwing exception and starting from scratch...");
      //   }
      // }
      // System.out.println("Update complete...");
      // Thread.sleep(200);

      // Write the logic for verification of the counts here.
      // One connection to mysql and one to YB

      // delete the inserted rows
      
      /*
      for (int i = 1; i <= 1000; ++i) {
        st.executeUpdate("delete from test_cdc_app where id = " + i + ";");
      }
      System.out.println("Deletion of 1000 rows complete...");
      Thread.sleep(1000);
      */
      ++iterations;
      System.out.println("Iteration count: " + iterations);
      Thread.sleep(5000);

      startKey = endKey + 1;
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
        if (index >= args.length) {
          index = 0;
        }
      }
    }
  }
}
