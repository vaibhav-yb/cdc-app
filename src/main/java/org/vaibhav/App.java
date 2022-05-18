package org.vaibhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class App {
  private static long iterations = 0;
  private static boolean firstTime = true;

  private static void runWorkload(String endpoint) throws Exception {
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

      // update the inserted rows
      if (true) {
        int resUpdate = st.executeUpdate("update test_cdc_app set name = 'VKVK' where id >= " + startKey + " and id <= " + endKey + ";");
        if (resUpdate != 512) {
          throw new RuntimeException("Unable to update rows, throwing exception and starting from scratch...");
        }
      }
      System.out.println("Update complete...");
      Thread.sleep(200);

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
        runWorkload(args[index]);
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
