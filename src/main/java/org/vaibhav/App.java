package org.vaibhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class App {
  private static long iterations = 0;
  private static boolean firstTime = true;

  private static void runWorkload(String endpoint) throws Exception {
    String ybUrl = "jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
      "user=yugabyte&password=yugabyte&load-balance=true";
    Connection conn = DriverManager.getConnection(ybUrl);
    Statement st = conn.createStatement();
    // set up the table if it doesn't exist
    boolean res = st.execute("create table if not exists test_cdc_app (id int primary key, " +
      "name text default 'Vaibhav');");
    if (!res && firstTime) {
      // this means that the table is created
      System.out.println("Table created for the first time, waiting for 40s to let the " +
        "deployment happen");
      firstTime = false;
      Thread.sleep(40000);
    }

    // make sure the table doesn't contain anything
    st.execute("delete from test_cdc_app;");

    while(true) {
      // insert a thousand rows first
      for (int i = 1; i <= 1000; ++i) {
        int resInsert = st.executeUpdate("insert into test_cdc_app values (" + i + ");");
        if (resInsert != 1) {
          throw new RuntimeException("Unable to insert more rows, trying from scratch again...");
        }
      }
      System.out.println("Insertion of 1000 rows complete...");
      Thread.sleep(1000);

      // update the inserted rows
      for (int i = 1; i <= 1000; ++i) {
        int resUpdate = st.executeUpdate("update test_cdc_app set name = 'VKVK' where id = " + i + ";");
        if (resUpdate != 1) {
          throw new RuntimeException("Unable to update rows, throwing exception and starting from scratch...");
        }
      }
      System.out.println("Updation of 1000 rows complete...");
      Thread.sleep(1000);

      // delete the inserted rows
      for (int i = 1; i <= 1000; ++i) {
        st.executeUpdate("delete from test_cdc_app where id = " + i + ";");
      }
      System.out.println("Deletion of 1000 rows complete...");
      Thread.sleep(1000);

      ++iterations;
      System.out.println("Iteration count: " + iterations);
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
        System.out.println("Exception caught, trying again...");
        ++index;
        if (index >= args.length) {
          index = 0;
        }
      }
    }
  }
}
