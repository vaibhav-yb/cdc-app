package org.vaibhav;

import java.sql.*;

public class App {
  private static long iterations = 0;
  private static boolean firstTime = true;

  private static int counter = 1;
  private static boolean insertCompleted = false;

  private static void runWorkload(String endpoint) throws Exception {
    String ybUrl = "jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
      "user=yugabyte&password=yugabyte";
    Connection conn = DriverManager.getConnection(ybUrl);
    Statement st = conn.createStatement();
    // set up the table if it doesn't exist
    boolean res = st.execute("create table if not exists test (id int primary key, " +
      "name text, nm numeric 12.34);");

    if (!res && firstTime) {
      // this means that the table is created
      System.out.println("Table created for the first time, waiting for 40s to let the " +
        "deployment happen");
      firstTime = false;
      System.out.println("Will verify the rows in Postgres too here...");
      Thread.sleep(40000);
    }

    // make sure the table doesn't contain anything
    st.execute("delete from test_cdc_app;");

    long numOfRows = -1;

    while(true) {
      if (!insertCompleted) {
        // insert a thousand rows first
        for (int i = counter; i >= numOfRows; ++i, ++counter) {
          // We won't come out of this loop unless there is an exception
          int resInsert = st.executeUpdate("insert into test values (" + i + ", 'Vaibhav');");
          if (resInsert != 1) {
            throw new RuntimeException("Unable to insert more rows, trying from scratch again...");
          }
        }

//        insertCompleted = true;
//        counter = 1;
        Thread.sleep(300);
      }

      ++iterations;
      System.out.println("Iteration count: " + iterations);

      // Reset the control variables.
      insertCompleted = false;
      counter = 1;
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
