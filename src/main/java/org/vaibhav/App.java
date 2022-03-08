package org.vaibhav;

import java.sql.*;

public class App {
  private static long iterations = 0;
  private static boolean firstTime = true;

  private static int counter = 1;
  private static boolean insertCompleted = false;
  private static boolean updateCompleted = false;
  private static boolean deleteCompleted = false;

  private static void runWorkload(String endpoint) throws Exception {
    String ybUrl = "jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
      "user=yugabyte&password=yugabyte";
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
      System.out.println("Will verify the rows in Postgres too here...");
      Thread.sleep(40000);
    }

    // make sure the table doesn't contain anything
    st.execute("delete from test_cdc_app;");

    long numOfRows = 10000;

    while(true) {
      if (!insertCompleted) {
        // insert a thousand rows first
        for (int i = counter; i <= numOfRows; ++i, ++counter) {
          int resInsert = st.executeUpdate("insert into test_cdc_app values (" + i + ");");
          if (resInsert != 1) {
            throw new RuntimeException("Unable to insert more rows, trying from scratch again...");
          }
        }
        System.out.println("Insertion of " + numOfRows + " rows complete...");

        System.out.println("Waiting for 1 s for Postgres to get all the data...");

        insertCompleted = true;
        counter = 1;
        Thread.sleep(300);
      }

      if (!updateCompleted) {
        // update the inserted rows
        for (int i = counter; i <= numOfRows; ++i, ++counter) {
          int resUpdate = st.executeUpdate("update test_cdc_app set name = 'VKVK' where id = " + i + ";");
          if (resUpdate != 1) {
            throw new RuntimeException("Unable to update rows, throwing exception and starting from scratch...");
          }
        }
        System.out.println("Updation of " + numOfRows + " rows complete...");

        System.out.println("Waiting for 1 s for Postgres to get all the data...");

        updateCompleted = true;
        counter = 1;
        Thread.sleep(300);
      }

      if (!deleteCompleted) {
        // delete the inserted rows
        for (int i = counter; i <= numOfRows; ++i, ++counter) {
          st.executeUpdate("delete from test_cdc_app where id = " + i + ";");
        }
        System.out.println("Deletion of " + numOfRows + " rows complete...");

        System.out.println("Waiting for 1 s for Postgres to get all the data...");

        deleteCompleted = true;
        counter = 1;
        Thread.sleep(300);
      }

      ++iterations;
      System.out.println("Iteration count: " + iterations);

      // Reset the control variables.
      insertCompleted = false;
      updateCompleted = false;
      deleteCompleted = false;
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
