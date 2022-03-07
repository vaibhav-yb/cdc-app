package org.vaibhav;

import java.sql.*;

public class App {
  private static long iterations = 0;
  private static boolean firstTime = true;

  private static int counter = 1;
  private static boolean insertCompleted = false;
  private static boolean updateCompleted = false;
  private static boolean deleteCompleted = false;

  private static void runWorkload(String endpoint, String postgresIp) throws Exception {
    String ybUrl = "jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
      "user=yugabyte&password=yugabyte";
    String pgUrl = "jdbc:postgresql://" + postgresIp + ":5432/postgres?user=postgres&password=postgres&sslMode=require";
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

    long numOfRows = 1000;

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
        Thread.sleep(1200);
        // Now verify data in postgres. Since we have inserted 10000 rows, the count should be the same
        try (Connection insertConn = DriverManager.getConnection(pgUrl)) {
          ResultSet rs = insertConn.createStatement().executeQuery("select count(*) from sink;");
          if (rs.next()) {
            int countInResult = rs.getInt(1);
            if (countInResult == numOfRows) {
              System.out.println("Count in PG verified to be " + countInResult);
            }
            else {
              System.out.println("Count in PG not equal, PG = " + countInResult + " , YB = " + numOfRows);
            }
          }
        }
        catch (SQLException ex) {
          ex.printStackTrace();
        }
        insertCompleted = true;
        counter = 0;
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
        Thread.sleep(1200);
        // Now verify data in postgres. All the updated rows should have the name value equal to 'VKVK'
        try (Connection newConn = DriverManager.getConnection(pgUrl)) {
          ResultSet rsUpdate = newConn.createStatement().executeQuery("select count(*) from sink where name = 'VKVK';");
          if (rsUpdate.next()) {
            int countInResult = rsUpdate.getInt(1);
            if (countInResult == numOfRows) {
              System.out.println("Update count in PG verified to be " + countInResult);
            }
            else {
              System.out.println("Update count in PG not equal, PG = " + countInResult + " , YB = " + numOfRows);
            }
          }
        }
        catch (SQLException ex) {
          ex.printStackTrace();
        }
        updateCompleted = true;
        counter = 0;
        Thread.sleep(300);
      }

      if (!deleteCompleted) {
        // delete the inserted rows
        for (int i = counter; i <= numOfRows; ++i, ++counter) {
          st.executeUpdate("delete from test_cdc_app where id = " + i + ";");
        }
        System.out.println("Deletion of " + numOfRows + " rows complete...");

        System.out.println("Waiting for 1 s for Postgres to get all the data...");
        Thread.sleep(1200);
        try (Connection deleteConn = DriverManager.getConnection(pgUrl)) {
          ResultSet rsDelete = deleteConn.createStatement().executeQuery("select count(*) from sink;");
          if (rsDelete.next()) {
            int countInResult = rsDelete.getInt(1);
            if (countInResult == 0) {
              System.out.println("Delete count in PG verified to be zero.");
            }
            else {
              System.out.println("Rows left after deletion (result of select count(*) on postgres): " + countInResult);
            }
          }
        }
        catch (SQLException ex) {
          ex.printStackTrace();
        }
        deleteCompleted = true;
        counter = 0;
        Thread.sleep(300);
      }

      ++iterations;
      System.out.println("Iteration count: " + iterations);

      // Reset the control variables.
      insertCompleted = false;
      updateCompleted = false;
      deleteCompleted = false;
      counter = 0;
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
        // The last index will always refer to the IP where postgres is running.
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
