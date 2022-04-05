package org.vaibhav;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.vaibhav.tables.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

// mvn exec:java -Dexec.mainClass=org.vaibhav.App -Dexec.args="192.168.1.51"

public class App {
  private static final Path resourcePath = Paths.get("src", "main", "resources");
  private static final String RESOURCES_PATH = resourcePath.toFile().getAbsolutePath();
  private static final long TEN_MILLION = 10000000;

  public static void runSqlScript(Connection conn, String fileName) {
    System.out.println("Running the SQL script: " + fileName);
    ScriptRunner sr = new ScriptRunner(conn);
    sr.setAutoCommit(true);

    // This prevents ScriptWriter from printing the queries on terminal or logging them either.
//    sr.setLogWriter(null);

    final String sqlFile = RESOURCES_PATH + "/"+ fileName;
    try {
      Reader reader = new BufferedReader(new FileReader(sqlFile));
      sr.runScript(reader);
    } catch (FileNotFoundException f) {
      f.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // args will contain the endpoints
    if (args.length == 0) {
      System.out.println("No endpoints specified to connect to, exiting...");
      System.exit(-1);
    }

    try {
      Connection conn = DriverManager.getConnection("jdbc:yugabytedb://" + args[0] + ":5433/yugabyte?" +
              "user=yugabyte&password=yugabyte");
//      runSqlScript(conn, "create_tables.sql");
      // CREATE TABLE test_app (k INT PRIMARY KEY, v INT);
      // Insert one row in the table
      Statement st = conn.createStatement();
      st.executeUpdate("INSERT INTO test_app VALUES (1, 0);");
      System.out.println("One row inserted, waiting for 5 seconds...");
      st.close();
      conn.close();
      Thread.sleep(5000);
    } catch (Exception se) {
      System.out.println("Exception thrown: " + se);
      System.exit(-2);
    }

    int numThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    while (true) {
      try {
        Future f1_1 = executorService.submit(new IndefiniteUpdate(args, 1, 10000));
        Future f1_2 = executorService.submit(new IndefiniteUpdate(args, 10001, 20000));
        Future f1_3 = executorService.submit(new IndefiniteUpdate(args, 20001, 30000));
        Future f1_4 = executorService.submit(new IndefiniteUpdate(args, 30001, 40000));
        Future f1_5 = executorService.submit(new IndefiniteUpdate(args, 40001, 50000));
        Future f1_6 = executorService.submit(new IndefiniteUpdate(args, 50001, 60000));
        Future f1_7 = executorService.submit(new IndefiniteUpdate(args, 60001, 70000));
        Future f1_8 = executorService.submit(new IndefiniteUpdate(args, 70001, 90000));
        Future f1_9 = executorService.submit(new IndefiniteUpdate(args, 80001, 90000));
        Future f1_10 = executorService.submit(new IndefiniteUpdate(args, 90001, 100000));

        f1_1.get();
        f1_2.get();
        f1_3.get();
        f1_4.get();
        f1_5.get();
        f1_6.get();
        f1_7.get();
        f1_8.get();
        f1_9.get();
        f1_10.get();

      } catch (Exception e) {
        System.out.println("Exception thrown in main application...");
      }
    }
  }
}
